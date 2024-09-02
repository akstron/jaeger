// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
// SPDX-License-Identifier: Apache-2.0

package cassandra

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/cassandra"
	"github.com/jaegertracing/jaeger/pkg/cassandra/config"
	"github.com/jaegertracing/jaeger/pkg/distributedlock"
	"github.com/jaegertracing/jaeger/pkg/hostname"
	"github.com/jaegertracing/jaeger/pkg/metrics"
	"github.com/jaegertracing/jaeger/plugin"
	cLock "github.com/jaegertracing/jaeger/plugin/pkg/distributedlock/cassandra"
	cDepStore "github.com/jaegertracing/jaeger/plugin/storage/cassandra/dependencystore"
	cSamplingStore "github.com/jaegertracing/jaeger/plugin/storage/cassandra/samplingstore"
	cSpanStore "github.com/jaegertracing/jaeger/plugin/storage/cassandra/spanstore"
	"github.com/jaegertracing/jaeger/plugin/storage/cassandra/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/storage"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/samplingstore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	primaryStorageConfig = "cassandra"
	archiveStorageConfig = "cassandra-archive"
)

var ( // interface comformance checks
	_ storage.Factory              = (*Factory)(nil)
	_ storage.Purger               = (*Factory)(nil)
	_ storage.ArchiveFactory       = (*Factory)(nil)
	_ storage.SamplingStoreFactory = (*Factory)(nil)
	_ io.Closer                    = (*Factory)(nil)
	_ plugin.Configurable          = (*Factory)(nil)
)

// Factory implements storage.Factory for Cassandra backend.
type Factory struct {
	Options *Options

	primaryMetricsFactory metrics.Factory
	archiveMetricsFactory metrics.Factory
	logger                *zap.Logger
	tracer                trace.TracerProvider

	primaryConfig  config.SessionBuilder
	primarySession cassandra.Session
	archiveConfig  config.SessionBuilder
	archiveSession cassandra.Session
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{
		tracer:  otel.GetTracerProvider(),
		Options: NewOptions(primaryStorageConfig, archiveStorageConfig),
	}
}

// NewFactoryWithConfig initializes factory with Config.
func NewFactoryWithConfig(
	opts Options,
	metricsFactory metrics.Factory,
	logger *zap.Logger,
) (*Factory, error) {
	f := NewFactory()
	// use this to help with testing
	b := &withConfigBuilder{
		f:              f,
		opts:           &opts,
		metricsFactory: metricsFactory,
		logger:         logger,
		initializer:    f.Initialize, // this can be mocked in tests
	}
	return b.build()
}

type withConfigBuilder struct {
	f              *Factory
	opts           *Options
	metricsFactory metrics.Factory
	logger         *zap.Logger
	initializer    func(metricsFactory metrics.Factory, logger *zap.Logger) error
}

func (b *withConfigBuilder) build() (*Factory, error) {
	b.f.configureFromOptions(b.opts)
	if err := b.opts.Primary.Validate(); err != nil {
		return nil, err
	}
	err := b.initializer(b.metricsFactory, b.logger)
	if err != nil {
		return nil, err
	}
	return b.f, nil
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper, _ *zap.Logger) {
	f.Options.InitFromViper(v)
	f.configureFromOptions(f.Options)
}

// InitFromOptions initializes factory from options.
func (f *Factory) configureFromOptions(o *Options) {
	f.Options = o
	// TODO this is a hack because we do not define defaults in Options
	if o.others == nil {
		o.others = make(map[string]*NamespaceConfig)
	}
	f.primaryConfig = o.GetPrimary()
	if cfg := f.Options.Get(archiveStorageConfig); cfg != nil {
		f.archiveConfig = cfg // this is so stupid - see https://golang.org/doc/faq#nil_error
	}
}

const (
	MODE               = `MODE`
	DATACENTER         = `DATACENTER`
	TRACE_TTL          = `TRACE_TTL`
	DEPENDENCIES_TTL   = `DEPENDENCIES_TTL`
	KEYSPACE           = `KEYSPACE`
	REPLICATION_FACTOR = `REPLICATION_FACTOR`
	VERSION            = `VERSION`
	COMPACTION_WINDOW  = `COMPACTION_WINDOW`
)

// Parameters required for initilizing the db
type StorageConfigParams struct {
	mode                   string
	datacenter             string
	trace_ttl              int
	dependencies_ttl       int
	keyspace               string
	replication_factor     int
	replication            string
	cas_version            int
	compaction_window_size int
	compaction_window_unit string
}

func constructStorageConfigParams() (*StorageConfigParams, error) {
	var err error

	datacenter := os.Getenv(DATACENTER)
	replication_factor_string := os.Getenv(REPLICATION_FACTOR)

	var replication_factor, compaction_window_size int
	var replication, compaction_window_unit string

	mode := os.Getenv(MODE)

	if mode == "" {
		return nil, fmt.Errorf("missing MODE parameter")
	}

	if mode != "test" || mode != "prod" {
		return nil, fmt.Errorf("invalid MODE=%s, expecting 'prod' or 'test'", mode)
	}

	if mode == "test" {
		if datacenter == "" {
			datacenter = "test"
		}

		replication_factor = 1
		if replication_factor_string != "" {
			replication_factor, err = strconv.Atoi(replication_factor_string)
			if err != nil {
				return nil, err
			}
		}

		replication = fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': '%v'}", replication_factor)
	}

	if mode == "prod" {
		if datacenter == "" {
			return nil, fmt.Errorf("missing DATACENTER parameter for prod mode")
		}

		replication_factor = 2
		if replication_factor_string != "" {
			replication_factor, err = strconv.Atoi(replication_factor_string)
			if err != nil {
				return nil, err
			}
		}

		replication = fmt.Sprintf("{'class': 'NetworkTopologyStrategy', '%s': '%v' }", datacenter, replication_factor)
	}

	trace_ttl_string := os.Getenv(TRACE_TTL)
	trace_ttl := 172800
	if trace_ttl_string != "" {
		trace_ttl, err = strconv.Atoi(trace_ttl_string)
		if err != nil {
			return nil, err
		}
	}

	dependencies_ttl_string := os.Getenv(DEPENDENCIES_TTL)
	dependencies_ttl := 0
	if dependencies_ttl_string != "" {
		dependencies_ttl, err = strconv.Atoi(dependencies_ttl_string)
		if err != nil {
			return nil, err
		}
	}

	cas_version_string := os.Getenv(VERSION)
	cas_version := 4
	if cas_version_string != "" {
		cas_version, err = strconv.Atoi(cas_version_string)
		if err != nil {
			return nil, err
		}
	}

	keyspace := os.Getenv(KEYSPACE)
	if keyspace == "" {
		keyspace = fmt.Sprint("jaeger_v1_%s", datacenter)
	}

	var isMatch bool
	isMatch, err = regexp.MatchString("[^a-zA-Z0-9_]", keyspace)
	if err != nil {
		return nil, err
	}

	if isMatch {
		return nil, fmt.Errorf(`invalid characters in KEYSPACE=%s parameter, please use letters, digits or underscores`, keyspace)
	}

	if compaction_window := os.Getenv(COMPACTION_WINDOW); compaction_window != `` {
		isMatch, err = regexp.MatchString("^[0-9]+[mhd]$", compaction_window)
		if err != nil {
			return nil, err
		}

		if !isMatch {
			return nil, fmt.Errorf("Invalid compaction window size format. Please use numeric value followed by 'm' for minutes, 'h' for hours, or 'd' for days.")
		}

		compaction_window_size, err = strconv.Atoi(compaction_window[:len(compaction_window)-1])
		if err != nil {
			return nil, err
		}

		compaction_window_unit = compaction_window[len(compaction_window)-1:]
	} else {
		trace_ttl_minutes := trace_ttl / 60

		compaction_window_size = (trace_ttl_minutes + 30 - 1) / 30
		compaction_window_unit = "m"
	}

	switch compaction_window_unit {
	case `m`:
		compaction_window_unit = `MINUTES`
	case `h`:
		compaction_window_unit = `HOURS`
	case `d`:
		compaction_window_unit = `DAYS`
	}

	return &StorageConfigParams{
		mode:                   mode,
		datacenter:             datacenter,
		trace_ttl:              trace_ttl,
		dependencies_ttl:       dependencies_ttl,
		keyspace:               keyspace,
		replication_factor:     replication_factor,
		replication:            replication,
		cas_version:            cas_version,
		compaction_window_size: compaction_window_size,
		compaction_window_unit: compaction_window_unit,
	}, nil
}

// Embed all the template files in binaries

//go:embed schema/v001.cql.tmpl
//go:embed schema/v002.cql.tmpl
//go:embed schema/v003.cql.tmpl
//go:embed schema/v004.cql.tmpl
var schemaFile embed.FS

func handleTemplateReplacements(data []byte, params *StorageConfigParams) []byte {
	templateKeysValuePairs := map[string]string{
		`trace_ttl`:              strconv.Itoa(params.trace_ttl),
		`dependecies_ttl`:        strconv.Itoa(params.dependencies_ttl),
		`keyspace`:               params.keyspace,
		`replication_factor`:     strconv.Itoa(params.replication_factor),
		`replication`:            params.replication,
		`cas_version`:            strconv.Itoa(params.cas_version),
		`compaction_window_size`: strconv.Itoa(params.compaction_window_size),
		`compaction_window_unit`: params.compaction_window_unit,
	}

	result := data
	for key, value := range templateKeysValuePairs {
		result = bytes.ReplaceAll(result, []byte(key), []byte(value))
	}
	return result
}

func constructQueriesFromTemplateFiles(session cassandra.Session, params *StorageConfigParams) ([]cassandra.Query, error) {
	var queries []cassandra.Query

	schemaFileName := fmt.Sprintf(`schema/v00%s.cql.tmpl`, strconv.Itoa(4))
	schemaData, err := schemaFile.ReadFile(schemaFileName)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(schemaData, []byte("\n"))
	var extractedLines [][]byte

	for _, line := range lines {
		// Remove any comments, if at the end of the line
		commentIndex := bytes.LastIndex(line, []byte(`--`))
		if commentIndex != -1 {
			// remove everything after comment
			line = line[0:commentIndex]
		}

		if len(line) == 0 {
			continue
		}

		extractedLines = append(extractedLines, bytes.TrimSpace(handleTemplateReplacements(line, params)))
	}

	// Construct individual queries
	var queryString string
	for _, line := range extractedLines {
		queryString += string(line)
		if bytes.HasSuffix(line, []byte(";")) {
			queries = append(queries, session.Query(queryString))
			queryString = ""
		}
	}

	if len(queryString) > 0 {
		return nil, fmt.Errorf(`Invalid template`)
	}

	return queries, nil
}

func (f *Factory) InitializeDB(session cassandra.Session) error {
	params, err := constructStorageConfigParams()
	if err != nil {
		return err
	}

	queries, err := constructQueriesFromTemplateFiles(session, params)
	if err != nil {
		return err
	}

	for _, query := range queries {
		err := query.Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.primaryMetricsFactory = metricsFactory.Namespace(metrics.NSOptions{Name: "cassandra", Tags: nil})
	f.archiveMetricsFactory = metricsFactory.Namespace(metrics.NSOptions{Name: "cassandra-archive", Tags: nil})
	f.logger = logger

	primarySession, err := f.primaryConfig.NewSession(logger)
	if err != nil {
		return err
	}
	f.primarySession = primarySession

	// After creating a session, execute commands to initialize the setup if not already present
	if err := f.InitializeDB(primarySession); err != nil {
		return err
	}

	if f.archiveConfig != nil {
		archiveSession, err := f.archiveConfig.NewSession(logger)
		if err != nil {
			return err
		}
		f.archiveSession = archiveSession

		if err := f.InitializeDB(archiveSession); err != nil {
			return err
		}
	} else {
		logger.Info("Cassandra archive storage configuration is empty, skipping")
	}
	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return cSpanStore.NewSpanReader(f.primarySession, f.primaryMetricsFactory, f.logger, f.tracer.Tracer("cSpanStore.SpanReader")), nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	options, err := writerOptions(f.Options)
	if err != nil {
		return nil, err
	}
	return cSpanStore.NewSpanWriter(f.primarySession, f.Options.SpanStoreWriteCacheTTL, f.primaryMetricsFactory, f.logger, options...), nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	version := cDepStore.GetDependencyVersion(f.primarySession)
	return cDepStore.NewDependencyStore(f.primarySession, f.primaryMetricsFactory, f.logger, version)
}

// CreateArchiveSpanReader implements storage.ArchiveFactory
func (f *Factory) CreateArchiveSpanReader() (spanstore.Reader, error) {
	if f.archiveSession == nil {
		return nil, storage.ErrArchiveStorageNotConfigured
	}
	return cSpanStore.NewSpanReader(f.archiveSession, f.archiveMetricsFactory, f.logger, f.tracer.Tracer("cSpanStore.SpanReader")), nil
}

// CreateArchiveSpanWriter implements storage.ArchiveFactory
func (f *Factory) CreateArchiveSpanWriter() (spanstore.Writer, error) {
	if f.archiveSession == nil {
		return nil, storage.ErrArchiveStorageNotConfigured
	}
	options, err := writerOptions(f.Options)
	if err != nil {
		return nil, err
	}
	return cSpanStore.NewSpanWriter(f.archiveSession, f.Options.SpanStoreWriteCacheTTL, f.archiveMetricsFactory, f.logger, options...), nil
}

// CreateLock implements storage.SamplingStoreFactory
func (f *Factory) CreateLock() (distributedlock.Lock, error) {
	hostname, err := hostname.AsIdentifier()
	if err != nil {
		return nil, err
	}
	f.logger.Info("Using unique participantName in the distributed lock", zap.String("participantName", hostname))

	return cLock.NewLock(f.primarySession, hostname), nil
}

// CreateSamplingStore implements storage.SamplingStoreFactory
func (f *Factory) CreateSamplingStore(int /* maxBuckets */) (samplingstore.Store, error) {
	return cSamplingStore.New(f.primarySession, f.primaryMetricsFactory, f.logger), nil
}

func writerOptions(opts *Options) ([]cSpanStore.Option, error) {
	var tagFilters []dbmodel.TagFilter

	// drop all tag filters
	if !opts.Index.Tags || !opts.Index.ProcessTags || !opts.Index.Logs {
		tagFilters = append(tagFilters, dbmodel.NewTagFilterDropAll(!opts.Index.Tags, !opts.Index.ProcessTags, !opts.Index.Logs))
	}

	// black/white list tag filters
	tagIndexBlacklist := opts.TagIndexBlacklist()
	tagIndexWhitelist := opts.TagIndexWhitelist()
	if len(tagIndexBlacklist) > 0 && len(tagIndexWhitelist) > 0 {
		return nil, errors.New("only one of TagIndexBlacklist and TagIndexWhitelist can be specified")
	}
	if len(tagIndexBlacklist) > 0 {
		tagFilters = append(tagFilters, dbmodel.NewBlacklistFilter(tagIndexBlacklist))
	} else if len(tagIndexWhitelist) > 0 {
		tagFilters = append(tagFilters, dbmodel.NewWhitelistFilter(tagIndexWhitelist))
	}

	if len(tagFilters) == 0 {
		return nil, nil
	} else if len(tagFilters) == 1 {
		return []cSpanStore.Option{cSpanStore.TagFilter(tagFilters[0])}, nil
	}

	return []cSpanStore.Option{cSpanStore.TagFilter(dbmodel.NewChainedTagFilter(tagFilters...))}, nil
}

var _ io.Closer = (*Factory)(nil)

// Close closes the resources held by the factory
func (f *Factory) Close() error {
	if f.primarySession != nil {
		f.primarySession.Close()
	}
	if f.archiveSession != nil {
		f.archiveSession.Close()
	}

	var errs []error
	if cfg := f.Options.Get(archiveStorageConfig); cfg != nil {
		errs = append(errs, cfg.TLS.Close())
	}
	errs = append(errs, f.Options.GetPrimary().TLS.Close())
	return errors.Join(errs...)
}

func (f *Factory) Purge(_ context.Context) error {
	return f.primarySession.Query("TRUNCATE traces").Exec()
}
