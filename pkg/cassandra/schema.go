package cassandra

type SchemaConfig struct {
	Datacenter        string `mapstructure:"datacenter"`
	TraceTTL          int    `mapstructure:"trace_ttl"`
	DependenciesTTL   int    `mapstructure:"dependencies_ttl"`
	ReplicationFactor int    `mapstructure:"replication_factor"`
	CasVersion        int    `mapstructure:"cas_version"`
	CompactionWindow  string `mapstructure:"compaction_window"`
}

/*
	Below 3 properties has to be created:

	Replication
	CompactionWindowSize
	CompactionWindowUnit
*/

func constructStorageConfigParams() (*SchemaConfig, error) {

}
