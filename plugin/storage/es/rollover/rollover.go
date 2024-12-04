package rollover

import (
	"net/http"

	"github.com/jaegertracing/jaeger/cmd/es-rollover/app/rollover"
	"github.com/jaegertracing/jaeger/pkg/es/client"
	"github.com/jaegertracing/jaeger/pkg/es/config"
)

type RolloverManager struct {
	cfg config.RolloverConfig
}

func newESClient(endpoint string /*, cfg *Config, tlsCfg *tls.Config*/) client.Client {
	httpClient := &http.Client{
		// Timeout: time.Duration(cfg.Timeout) * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// TLSClientConfig: tlsCfg,
		},
	}
	return client.Client{
		Endpoint: endpoint,
		Client:   httpClient,
		// BasicAuth: client.BasicAuth(cfg.Username, cfg.Password),
	}
}

func (rm *RolloverManager) StartRollover(endpoint string) error {
	// Periodically execute action

	// httpClient := &http.Client{
	// 	Timeout: time.Duration(cfg.Timeout) * time.Second,
	// 	Transport: &http.Transport{
	// 		Proxy:           http.ProxyFromEnvironment,
	// 		TLSClientConfig: tlsCfg,
	// 	},
	// }

	// client := client.Client{
	// 	Endpoint:  endpoint,
	// 	Client:    httpClient,
	// 	BasicAuth: client.BasicAuth(cfg.Username, cfg.Password),
	// }

	indicesClient := client.IndicesClient{}
	action := rollover.Action{}

}
