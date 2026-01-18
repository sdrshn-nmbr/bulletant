package main

import (
	"context"
	"net/http"
	"time"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

type cliState struct {
	mode          string
	keyEncoding   string
	valueEncoding string
	timeout       time.Duration
	local         *client.LocalClient
	http          *client.HTTPClient
}

func newCLIState(cfg cliConfig) (*cliState, error) {
	state := &cliState{
		mode:          cfg.mode,
		keyEncoding:   cfg.keyEncoding,
		valueEncoding: cfg.valueEncoding,
		timeout:       cfg.timeout,
	}

	if cfg.mode == "http" {
		httpClient, err := client.NewHTTPClient(client.HTTPOptions{
			BaseURL:          cfg.baseURL,
			HTTPClient:       &http.Client{Timeout: cfg.timeout},
			RetryPolicy:      client.DefaultRetryPolicy(),
			KeyEncoding:      cfg.keyEncoding,
			ValueEncoding:    cfg.valueEncoding,
			MaxResponseBytes: 8 << 20,
		})
		if err != nil {
			return nil, err
		}
		state.http = httpClient
		return state, nil
	}

	local, err := client.OpenLocal(cfg.local)
	if err != nil {
		return nil, err
	}
	state.local = local
	return state, nil
}

func (c *cliState) Close() {
	if c.local != nil {
		_ = c.local.Close()
	}
}

func (c *cliState) withContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.timeout)
}

func (c *cliState) requestOptions() client.RequestOptions {
	return client.RequestOptions{}
}
