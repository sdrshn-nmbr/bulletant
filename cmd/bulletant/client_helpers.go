package main

import (
	"context"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func (c *cliState) get(ctx context.Context, key []byte) ([]byte, error) {
	if c.http != nil {
		return c.http.Get(ctx, key, c.requestOptions())
	}
	return c.local.Get(ctx, key)
}

func (c *cliState) put(ctx context.Context, key []byte, value []byte) error {
	if c.http != nil {
		return c.http.Put(ctx, key, value, c.requestOptions())
	}
	return c.local.Put(ctx, key, value)
}

func (c *cliState) del(ctx context.Context, key []byte) error {
	if c.http != nil {
		return c.http.Delete(ctx, key, c.requestOptions())
	}
	return c.local.Delete(ctx, key)
}

func (c *cliState) scan(ctx context.Context, req client.ScanRequest) (client.ScanResult, error) {
	if c.http != nil {
		return c.http.Scan(ctx, req, c.requestOptions())
	}
	return c.local.Scan(ctx, req)
}

func (c *cliState) txn(ctx context.Context, ops []client.TransactionOperation) (client.TransactionStatus, error) {
	if c.http != nil {
		return c.http.ExecuteTransaction(ctx, ops, c.requestOptions())
	}
	return c.local.ExecuteTransaction(ctx, ops)
}

func (c *cliState) addVector(ctx context.Context, values []float64, metadata map[string]interface{}) (string, error) {
	if c.http != nil {
		return c.http.AddVector(ctx, values, metadata, c.requestOptions())
	}
	return c.local.AddVector(ctx, values, metadata)
}

func (c *cliState) getVector(ctx context.Context, id string) (*client.Vector, error) {
	if c.http != nil {
		return c.http.GetVector(ctx, id, c.requestOptions())
	}
	return c.local.GetVector(ctx, id)
}

func (c *cliState) deleteVector(ctx context.Context, id string) error {
	if c.http != nil {
		return c.http.DeleteVector(ctx, id, c.requestOptions())
	}
	return c.local.DeleteVector(ctx, id)
}

func (c *cliState) compact(ctx context.Context, opts client.CompactOptions) (client.CompactStats, error) {
	if c.http != nil {
		return c.http.Compact(ctx, opts, c.requestOptions())
	}
	return c.local.Compact(ctx, opts)
}

func (c *cliState) snapshot(ctx context.Context, opts client.SnapshotOptions) (client.SnapshotStats, error) {
	if c.http != nil {
		return c.http.Snapshot(ctx, opts, c.requestOptions())
	}
	return c.local.Snapshot(ctx, opts)
}

func (c *cliState) backup(ctx context.Context, opts client.BackupOptions) (client.BackupStats, error) {
	if c.http != nil {
		return c.http.Backup(ctx, opts, c.requestOptions())
	}
	return c.local.Backup(ctx, opts)
}
