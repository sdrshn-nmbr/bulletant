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

func (c *cliState) createAccount(ctx context.Context, account client.Account) (client.Account, error) {
	if c.http != nil {
		return c.http.CreateAccount(ctx, account, c.requestOptions())
	}
	return c.local.CreateAccount(ctx, account)
}

func (c *cliState) getAccount(ctx context.Context, tenant string, accountID string) (client.Account, error) {
	if c.http != nil {
		return c.http.GetAccount(ctx, tenant, accountID, c.requestOptions())
	}
	return c.local.GetAccount(ctx, tenant, accountID)
}

func (c *cliState) creditAccount(ctx context.Context, tenant string, accountID string, amount int64) (client.Balance, error) {
	if c.http != nil {
		return c.http.CreditAccount(ctx, tenant, accountID, amount, c.requestOptions())
	}
	return c.local.CreditAccount(ctx, tenant, accountID, amount)
}

func (c *cliState) getBalance(ctx context.Context, tenant string, accountID string) (client.Balance, error) {
	if c.http != nil {
		return c.http.GetBalance(ctx, tenant, accountID, c.requestOptions())
	}
	return c.local.GetBalance(ctx, tenant, accountID)
}

func (c *cliState) recordUsage(ctx context.Context, event client.UsageEvent) (client.LedgerEntry, error) {
	if c.http != nil {
		return c.http.RecordUsage(ctx, event, c.requestOptions())
	}
	return c.local.RecordUsage(ctx, event)
}

func (c *cliState) listUsageEvents(
	ctx context.Context,
	tenant string,
	accountID string,
	cursor string,
	limit uint32,
) ([]client.UsageEvent, string, error) {
	if c.http != nil {
		return c.http.ListUsageEvents(ctx, tenant, accountID, cursor, limit, c.requestOptions())
	}
	return c.local.ListUsageEvents(ctx, tenant, accountID, cursor, limit)
}

func (c *cliState) exportEvents(
	ctx context.Context,
	tenant string,
	cursor string,
	limit uint32,
	handler func(client.UsageEvent) error,
) (string, error) {
	if c.http != nil {
		return c.http.ExportEvents(ctx, tenant, cursor, limit, c.requestOptions(), handler)
	}
	return c.local.ExportEvents(ctx, tenant, cursor, limit, handler)
}

func (c *cliState) importEvents(ctx context.Context, events []client.UsageEvent) (client.ImportStats, error) {
	if c.http != nil {
		return c.http.ImportEvents(ctx, events, c.requestOptions())
	}
	return c.local.ImportEvents(ctx, events)
}
