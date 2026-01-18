package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/server"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type billingClientOps struct {
	createAccount func(context.Context, Account) (Account, error)
	creditAccount func(context.Context, string, string, int64) (Balance, error)
	recordUsage   func(context.Context, UsageEvent) (LedgerEntry, error)
	getBalance    func(context.Context, string, string) (Balance, error)
	listEvents    func(context.Context, string, string, string, uint32) ([]UsageEvent, string, error)
	exportEvents  func(context.Context, string, string, uint32, func(UsageEvent) error) (string, error)
	importEvents  func(context.Context, []UsageEvent) (ImportStats, error)
}

func runBillingClientTests(t *testing.T, ops billingClientOps) {
	t.Helper()
	ctx := context.Background()
	account, err := ops.createAccount(ctx, Account{Tenant: "tenant-a", ID: "acct-1"})
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	if account.ID != "acct-1" {
		t.Fatalf("Expected account id acct-1, got %s", account.ID)
	}

	if _, err := ops.creditAccount(ctx, "tenant-a", "acct-1", 1000); err != nil {
		t.Fatalf("CreditAccount failed: %v", err)
	}
	if _, err := ops.recordUsage(ctx, UsageEvent{
		ID:        "event-1",
		Tenant:    "tenant-a",
		AccountID: "acct-1",
		Units:     5,
		UnitPrice: 10,
	}); err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	balance, err := ops.getBalance(ctx, "tenant-a", "acct-1")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balance.Amount != 950 {
		t.Fatalf("Expected balance 950, got %d", balance.Amount)
	}

	events, _, err := ops.listEvents(ctx, "tenant-a", "acct-1", "", 10)
	if err != nil {
		t.Fatalf("ListUsageEvents failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	exported := make([]UsageEvent, 0)
	_, err = ops.exportEvents(ctx, "tenant-a", "", 10, func(event UsageEvent) error {
		exported = append(exported, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ExportEvents failed: %v", err)
	}
	if len(exported) != 1 {
		t.Fatalf("Expected 1 exported event, got %d", len(exported))
	}

	stats, err := ops.importEvents(ctx, exported)
	if err != nil {
		t.Fatalf("ImportEvents failed: %v", err)
	}
	if stats.Skipped != len(exported) {
		t.Fatalf("Expected %d skipped, got %d", len(exported), stats.Skipped)
	}
}

func TestBillingClientLocal(t *testing.T) {
	local, err := OpenLocal(LocalOptions{StorageType: StorageMemory})
	if err != nil {
		t.Fatalf("OpenLocal failed: %v", err)
	}
	defer local.Close()

	runBillingClientTests(t, billingClientOps{
		createAccount: local.CreateAccount,
		creditAccount: local.CreditAccount,
		recordUsage:   local.RecordUsage,
		getBalance:    local.GetBalance,
		listEvents:    local.ListUsageEvents,
		exportEvents:  local.ExportEvents,
		importEvents:  local.ImportEvents,
	})
}

func TestBillingClientHTTP(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	ts := httptest.NewServer(server.NewHandler(database))
	defer ts.Close()

	httpClient, err := NewHTTPClient(HTTPOptions{
		BaseURL:          ts.URL,
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		RetryPolicy:      DefaultRetryPolicy(),
		KeyEncoding:      "base64",
		ValueEncoding:    "base64",
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	opts := RequestOptions{}
	runBillingClientTests(t, billingClientOps{
		createAccount: func(ctx context.Context, account Account) (Account, error) {
			return httpClient.CreateAccount(ctx, account, opts)
		},
		creditAccount: func(ctx context.Context, tenant string, accountID string, amount int64) (Balance, error) {
			return httpClient.CreditAccount(ctx, tenant, accountID, amount, opts)
		},
		recordUsage: func(ctx context.Context, event UsageEvent) (LedgerEntry, error) {
			return httpClient.RecordUsage(ctx, event, opts)
		},
		getBalance: func(ctx context.Context, tenant string, accountID string) (Balance, error) {
			return httpClient.GetBalance(ctx, tenant, accountID, opts)
		},
		listEvents: func(ctx context.Context, tenant string, accountID string, cursor string, limit uint32) ([]UsageEvent, string, error) {
			return httpClient.ListUsageEvents(ctx, tenant, accountID, cursor, limit, opts)
		},
		exportEvents: func(ctx context.Context, tenant string, cursor string, limit uint32, handler func(UsageEvent) error) (string, error) {
			return httpClient.ExportEvents(ctx, tenant, cursor, limit, opts, handler)
		},
		importEvents: func(ctx context.Context, events []UsageEvent) (ImportStats, error) {
			return httpClient.ImportEvents(ctx, events, opts)
		},
	})
}
