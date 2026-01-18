package billing

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

func newBillingService(t *testing.T) (*Service, *db.DB) {
	t.Helper()
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	service := NewService(database, ServiceOptions{})
	return service, database
}

func TestRecordUsageIdempotent(t *testing.T) {
	service, database := newBillingService(t)
	defer database.Close()

	ctx := context.Background()
	_, err := service.CreateAccount(ctx, Account{Tenant: "tenant-a", ID: "acct-1"})
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	if _, err := service.CreditAccount(ctx, "tenant-a", "acct-1", 100); err != nil {
		t.Fatalf("CreditAccount failed: %v", err)
	}

	event := UsageEvent{
		ID:        "event-1",
		Tenant:    "tenant-a",
		AccountID: "acct-1",
		Units:     5,
		UnitPrice: 10,
	}
	entry, err := service.RecordUsage(ctx, event)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}
	replay, err := service.RecordUsage(ctx, event)
	if err != nil {
		t.Fatalf("RecordUsage replay failed: %v", err)
	}
	if entry.BalanceAfter != replay.BalanceAfter {
		t.Fatalf("Expected balance %d, got %d", entry.BalanceAfter, replay.BalanceAfter)
	}

	balance, err := service.GetBalance(ctx, "tenant-a", "acct-1")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balance.Amount != entry.BalanceAfter {
		t.Fatalf("Expected balance %d, got %d", entry.BalanceAfter, balance.Amount)
	}
}

func TestRecordUsageInsufficientCredits(t *testing.T) {
	service, database := newBillingService(t)
	defer database.Close()

	ctx := context.Background()
	_, err := service.CreateAccount(ctx, Account{Tenant: "tenant-a", ID: "acct-1"})
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	_, err = service.RecordUsage(ctx, UsageEvent{
		ID:        "event-1",
		Tenant:    "tenant-a",
		AccountID: "acct-1",
		Units:     10,
		UnitPrice: 5,
	})
	if err == nil || err != ErrInsufficientCredits {
		t.Fatalf("Expected ErrInsufficientCredits, got %v", err)
	}
}

func TestRecordUsagePriceSnapshot(t *testing.T) {
	service, database := newBillingService(t)
	defer database.Close()

	ctx := context.Background()
	_, err := service.CreateAccount(ctx, Account{Tenant: "tenant-a", ID: "acct-1"})
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	if _, err := service.CreditAccount(ctx, "tenant-a", "acct-1", 1000); err != nil {
		t.Fatalf("CreditAccount failed: %v", err)
	}

	if err := service.PutPricePlan(ctx, PricePlan{
		ID:        "plan-basic",
		UnitPrice: 7,
		Unit:      "token",
	}); err != nil {
		t.Fatalf("PutPricePlan failed: %v", err)
	}

	event := UsageEvent{
		ID:          "event-1",
		Tenant:      "tenant-a",
		AccountID:   "acct-1",
		Units:       3,
		PricePlanID: "plan-basic",
	}
	if _, err := service.RecordUsage(ctx, event); err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	snapshotBytes, err := database.Get([]byte(PriceSnapshotKey(event.ID)))
	if err != nil {
		t.Fatalf("PriceSnapshot not stored: %v", err)
	}

	var snapshot priceSnapshot
	if err := json.Unmarshal(snapshotBytes, &snapshot); err != nil {
		t.Fatalf("Decode price snapshot failed: %v", err)
	}
	if snapshot.PricePlanID != "plan-basic" {
		t.Fatalf("Expected plan-basic, got %s", snapshot.PricePlanID)
	}
	if snapshot.UnitPrice != 7 {
		t.Fatalf("Expected unit price 7, got %d", snapshot.UnitPrice)
	}
}

func TestListUsageEventsOrdering(t *testing.T) {
	service, database := newBillingService(t)
	defer database.Close()

	ctx := context.Background()
	_, err := service.CreateAccount(ctx, Account{Tenant: "tenant-a", ID: "acct-1"})
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	if _, err := service.CreditAccount(ctx, "tenant-a", "acct-1", 1000); err != nil {
		t.Fatalf("CreditAccount failed: %v", err)
	}

	early := time.Unix(0, 50)
	late := time.Unix(0, 100)
	_, err = service.RecordUsage(ctx, UsageEvent{
		ID:        "event-early",
		Tenant:    "tenant-a",
		AccountID: "acct-1",
		Timestamp: early,
		Units:     1,
		UnitPrice: 10,
	})
	if err != nil {
		t.Fatalf("RecordUsage early failed: %v", err)
	}
	_, err = service.RecordUsage(ctx, UsageEvent{
		ID:        "event-late",
		Tenant:    "tenant-a",
		AccountID: "acct-1",
		Timestamp: late,
		Units:     1,
		UnitPrice: 10,
	})
	if err != nil {
		t.Fatalf("RecordUsage late failed: %v", err)
	}

	events, _, err := service.ListUsageEvents(ctx, "tenant-a", "acct-1", "", 10)
	if err != nil {
		t.Fatalf("ListUsageEvents failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(events))
	}
	if !events[0].Timestamp.Equal(early) {
		t.Fatalf("Expected first event at %v, got %v", early, events[0].Timestamp)
	}
	if !events[1].Timestamp.Equal(late) {
		t.Fatalf("Expected second event at %v, got %v", late, events[1].Timestamp)
	}
}
