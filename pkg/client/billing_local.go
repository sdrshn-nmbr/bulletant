package client

import (
	"context"
	"errors"

	"github.com/sdrshn-nmbr/bulletant/internal/billing"
)

func (c *LocalClient) CreateAccount(ctx context.Context, account Account) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if account.Tenant == "" || account.ID == "" {
		return Account{}, ErrInvalidArgument
	}
	created, err := c.billing.CreateAccount(ctx, toBillingAccount(account))
	if err != nil {
		return Account{}, mapBillingError(err)
	}
	return fromBillingAccount(created), nil
}

func (c *LocalClient) GetAccount(ctx context.Context, tenant string, accountID string) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if tenant == "" || accountID == "" {
		return Account{}, ErrInvalidArgument
	}
	account, err := c.billing.GetAccount(ctx, tenant, accountID)
	if err != nil {
		return Account{}, mapBillingError(err)
	}
	return fromBillingAccount(account), nil
}

func (c *LocalClient) CreditAccount(ctx context.Context, tenant string, accountID string, amount int64) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" || amount <= 0 {
		return Balance{}, ErrInvalidArgument
	}
	balance, err := c.billing.CreditAccount(ctx, tenant, accountID, amount)
	if err != nil {
		return Balance{}, mapBillingError(err)
	}
	return fromBillingBalance(balance), nil
}

func (c *LocalClient) GetBalance(ctx context.Context, tenant string, accountID string) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" {
		return Balance{}, ErrInvalidArgument
	}
	balance, err := c.billing.GetBalance(ctx, tenant, accountID)
	if err != nil {
		return Balance{}, mapBillingError(err)
	}
	return fromBillingBalance(balance), nil
}

func (c *LocalClient) RecordUsage(ctx context.Context, event UsageEvent) (LedgerEntry, error) {
	if err := ctx.Err(); err != nil {
		return LedgerEntry{}, err
	}
	if event.ID == "" || event.Tenant == "" || event.AccountID == "" || event.Units <= 0 {
		return LedgerEntry{}, ErrInvalidArgument
	}
	if event.UnitPrice <= 0 && event.PricePlanID == "" {
		return LedgerEntry{}, ErrInvalidArgument
	}
	entry, err := c.billing.RecordUsage(ctx, toBillingUsageEvent(event))
	if err != nil {
		return LedgerEntry{}, mapBillingError(err)
	}
	return fromBillingLedgerEntry(entry), nil
}

func (c *LocalClient) ListUsageEvents(
	ctx context.Context,
	tenant string,
	accountID string,
	cursor string,
	limit uint32,
) ([]UsageEvent, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if tenant == "" || accountID == "" || limit == 0 {
		return nil, "", ErrInvalidArgument
	}
	events, nextCursor, err := c.billing.ListUsageEvents(ctx, tenant, accountID, cursor, limit)
	if err != nil {
		return nil, "", mapBillingError(err)
	}
	converted := make([]UsageEvent, 0, len(events))
	for _, event := range events {
		converted = append(converted, fromBillingUsageEvent(event))
	}
	return converted, nextCursor, nil
}

func (c *LocalClient) ExportEvents(
	ctx context.Context,
	tenant string,
	cursor string,
	limit uint32,
	handler func(UsageEvent) error,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if handler == nil || limit == 0 {
		return "", ErrInvalidArgument
	}
	nextCursor, err := c.billing.ExportEvents(ctx, tenant, cursor, limit, func(event billing.UsageEvent) error {
		return handler(fromBillingUsageEvent(event))
	})
	if err != nil {
		return "", mapBillingError(err)
	}
	return nextCursor, nil
}

func (c *LocalClient) ImportEvents(ctx context.Context, events []UsageEvent) (ImportStats, error) {
	if err := ctx.Err(); err != nil {
		return ImportStats{}, err
	}
	if len(events) == 0 {
		return ImportStats{}, ErrInvalidArgument
	}
	converted := make([]billing.UsageEvent, 0, len(events))
	for _, event := range events {
		converted = append(converted, toBillingUsageEvent(event))
	}
	stats, err := c.billing.ImportEvents(ctx, converted)
	if err != nil {
		return ImportStats{}, mapBillingError(err)
	}
	return ImportStats{Imported: stats.Imported, Skipped: stats.Skipped}, nil
}

func toBillingAccount(account Account) billing.Account {
	return billing.Account{
		Tenant:    account.Tenant,
		ID:        account.ID,
		CreatedAt: account.CreatedAt,
		Metadata:  account.Metadata,
	}
}

func fromBillingAccount(account billing.Account) Account {
	return Account{
		Tenant:    account.Tenant,
		ID:        account.ID,
		CreatedAt: account.CreatedAt,
		Metadata:  account.Metadata,
	}
}

func toBillingUsageEvent(event UsageEvent) billing.UsageEvent {
	return billing.UsageEvent{
		ID:          event.ID,
		Tenant:      event.Tenant,
		AccountID:   event.AccountID,
		Timestamp:   event.Timestamp,
		Units:       event.Units,
		PricePlanID: event.PricePlanID,
		UnitPrice:   event.UnitPrice,
		Cost:        event.Cost,
		PrevHash:    event.PrevHash,
		Hash:        event.Hash,
		Metadata:    event.Metadata,
	}
}

func fromBillingUsageEvent(event billing.UsageEvent) UsageEvent {
	return UsageEvent{
		ID:          event.ID,
		Tenant:      event.Tenant,
		AccountID:   event.AccountID,
		Timestamp:   event.Timestamp,
		Units:       event.Units,
		PricePlanID: event.PricePlanID,
		UnitPrice:   event.UnitPrice,
		Cost:        event.Cost,
		PrevHash:    event.PrevHash,
		Hash:        event.Hash,
		Metadata:    event.Metadata,
	}
}

func fromBillingLedgerEntry(entry billing.LedgerEntry) LedgerEntry {
	return LedgerEntry{
		EventID:      entry.EventID,
		Tenant:       entry.Tenant,
		AccountID:    entry.AccountID,
		Units:        entry.Units,
		UnitPrice:    entry.UnitPrice,
		Cost:         entry.Cost,
		BalanceAfter: entry.BalanceAfter,
		Timestamp:    entry.Timestamp,
	}
}

func fromBillingBalance(balance billing.Balance) Balance {
	return Balance{
		Tenant:    balance.Tenant,
		AccountID: balance.AccountID,
		Amount:    balance.Amount,
		UpdatedAt: balance.UpdatedAt,
	}
}

func mapBillingError(err error) error {
	switch {
	case errors.Is(err, billing.ErrAccountExists):
		return ErrAccountExists
	case errors.Is(err, billing.ErrAccountNotFound):
		return ErrAccountNotFound
	case errors.Is(err, billing.ErrInsufficientCredits):
		return ErrInsufficientCredits
	case errors.Is(err, billing.ErrDuplicateEvent):
		return ErrDuplicateEvent
	case errors.Is(err, billing.ErrPricePlanNotFound):
		return ErrPricePlanNotFound
	default:
		return err
	}
}
