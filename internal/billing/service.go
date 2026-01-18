package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

const maxEventValueBytes uint32 = 8 << 20

type Service struct {
	db             *db.DB
	now            func() time.Time
	allowOverdraft bool
}

type ServiceOptions struct {
	AllowOverdraft bool
	Now            func() time.Time
}

func NewService(database *db.DB, opts ServiceOptions) *Service {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Service{
		db:             database,
		now:            nowFn,
		allowOverdraft: opts.AllowOverdraft,
	}
}

func (s *Service) CreateAccount(ctx context.Context, account Account) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if account.Tenant == "" || account.ID == "" {
		return Account{}, errors.New("tenant and id are required")
	}
	if account.CreatedAt.IsZero() {
		account.CreatedAt = s.now()
	}

	accountKey := AccountKey(account.Tenant, account.ID)
	if _, err := s.db.Get([]byte(accountKey)); err == nil {
		return Account{}, ErrAccountExists
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return Account{}, err
	}

	encodedAccount, err := EncodeAccount(account)
	if err != nil {
		return Account{}, err
	}

	balance := Balance{
		Tenant:    account.Tenant,
		AccountID: account.ID,
		Amount:    0,
		UpdatedAt: account.CreatedAt,
	}
	encodedBalance, err := EncodeBalance(balance)
	if err != nil {
		return Account{}, err
	}

	txn := transaction.NewTransaction()
	txn.Put(types.Key(accountKey), types.Value(encodedAccount))
	txn.Put(types.Key(BalanceKey(account.Tenant, account.ID)), types.Value(encodedBalance))
	if err := s.db.ExecuteTransaction(txn); err != nil {
		return Account{}, err
	}
	return account, nil
}

func (s *Service) GetAccount(ctx context.Context, tenant string, accountID string) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if tenant == "" || accountID == "" {
		return Account{}, errors.New("tenant and account_id are required")
	}

	accountKey := AccountKey(tenant, accountID)
	data, err := s.db.Get([]byte(accountKey))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return Account{}, ErrAccountNotFound
		}
		return Account{}, err
	}
	return DecodeAccount(data)
}

func (s *Service) CreditAccount(ctx context.Context, tenant string, accountID string, amount int64) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" {
		return Balance{}, errors.New("tenant and account_id are required")
	}
	if amount <= 0 {
		return Balance{}, errors.New("amount must be greater than zero")
	}

	balance, err := s.loadBalance(tenant, accountID)
	if err != nil {
		return Balance{}, err
	}
	next, err := safeAdd(balance.Amount, amount)
	if err != nil {
		return Balance{}, err
	}
	balance.Amount = next
	balance.UpdatedAt = s.now()

	encodedBalance, err := EncodeBalance(balance)
	if err != nil {
		return Balance{}, err
	}

	txn := transaction.NewTransaction()
	txn.Put(types.Key(BalanceKey(tenant, accountID)), types.Value(encodedBalance))
	if err := s.db.ExecuteTransaction(txn); err != nil {
		return Balance{}, err
	}
	return balance, nil
}

func (s *Service) GetBalance(ctx context.Context, tenant string, accountID string) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" {
		return Balance{}, errors.New("tenant and account_id are required")
	}
	return s.loadBalance(tenant, accountID)
}

func (s *Service) RecordUsage(ctx context.Context, event UsageEvent) (LedgerEntry, error) {
	entry, _, err := s.recordUsage(ctx, event)
	return entry, err
}

func (s *Service) ListUsageEvents(
	ctx context.Context,
	tenant string,
	accountID string,
	cursor string,
	limit uint32,
) ([]UsageEvent, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if tenant == "" || accountID == "" {
		return nil, "", errors.New("tenant and account_id are required")
	}
	if limit == 0 {
		return nil, "", errors.New("limit must be greater than zero")
	}

	req := storage.ScanRequest{
		Cursor:        types.Key(cursor),
		Prefix:        types.Key(AccountEventPrefix(tenant, accountID)),
		Limit:         limit,
		IncludeValues: true,
		MaxValueBytes: maxEventValueBytes,
	}
	result, err := s.db.Scan(req)
	if err != nil {
		return nil, "", err
	}

	events := make([]UsageEvent, 0, len(result.Entries))
	for _, entry := range result.Entries {
		event, err := DecodeUsageEvent(entry.Value)
		if err != nil {
			return nil, "", err
		}
		events = append(events, event)
	}
	return events, string(result.NextCursor), nil
}

func (s *Service) ExportEvents(
	ctx context.Context,
	tenant string,
	cursor string,
	limit uint32,
	handler func(UsageEvent) error,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if limit == 0 {
		return "", errors.New("limit must be greater than zero")
	}

	req := storage.ScanRequest{
		Cursor:        types.Key(cursor),
		Prefix:        types.Key(EventPrefix(tenant)),
		Limit:         limit,
		IncludeValues: true,
		MaxValueBytes: maxEventValueBytes,
	}
	result, err := s.db.Scan(req)
	if err != nil {
		return "", err
	}

	for _, entry := range result.Entries {
		event, err := DecodeUsageEvent(entry.Value)
		if err != nil {
			return "", err
		}
		if handler != nil {
			if err := handler(event); err != nil {
				return "", err
			}
		}
	}
	return string(result.NextCursor), nil
}

func (s *Service) ImportEvents(ctx context.Context, events []UsageEvent) (ImportStats, error) {
	if err := ctx.Err(); err != nil {
		return ImportStats{}, err
	}
	stats := ImportStats{}
	for _, event := range events {
		_, existed, err := s.recordUsage(ctx, event)
		if err != nil {
			return stats, err
		}
		if existed {
			stats.Skipped++
		} else {
			stats.Imported++
		}
	}
	return stats, nil
}

func (s *Service) PutPricePlan(ctx context.Context, plan PricePlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if plan.ID == "" {
		return errors.New("plan id is required")
	}
	if plan.UnitPrice <= 0 {
		return errors.New("unit_price must be greater than zero")
	}
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = s.now()
	}

	encoded, err := EncodePricePlan(plan)
	if err != nil {
		return err
	}
	txn := transaction.NewTransaction()
	txn.Put(types.Key(PricePlanKey(plan.ID)), types.Value(encoded))
	return s.db.ExecuteTransaction(txn)
}

func (s *Service) GetPricePlan(ctx context.Context, planID string) (PricePlan, error) {
	if err := ctx.Err(); err != nil {
		return PricePlan{}, err
	}
	if planID == "" {
		return PricePlan{}, errors.New("plan id is required")
	}
	data, err := s.db.Get([]byte(PricePlanKey(planID)))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return PricePlan{}, ErrPricePlanNotFound
		}
		return PricePlan{}, err
	}
	return DecodePricePlan(data)
}

func (s *Service) recordUsage(ctx context.Context, event UsageEvent) (LedgerEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return LedgerEntry{}, false, err
	}
	if event.ID == "" || event.Tenant == "" || event.AccountID == "" {
		return LedgerEntry{}, false, errors.New("id, tenant, and account_id are required")
	}
	if event.Units <= 0 {
		return LedgerEntry{}, false, errors.New("units must be greater than zero")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = s.now()
	}

	entryKey := EntryKey(event.ID)
	if data, err := s.db.Get([]byte(entryKey)); err == nil {
		entry, err := DecodeLedgerEntry(data)
		if err != nil {
			return LedgerEntry{}, true, err
		}
		return entry, true, nil
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return LedgerEntry{}, false, err
	}

	if err := s.ensureAccountExists(event.Tenant, event.AccountID); err != nil {
		return LedgerEntry{}, false, err
	}

	unitPrice := event.UnitPrice
	var plan PricePlan
	if unitPrice <= 0 && event.PricePlanID != "" {
		plan, err := s.GetPricePlan(ctx, event.PricePlanID)
		if err != nil {
			return LedgerEntry{}, false, err
		}
		unitPrice = plan.UnitPrice
	}
	if unitPrice <= 0 {
		return LedgerEntry{}, false, ErrPricePlanNotFound
	}

	cost, err := safeMul(event.Units, unitPrice)
	if err != nil {
		return LedgerEntry{}, false, err
	}

	balance, err := s.loadBalance(event.Tenant, event.AccountID)
	if err != nil {
		return LedgerEntry{}, false, err
	}
	nextBalance, err := safeSub(balance.Amount, cost)
	if err != nil {
		return LedgerEntry{}, false, err
	}
	if !s.allowOverdraft && nextBalance < 0 {
		return LedgerEntry{}, false, ErrInsufficientCredits
	}

	event.UnitPrice = unitPrice
	event.Cost = cost

	lastHash, err := s.getAccountHash(event.Tenant, event.AccountID)
	if err != nil {
		return LedgerEntry{}, false, err
	}
	event.PrevHash = lastHash
	event.Hash = computeEventHash(event, lastHash)

	entry := LedgerEntry{
		EventID:      event.ID,
		Tenant:       event.Tenant,
		AccountID:    event.AccountID,
		Units:        event.Units,
		UnitPrice:    unitPrice,
		Cost:         cost,
		BalanceAfter: nextBalance,
		Timestamp:    event.Timestamp,
	}

	eventBytes, err := EncodeUsageEvent(event)
	if err != nil {
		return LedgerEntry{}, false, err
	}
	entryBytes, err := EncodeLedgerEntry(entry)
	if err != nil {
		return LedgerEntry{}, false, err
	}

	balance.Amount = nextBalance
	balance.UpdatedAt = s.now()
	balanceBytes, err := EncodeBalance(balance)
	if err != nil {
		return LedgerEntry{}, false, err
	}

	txn := transaction.NewTransaction()
	txn.Put(types.Key(EventKey(event.Tenant, event.Timestamp, event.ID)), types.Value(eventBytes))
	txn.Put(types.Key(AccountEventKey(event.Tenant, event.AccountID, event.Timestamp, event.ID)), types.Value(eventBytes))
	txn.Put(types.Key(entryKey), types.Value(entryBytes))
	txn.Put(types.Key(BalanceKey(event.Tenant, event.AccountID)), types.Value(balanceBytes))
	txn.Put(types.Key(AccountHashKey(event.Tenant, event.AccountID)), types.Value([]byte(event.Hash)))

	if event.PricePlanID != "" || unitPrice > 0 {
		snapshotBytes, err := encodePriceSnapshot(priceSnapshot{
			PricePlanID: event.PricePlanID,
			UnitPrice:   unitPrice,
			Unit:        plan.Unit,
			CapturedAt:  s.now(),
		})
		if err != nil {
			return LedgerEntry{}, false, err
		}
		txn.Put(types.Key(PriceSnapshotKey(event.ID)), types.Value(snapshotBytes))
	}

	if err := s.db.ExecuteTransaction(txn); err != nil {
		return LedgerEntry{}, false, err
	}
	return entry, false, nil
}

func (s *Service) ensureAccountExists(tenant string, accountID string) error {
	_, err := s.db.Get([]byte(AccountKey(tenant, accountID)))
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrKeyNotFound) {
		return ErrAccountNotFound
	}
	return err
}

func (s *Service) loadBalance(tenant string, accountID string) (Balance, error) {
	data, err := s.db.Get([]byte(BalanceKey(tenant, accountID)))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return Balance{}, ErrAccountNotFound
		}
		return Balance{}, err
	}
	return DecodeBalance(data)
}

func (s *Service) getAccountHash(tenant string, accountID string) (string, error) {
	data, err := s.db.Get([]byte(AccountHashKey(tenant, accountID)))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

func safeAdd(a int64, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errors.New("value out of range")
	}
	return a + b, nil
}

func safeSub(a int64, b int64) (int64, error) {
	if (b > 0 && a < math.MinInt64+b) || (b < 0 && a > math.MaxInt64+b) {
		return 0, errors.New("value out of range")
	}
	return a - b, nil
}

func safeMul(a int64, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == math.MinInt64 || b == math.MinInt64 {
		return 0, errors.New("value out of range")
	}
	if abs(a) > math.MaxInt64/abs(b) {
		return 0, errors.New("value out of range")
	}
	return a * b, nil
}

func abs(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

type priceSnapshot struct {
	PricePlanID string    `json:"price_plan_id,omitempty"`
	UnitPrice   int64     `json:"unit_price"`
	Unit        string    `json:"unit,omitempty"`
	CapturedAt  time.Time `json:"captured_at"`
}

func encodePriceSnapshot(snapshot priceSnapshot) ([]byte, error) {
	return json.Marshal(snapshot)
}

func computeEventHash(event UsageEvent, prevHash string) string {
	input := fmt.Sprintf(
		"%s|%s|%s|%d|%d|%d|%d|%s",
		event.ID,
		event.Tenant,
		event.AccountID,
		event.Timestamp.UnixNano(),
		event.Units,
		event.UnitPrice,
		event.Cost,
		prevHash,
	)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}
