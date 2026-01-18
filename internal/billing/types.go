package billing

import "time"

type Account struct {
	Tenant    string                 `json:"tenant"`
	ID        string                 `json:"id"`
	CreatedAt time.Time              `json:"created_at"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type UsageEvent struct {
	ID          string                 `json:"id"`
	Tenant      string                 `json:"tenant"`
	AccountID   string                 `json:"account_id"`
	Timestamp   time.Time              `json:"timestamp"`
	Units       int64                  `json:"units"`
	PricePlanID string                 `json:"price_plan_id,omitempty"`
	UnitPrice   int64                  `json:"unit_price,omitempty"`
	Cost        int64                  `json:"cost,omitempty"`
	PrevHash    string                 `json:"prev_hash,omitempty"`
	Hash        string                 `json:"hash,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type PricePlan struct {
	ID        string    `json:"id"`
	UnitPrice int64     `json:"unit_price"`
	Unit      string    `json:"unit"`
	CreatedAt time.Time `json:"created_at"`
}

type LedgerEntry struct {
	EventID      string    `json:"event_id"`
	Tenant       string    `json:"tenant"`
	AccountID    string    `json:"account_id"`
	Units        int64     `json:"units"`
	UnitPrice    int64     `json:"unit_price"`
	Cost         int64     `json:"cost"`
	BalanceAfter int64     `json:"balance_after"`
	Timestamp    time.Time `json:"timestamp"`
}

type Balance struct {
	Tenant    string    `json:"tenant"`
	AccountID string    `json:"account_id"`
	Amount    int64     `json:"amount"`
	UpdatedAt time.Time `json:"updated_at"`
}

type SyncCursor string

type ImportStats struct {
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
}
