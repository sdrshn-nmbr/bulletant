package billing

import (
	"fmt"
	"time"
)

const (
	accountPrefix       = "acct"
	balancePrefix       = "bal"
	eventPrefix         = "event"
	accountEventPrefix  = "acct_event"
	entryPrefix         = "entry"
	pricePlanPrefix     = "price"
	priceSnapshotPrefix = "price_snapshot"
	accountHashPrefix   = "acct_hash"
)

func AccountKey(tenant string, accountID string) string {
	return fmt.Sprintf("%s/%s/%s", accountPrefix, tenant, accountID)
}

func BalanceKey(tenant string, accountID string) string {
	return fmt.Sprintf("%s/%s/%s", balancePrefix, tenant, accountID)
}

func EventKey(tenant string, timestamp time.Time, eventID string) string {
	return fmt.Sprintf("%s/%s/%s/%s", eventPrefix, tenant, formatTimestamp(timestamp), eventID)
}

func EventPrefix(tenant string) string {
	if tenant == "" {
		return eventPrefix + "/"
	}
	return fmt.Sprintf("%s/%s/", eventPrefix, tenant)
}

func AccountEventKey(tenant string, accountID string, timestamp time.Time, eventID string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", accountEventPrefix, tenant, accountID, formatTimestamp(timestamp), eventID)
}

func AccountEventPrefix(tenant string, accountID string) string {
	return fmt.Sprintf("%s/%s/%s/", accountEventPrefix, tenant, accountID)
}

func EntryKey(eventID string) string {
	return fmt.Sprintf("%s/%s", entryPrefix, eventID)
}

func PricePlanKey(planID string) string {
	return fmt.Sprintf("%s/%s", pricePlanPrefix, planID)
}

func PriceSnapshotKey(eventID string) string {
	return fmt.Sprintf("%s/%s", priceSnapshotPrefix, eventID)
}

func AccountHashKey(tenant string, accountID string) string {
	return fmt.Sprintf("%s/%s/%s", accountHashPrefix, tenant, accountID)
}

func formatTimestamp(ts time.Time) string {
	if ts.IsZero() {
		return "0000000000000000000"
	}
	return fmt.Sprintf("%019d", ts.UnixNano())
}
