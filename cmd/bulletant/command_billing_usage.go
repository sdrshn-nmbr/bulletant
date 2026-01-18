package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runBillingUsage(state *cliState, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: billing-usage <record|list> [flags]")
	}
	switch args[0] {
	case "record":
		return runBillingUsageRecord(state, args[1:])
	case "list":
		return runBillingUsageList(state, args[1:])
	default:
		return fmt.Errorf("unknown billing-usage action: %s", args[0])
	}
}

func runBillingUsageRecord(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-usage record", flag.ContinueOnError)
	var tenant string
	var accountID string
	var eventID string
	var units int64
	var pricePlanID string
	var unitPrice int64
	var timestampRaw string
	var metadataRaw string
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "account-id", "", "account id")
	fs.StringVar(&eventID, "event-id", "", "event id")
	fs.Int64Var(&units, "units", 0, "usage units")
	fs.StringVar(&pricePlanID, "price-plan", "", "price plan id")
	fs.Int64Var(&unitPrice, "unit-price", 0, "unit price (overrides plan)")
	fs.StringVar(&timestampRaw, "timestamp", "", "timestamp RFC3339 (optional)")
	fs.StringVar(&metadataRaw, "metadata", "", "event metadata JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" || eventID == "" || units <= 0 {
		return errors.New("billing-usage record requires --tenant, --account-id, --event-id, and --units")
	}

	var ts time.Time
	if timestampRaw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, timestampRaw)
		if err != nil {
			return err
		}
		ts = parsed
	}

	var metadata map[string]interface{}
	if metadataRaw != "" {
		if err := json.Unmarshal([]byte(metadataRaw), &metadata); err != nil {
			return err
		}
	}

	ctx, cancel := state.withContext()
	defer cancel()
	entry, err := state.recordUsage(ctx, client.UsageEvent{
		ID:          eventID,
		Tenant:      tenant,
		AccountID:   accountID,
		Timestamp:   ts,
		Units:       units,
		PricePlanID: pricePlanID,
		UnitPrice:   unitPrice,
		Metadata:    metadata,
	})
	if err != nil {
		return err
	}
	out, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func runBillingUsageList(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-usage list", flag.ContinueOnError)
	var tenant string
	var accountID string
	var cursor string
	var limit uint
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "account-id", "", "account id")
	fs.StringVar(&cursor, "cursor", "", "cursor (optional)")
	fs.UintVar(&limit, "limit", 0, "max events to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" || limit == 0 {
		return errors.New("billing-usage list requires --tenant, --account-id, and --limit")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	events, nextCursor, err := state.listUsageEvents(ctx, tenant, accountID, cursor, uint32(limit))
	if err != nil {
		return err
	}
	out, err := json.Marshal(map[string]any{
		"events":      events,
		"next_cursor": nextCursor,
	})
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
