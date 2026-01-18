package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runBillingExport(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-export", flag.ContinueOnError)
	var tenant string
	var cursor string
	var limit uint
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&cursor, "cursor", "", "cursor (optional)")
	fs.UintVar(&limit, "limit", 0, "max events to export")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if limit == 0 {
		return errors.New("billing-export requires --limit")
	}

	encoder := json.NewEncoder(os.Stdout)
	ctx, cancel := state.withContext()
	defer cancel()
	nextCursor, err := state.exportEvents(ctx, tenant, cursor, uint32(limit), func(event client.UsageEvent) error {
		return encoder.Encode(event)
	})
	if err != nil {
		return err
	}
	return encoder.Encode(map[string]string{
		"type":        "cursor",
		"next_cursor": nextCursor,
	})
}
