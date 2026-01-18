package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runCompact(state *cliState, args []string) error {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	var maxEntries uint
	var maxBytes uint64
	var tempPath string
	fs.UintVar(&maxEntries, "max-entries", 0, "max entries")
	fs.Uint64Var(&maxBytes, "max-bytes", 0, "max bytes")
	fs.StringVar(&tempPath, "temp-path", "", "temp path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if maxEntries == 0 || maxBytes == 0 {
		return errors.New("compact requires --max-entries and --max-bytes")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	stats, err := state.compact(ctx, client.CompactOptions{
		MaxEntries: uint32(maxEntries),
		MaxBytes:   maxBytes,
		TempPath:   tempPath,
	})
	if err != nil {
		return err
	}
	out, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
