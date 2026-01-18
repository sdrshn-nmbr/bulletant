package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runSnapshot(state *cliState, args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	var path string
	var includeWAL bool
	fs.StringVar(&path, "path", "", "snapshot path")
	fs.BoolVar(&includeWAL, "include-wal", false, "include WAL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if path == "" {
		return errors.New("snapshot requires --path")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	stats, err := state.snapshot(ctx, client.SnapshotOptions{
		Path:       path,
		IncludeWAL: includeWAL,
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
