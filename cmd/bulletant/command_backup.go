package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runBackup(state *cliState, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	var directory string
	var includeWAL bool
	fs.StringVar(&directory, "directory", "", "backup directory")
	fs.BoolVar(&includeWAL, "include-wal", false, "include WAL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if directory == "" {
		return errors.New("backup requires --directory")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	stats, err := state.backup(ctx, client.BackupOptions{
		Directory:  directory,
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
