package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runScan(state *cliState, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	var limit uint
	var prefix string
	var cursor string
	var includeValues bool
	var maxValueBytes uint64
	fs.UintVar(&limit, "limit", 0, "max entries to return")
	fs.StringVar(&prefix, "prefix", "", "key prefix")
	fs.StringVar(&cursor, "cursor", "", "cursor key")
	fs.BoolVar(&includeValues, "include-values", false, "include values")
	fs.Uint64Var(&maxValueBytes, "max-value-bytes", 8<<20, "max value bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if limit == 0 {
		return errors.New("scan requires --limit")
	}

	prefixBytes, err := decodeInput(prefix, state.keyEncoding)
	if err != nil {
		return err
	}
	cursorBytes, err := decodeInput(cursor, state.keyEncoding)
	if err != nil {
		return err
	}

	req := client.ScanRequest{
		Prefix:        prefixBytes,
		Cursor:        cursorBytes,
		Limit:         uint32(limit),
		IncludeValues: includeValues,
		MaxValueBytes: uint32(maxValueBytes),
	}

	ctx, cancel := state.withContext()
	defer cancel()

	result, err := state.scan(ctx, req)
	if err != nil {
		return err
	}

	for _, entry := range result.Entries {
		keyOut, err := encodeOutput(entry.Key, state.keyEncoding)
		if err != nil {
			return err
		}
		if includeValues {
			valueOut, err := encodeOutput(entry.Value, state.valueEncoding)
			if err != nil {
				return err
			}
			fmt.Printf("%s\t%s\n", keyOut, valueOut)
		} else {
			fmt.Println(keyOut)
		}
	}
	if len(result.NextCursor) > 0 {
		nextCursor, err := encodeOutput(result.NextCursor, state.keyEncoding)
		if err != nil {
			return err
		}
		fmt.Printf("next_cursor=%s\n", nextCursor)
	}
	return nil
}
