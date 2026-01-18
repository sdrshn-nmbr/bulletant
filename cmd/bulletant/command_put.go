package main

import (
	"errors"
	"flag"
)

func runPut(state *cliState, args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: put <key> <value>")
	}

	key, err := decodeInput(fs.Arg(0), state.keyEncoding)
	if err != nil {
		return err
	}
	value, err := decodeInput(fs.Arg(1), state.valueEncoding)
	if err != nil {
		return err
	}

	ctx, cancel := state.withContext()
	defer cancel()
	return state.put(ctx, key, value)
}
