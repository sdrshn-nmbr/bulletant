package main

import (
	"errors"
	"flag"
	"fmt"
)

func runGet(state *cliState, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: get <key>")
	}

	key, err := decodeInput(fs.Arg(0), state.keyEncoding)
	if err != nil {
		return err
	}
	ctx, cancel := state.withContext()
	defer cancel()

	value, err := state.get(ctx, key)
	if err != nil {
		return err
	}
	encoded, err := encodeOutput(value, state.valueEncoding)
	if err != nil {
		return err
	}
	fmt.Println(encoded)
	return nil
}
