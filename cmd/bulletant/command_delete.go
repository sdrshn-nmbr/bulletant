package main

import (
	"errors"
	"flag"
)

func runDelete(state *cliState, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: delete <key>")
	}

	key, err := decodeInput(fs.Arg(0), state.keyEncoding)
	if err != nil {
		return err
	}
	ctx, cancel := state.withContext()
	defer cancel()
	return state.del(ctx, key)
}
