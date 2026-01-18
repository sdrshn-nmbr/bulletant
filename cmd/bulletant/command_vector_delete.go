package main

import (
	"errors"
	"flag"
)

func runVectorDelete(state *cliState, args []string) error {
	fs := flag.NewFlagSet("vector-delete", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: vector-delete <id>")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	return state.deleteVector(ctx, fs.Arg(0))
}
