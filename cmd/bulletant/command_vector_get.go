package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
)

func runVectorGet(state *cliState, args []string) error {
	fs := flag.NewFlagSet("vector-get", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: vector-get <id>")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	vector, err := state.getVector(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	out, err := json.Marshal(vector)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
