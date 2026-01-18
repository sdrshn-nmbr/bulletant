package main

import (
	"fmt"
	"os"
)

func main() {
	cfg, args, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	state, err := newCLIState(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer state.Close()

	if err := dispatchCommand(state, args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
