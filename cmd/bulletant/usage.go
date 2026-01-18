package main

import (
	"fmt"
)

func dispatchCommand(state *cliState, args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}

	switch args[0] {
	case "get":
		return runGet(state, args[1:])
	case "put":
		return runPut(state, args[1:])
	case "delete":
		return runDelete(state, args[1:])
	case "scan":
		return runScan(state, args[1:])
	case "txn":
		return runTxn(state, args[1:])
	case "vector-add":
		return runVectorAdd(state, args[1:])
	case "vector-get":
		return runVectorGet(state, args[1:])
	case "vector-delete":
		return runVectorDelete(state, args[1:])
	case "compact":
		return runCompact(state, args[1:])
	case "snapshot":
		return runSnapshot(state, args[1:])
	case "backup":
		return runBackup(state, args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func printUsage() {
	fmt.Println("usage: bulletant [global flags] <command> [command flags]")
	fmt.Println("commands: get, put, delete, scan, txn, vector-add, vector-get, vector-delete, compact, snapshot, backup")
}
