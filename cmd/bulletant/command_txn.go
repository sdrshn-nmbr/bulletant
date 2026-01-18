package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runTxn(state *cliState, args []string) error {
	fs := flag.NewFlagSet("txn", flag.ContinueOnError)
	var opArgs []string
	fs.Func("op", "operation: put:<key>:<value> or delete:<key>", func(value string) error {
		opArgs = append(opArgs, value)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(opArgs) == 0 {
		return errors.New("txn requires at least one --op")
	}

	ops := make([]client.TransactionOperation, 0, len(opArgs))
	for _, raw := range opArgs {
		if strings.HasPrefix(raw, "put:") {
			parts := strings.SplitN(raw, ":", 3)
			if len(parts) != 3 {
				return errors.New("put op format: put:<key>:<value>")
			}
			key, err := decodeInput(parts[1], state.keyEncoding)
			if err != nil {
				return err
			}
			value, err := decodeInput(parts[2], state.valueEncoding)
			if err != nil {
				return err
			}
			ops = append(ops, client.TransactionOperation{
				Type:  client.OperationPut,
				Key:   key,
				Value: value,
			})
			continue
		}
		if strings.HasPrefix(raw, "delete:") {
			parts := strings.SplitN(raw, ":", 2)
			if len(parts) != 2 {
				return errors.New("delete op format: delete:<key>")
			}
			key, err := decodeInput(parts[1], state.keyEncoding)
			if err != nil {
				return err
			}
			ops = append(ops, client.TransactionOperation{
				Type: client.OperationDelete,
				Key:  key,
			})
			continue
		}
		return fmt.Errorf("unknown op: %s", raw)
	}

	ctx, cancel := state.withContext()
	defer cancel()
	status, err := state.txn(ctx, ops)
	if err != nil {
		return err
	}
	fmt.Println(string(status))
	return nil
}
