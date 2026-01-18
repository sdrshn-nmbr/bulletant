package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
)

func runBillingBalance(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-balance", flag.ContinueOnError)
	var tenant string
	var accountID string
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "account-id", "", "account id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" {
		return errors.New("billing-balance requires --tenant and --account-id")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	balance, err := state.getBalance(ctx, tenant, accountID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(balance)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
