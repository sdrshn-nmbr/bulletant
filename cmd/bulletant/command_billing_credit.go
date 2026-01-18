package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
)

func runBillingCredit(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-credit", flag.ContinueOnError)
	var tenant string
	var accountID string
	var amount int64
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "account-id", "", "account id")
	fs.Int64Var(&amount, "amount", 0, "credit amount")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" || amount <= 0 {
		return errors.New("billing-credit requires --tenant, --account-id, and --amount")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	balance, err := state.creditAccount(ctx, tenant, accountID, amount)
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
