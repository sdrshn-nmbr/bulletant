package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func runBillingAccount(state *cliState, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: billing-account <create|get> [flags]")
	}
	switch args[0] {
	case "create":
		return runBillingAccountCreate(state, args[1:])
	case "get":
		return runBillingAccountGet(state, args[1:])
	default:
		return fmt.Errorf("unknown billing-account action: %s", args[0])
	}
}

func runBillingAccountCreate(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-account create", flag.ContinueOnError)
	var tenant string
	var accountID string
	var metadataRaw string
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "id", "", "account id")
	fs.StringVar(&metadataRaw, "metadata", "", "account metadata JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" {
		return errors.New("billing-account create requires --tenant and --id")
	}

	var metadata map[string]interface{}
	if metadataRaw != "" {
		if err := json.Unmarshal([]byte(metadataRaw), &metadata); err != nil {
			return err
		}
	}

	ctx, cancel := state.withContext()
	defer cancel()
	account, err := state.createAccount(ctx, client.Account{
		Tenant:   tenant,
		ID:       accountID,
		Metadata: metadata,
	})
	if err != nil {
		return err
	}
	out, err := json.Marshal(account)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func runBillingAccountGet(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-account get", flag.ContinueOnError)
	var tenant string
	var accountID string
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&accountID, "id", "", "account id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if tenant == "" || accountID == "" {
		return errors.New("billing-account get requires --tenant and --id")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	account, err := state.getAccount(ctx, tenant, accountID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(account)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
