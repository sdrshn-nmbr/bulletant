package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
)

func runVectorAdd(state *cliState, args []string) error {
	fs := flag.NewFlagSet("vector-add", flag.ContinueOnError)
	var valuesRaw string
	var metadataRaw string
	fs.StringVar(&valuesRaw, "values", "", "comma-separated values")
	fs.StringVar(&metadataRaw, "metadata", "", "metadata JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if valuesRaw == "" {
		return errors.New("vector-add requires --values")
	}

	values, err := parseFloatList(valuesRaw)
	if err != nil {
		return err
	}
	metadata, err := parseMetadata(metadataRaw)
	if err != nil {
		return err
	}

	ctx, cancel := state.withContext()
	defer cancel()
	id, err := state.addVector(ctx, values, metadata)
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}

func parseFloatList(raw string) ([]float64, error) {
	parts := strings.Split(raw, ",")
	values := make([]float64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, errors.New("no values provided")
	}
	return values, nil
}

func parseMetadata(raw string) (map[string]interface{}, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}
