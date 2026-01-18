package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

const maxBillingImportBytes int64 = 10 << 20

func runBillingImport(state *cliState, args []string) error {
	fs := flag.NewFlagSet("billing-import", flag.ContinueOnError)
	var path string
	fs.StringVar(&path, "file", "", "input file (NDJSON or JSON array). defaults to stdin")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var reader io.Reader
	if path == "" {
		reader = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader = file
	}

	events, err := readUsageEvents(reader)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return errors.New("billing-import requires at least one event")
	}

	ctx, cancel := state.withContext()
	defer cancel()
	stats, err := state.importEvents(ctx, events)
	if err != nil {
		return err
	}
	out, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func readUsageEvents(input io.Reader) ([]client.UsageEvent, error) {
	reader := bufio.NewReader(io.LimitReader(input, maxBillingImportBytes))
	first, err := firstNonSpaceImportByte(reader)
	if err != nil {
		return nil, errors.New("invalid payload")
	}

	var events []client.UsageEvent
	if first == '[' {
		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(&events); err != nil {
			return nil, err
		}
		return events, nil
	}

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), int(maxBillingImportBytes))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event client.UsageEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func firstNonSpaceImportByte(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if !isSpaceImportByte(b) {
			if err := reader.UnreadByte(); err != nil {
				return 0, err
			}
			return b, nil
		}
	}
}

func isSpaceImportByte(value byte) bool {
	switch value {
	case ' ', '\n', '\r', '\t':
		return true
	default:
		return false
	}
}
