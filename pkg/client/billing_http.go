package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
)

func (c *HTTPClient) CreateAccount(
	ctx context.Context,
	account Account,
	opts RequestOptions,
) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if account.Tenant == "" || account.ID == "" {
		return Account{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"tenant":   account.Tenant,
		"id":       account.ID,
		"metadata": account.Metadata,
	})
	if err != nil {
		return Account{}, err
	}
	requestURL := c.buildURL("/billing/accounts", nil)
	var created Account
	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &created)
	})
	if err != nil {
		return Account{}, err
	}
	return created, nil
}

func (c *HTTPClient) GetAccount(
	ctx context.Context,
	tenant string,
	accountID string,
	opts RequestOptions,
) (Account, error) {
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	if tenant == "" || accountID == "" {
		return Account{}, ErrInvalidArgument
	}

	query := url.Values{}
	query.Set("tenant", tenant)
	requestURL := c.buildURL("/billing/accounts/"+url.PathEscape(accountID), query)
	var account Account
	err := c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &account)
	})
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

func (c *HTTPClient) CreditAccount(
	ctx context.Context,
	tenant string,
	accountID string,
	amount int64,
	opts RequestOptions,
) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" || amount <= 0 {
		return Balance{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"tenant":     tenant,
		"account_id": accountID,
		"amount":     amount,
	})
	if err != nil {
		return Balance{}, err
	}

	requestURL := c.buildURL("/billing/credits", nil)
	var balance Balance
	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &balance)
	})
	if err != nil {
		return Balance{}, err
	}
	return balance, nil
}

func (c *HTTPClient) GetBalance(
	ctx context.Context,
	tenant string,
	accountID string,
	opts RequestOptions,
) (Balance, error) {
	if err := ctx.Err(); err != nil {
		return Balance{}, err
	}
	if tenant == "" || accountID == "" {
		return Balance{}, ErrInvalidArgument
	}

	query := url.Values{}
	query.Set("tenant", tenant)
	requestURL := c.buildURL("/billing/accounts/"+url.PathEscape(accountID)+"/balance", query)
	var balance Balance
	err := c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &balance)
	})
	if err != nil {
		return Balance{}, err
	}
	return balance, nil
}

func (c *HTTPClient) RecordUsage(
	ctx context.Context,
	event UsageEvent,
	opts RequestOptions,
) (LedgerEntry, error) {
	if err := ctx.Err(); err != nil {
		return LedgerEntry{}, err
	}
	if event.ID == "" || event.Tenant == "" || event.AccountID == "" {
		return LedgerEntry{}, ErrInvalidArgument
	}
	if event.Units <= 0 {
		return LedgerEntry{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return LedgerEntry{}, err
	}

	requestURL := c.buildURL("/billing/usage", nil)
	var entry LedgerEntry
	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &entry)
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	return entry, nil
}

func (c *HTTPClient) ListUsageEvents(
	ctx context.Context,
	tenant string,
	accountID string,
	cursor string,
	limit uint32,
	opts RequestOptions,
) ([]UsageEvent, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if tenant == "" || accountID == "" || limit == 0 {
		return nil, "", ErrInvalidArgument
	}

	query := url.Values{}
	query.Set("tenant", tenant)
	query.Set("account_id", accountID)
	query.Set("limit", formatUint(limit))
	if cursor != "" {
		query.Set("cursor", cursor)
	}

	requestURL := c.buildURL("/billing/usage", query)
	var payload struct {
		Events     []UsageEvent `json:"events"`
		NextCursor string       `json:"next_cursor,omitempty"`
	}
	err := c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &payload)
	})
	if err != nil {
		return nil, "", err
	}
	return payload.Events, payload.NextCursor, nil
}

func (c *HTTPClient) ExportEvents(
	ctx context.Context,
	tenant string,
	cursor string,
	limit uint32,
	opts RequestOptions,
	handler func(UsageEvent) error,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if handler == nil || limit == 0 {
		return "", ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"tenant": tenant,
		"cursor": cursor,
		"limit":  limit,
	})
	if err != nil {
		return "", err
	}

	requestURL := c.buildURL("/billing/sync/export", nil)
	streamOpts := opts
	streamOpts.DisableRetry = true
	var nextCursor string

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, streamOpts, func(resp *http.Response) error {
		return c.decodeBillingExportStream(resp, handler, &nextCursor)
	})
	if err != nil {
		return "", err
	}
	return nextCursor, nil
}

func (c *HTTPClient) ImportEvents(
	ctx context.Context,
	events []UsageEvent,
	opts RequestOptions,
) (ImportStats, error) {
	if err := ctx.Err(); err != nil {
		return ImportStats{}, err
	}
	if len(events) == 0 {
		return ImportStats{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(events)
	if err != nil {
		return ImportStats{}, err
	}

	requestURL := c.buildURL("/billing/sync/import", nil)
	var stats ImportStats
	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeBillingResponse(resp, &stats)
	})
	if err != nil {
		return ImportStats{}, err
	}
	return stats, nil
}

type billingStreamEnvelope struct {
	Type       string `json:"type,omitempty"`
	NextCursor string `json:"next_cursor,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (c *HTTPClient) decodeBillingResponse(resp *http.Response, dest any) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return err
	}
	return nil
}

func (c *HTTPClient) decodeBillingExportStream(
	resp *http.Response,
	handler func(UsageEvent) error,
	nextCursor *string,
) error {
	limited := &io.LimitedReader{
		R: resp.Body,
		N: int64(c.maxResponseBytes),
	}
	decoder := json.NewDecoder(limited)

	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				if limited.N == 0 {
					return ErrResponseTooLarge
				}
				return nil
			}
			return err
		}

		var envelope billingStreamEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
		if envelope.Type == "cursor" {
			*nextCursor = envelope.NextCursor
			return nil
		}
		if envelope.Type == "error" {
			if envelope.Error == "" {
				return ErrRequestFailed
			}
			return errors.New(envelope.Error)
		}

		var event UsageEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if err := handler(event); err != nil {
			return err
		}
	}
}
