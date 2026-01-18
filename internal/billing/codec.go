package billing

import "encoding/json"

func EncodeAccount(account Account) ([]byte, error) {
	return json.Marshal(account)
}

func DecodeAccount(data []byte) (Account, error) {
	var account Account
	if err := json.Unmarshal(data, &account); err != nil {
		return Account{}, err
	}
	return account, nil
}

func EncodeBalance(balance Balance) ([]byte, error) {
	return json.Marshal(balance)
}

func DecodeBalance(data []byte) (Balance, error) {
	var balance Balance
	if err := json.Unmarshal(data, &balance); err != nil {
		return Balance{}, err
	}
	return balance, nil
}

func EncodeUsageEvent(event UsageEvent) ([]byte, error) {
	return json.Marshal(event)
}

func DecodeUsageEvent(data []byte) (UsageEvent, error) {
	var event UsageEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return UsageEvent{}, err
	}
	return event, nil
}

func EncodeLedgerEntry(entry LedgerEntry) ([]byte, error) {
	return json.Marshal(entry)
}

func DecodeLedgerEntry(data []byte) (LedgerEntry, error) {
	var entry LedgerEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return LedgerEntry{}, err
	}
	return entry, nil
}

func EncodePricePlan(plan PricePlan) ([]byte, error) {
	return json.Marshal(plan)
}

func DecodePricePlan(data []byte) (PricePlan, error) {
	var plan PricePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return PricePlan{}, err
	}
	return plan, nil
}
