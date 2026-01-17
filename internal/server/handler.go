package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

const maxBodyBytes = 10 << 20

type handler struct {
	db *db.DB
}

type kvRequest struct {
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
}

type kvResponse struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
}

type txnRequest struct {
	Operations []txnOperation `json:"operations"`
}

type txnOperation struct {
	Type     string `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

type txnResponse struct {
	Status string `json:"status"`
}

type vectorRequest struct {
	Values   []float64              `json:"values"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type vectorResponse struct {
	ID       string                 `json:"id"`
	Values   []float64              `json:"values,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

func NewHandler(db *db.DB) http.Handler {
	h := &handler{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/txn", h.handleTxn)
	mux.HandleFunc("/vectors", h.handleVectors)
	mux.HandleFunc("/vectors/", h.handleVectorByID)
	return mux
}

func (h *handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":    "bulletant",
		"status":  "ok",
		"message": "ready",
	})
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (h *handler) handleKV(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromPath("/kv/", r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := h.db.Get([]byte(key))
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		if r.URL.Query().Get("raw") == "1" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(value)
			return
		}
		writeJSON(w, http.StatusOK, kvResponse{
			Key:      key,
			Value:    base64.StdEncoding.EncodeToString(value),
			Encoding: "base64",
		})
	case http.MethodPut, http.MethodPost:
		value, err := readValue(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.db.Put([]byte(key), value); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.db.Delete([]byte(key)); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *handler) handleTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req txnRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if len(req.Operations) == 0 {
		writeError(w, http.StatusBadRequest, "no operations provided")
		return
	}

	txn := transaction.NewTransaction()
	for _, op := range req.Operations {
		opType, err := types.ParseOperationType(op.Type)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		switch opType {
		case types.Put:
			value, err := decodeValue(op.Value, op.Encoding)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			txn.Put(types.Key(op.Key), types.Value(value))
		case types.Delete:
			txn.Delete(types.Key(op.Key))
		default:
			writeError(w, http.StatusBadRequest, "only put and delete are supported")
			return
		}
	}

	if err := h.db.ExecuteTransaction(txn); err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, txnResponse{
		Status: txn.Status.String(),
	})
}

func (h *handler) handleVectors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req vectorRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Values) == 0 {
		writeError(w, http.StatusBadRequest, "values are required")
		return
	}

	id, err := h.db.AddVector(req.Values, req.Metadata)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, vectorResponse{
		ID: id,
	})
}

func (h *handler) handleVectorByID(w http.ResponseWriter, r *http.Request) {
	id, err := keyFromPath("/vectors/", r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		vector, err := h.db.GetVector(id)
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		writeJSON(w, http.StatusOK, vectorResponse{
			ID:       vector.ID,
			Values:   vector.Values,
			Metadata: vector.Metadata,
		})
	case http.MethodDelete:
		if err := h.db.DeleteVector(id); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func readValue(r *http.Request) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var req kvRequest
		if err := decodeJSON(r, &req); err != nil {
			return nil, err
		}
		return decodeValue(req.Value, req.Encoding)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func decodeValue(value string, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "text":
		return []byte(value), nil
	case "base64", "b64":
		return base64.StdEncoding.DecodeString(value)
	default:
		return nil, errors.New("unsupported value encoding")
	}
}

func decodeJSON(r *http.Request, dest any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dest)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, storage.ErrKeyNotFound),
		errors.Is(err, storage.ErrVectorNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrKeyLocked):
		return http.StatusConflict
	case errors.Is(err, storage.ErrReservedValue):
		return http.StatusConflict
	case errors.Is(err, storage.ErrUnsupported):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

func keyFromPath(prefix string, path string) (string, error) {
	if !strings.HasPrefix(path, prefix) {
		return "", errors.New("invalid path")
	}
	raw := strings.TrimPrefix(path, prefix)
	if raw == "" {
		return "", errors.New("missing key")
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", err
	}
	return decoded, nil
}
