package lsm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const manifestFileName = "manifest.json"

type segmentMeta struct {
	ID       uint64 `json:"id"`
	Level    int    `json:"level"`
	Filename string `json:"filename"`
	MinKey   string `json:"min_key"`
	MaxKey   string `json:"max_key"`
	Entries  uint32 `json:"entries"`
	Bytes    uint64 `json:"bytes"`
}

type manifest struct {
	Version  int           `json:"version"`
	NextID   uint64        `json:"next_id"`
	Segments []segmentMeta `json:"segments"`
}

func loadManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, manifestFileName)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &manifest{
				Version: 1,
				NextID:  1,
			}, nil
		}
		return nil, err
	}
	if info.IsDir() {
		return nil, ErrInvalidOptions
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.NextID == 0 {
		m.NextID = 1
	}
	return &m, nil
}

func saveManifest(dir string, m *manifest) error {
	path := filepath.Join(dir, manifestFileName)
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(m); err != nil {
		file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return os.Rename(tempPath, path)
}

func segmentPath(dir string, filename string) string {
	return filepath.Join(dir, filename)
}

func segmentFileName(id uint64) string {
	return "segment-" + formatID(id) + ".sst"
}

func formatID(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func sortSegmentsByIDDesc(segments []segmentMeta) {
	sort.Slice(segments, func(i int, j int) bool {
		return segments[i].ID > segments[j].ID
	})
}
