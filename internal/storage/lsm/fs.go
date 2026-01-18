package lsm

import (
	"io"
	"os"
)

func copyFileAtomic(srcPath string, tempPath string, destPath string) (uint64, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer src.Close()

	dest, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return 0, err
	}

	written, err := io.Copy(dest, src)
	if err != nil {
		dest.Close()
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := dest.Sync(); err != nil {
		dest.Close()
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := dest.Close(); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}

	if err := os.Rename(tempPath, destPath); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}

	return uint64(written), nil
}
