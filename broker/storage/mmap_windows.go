//go:build windows

package storage

import (
	"io"
	"os"
)

func mapFile(f *os.File, size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	data := make([]byte, size)
	_, err := io.ReadFull(f, data)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}

func munmapFile(data []byte) error {
	return nil
}
