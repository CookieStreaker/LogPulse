package network

import (
	"io"
	"net"
	"os"
)

func TransferFile(conn net.Conn, f *os.File, offset int64, count int64) (int64, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return io.CopyN(conn, f, count)
}

func TransferSegment(conn net.Conn, segPath string, offset int64, count int64) (int64, error) {
	f, err := os.Open(segPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return TransferFile(conn, f, offset, count)
}
