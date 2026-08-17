package storage

import (
	"encoding/binary"
	"os"
)

type Index struct {
	file       *os.File
	mmap       []byte
	baseOffset uint64
	size       int64
	path       string
}

func NewIndex(path string, baseOffset uint64) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := info.Size()
	var m []byte
	if size > 0 {
		m, err = mapFile(f, int(size))
		if err != nil {
			f.Close()
			return nil, err
		}
	}

	return &Index{
		file:       f,
		mmap:       m,
		baseOffset: baseOffset,
		size:       size,
		path:       path,
	}, nil
}

func (i *Index) Write(offset uint64, position uint64) error {
	relOffset := uint32(offset - i.baseOffset)
	pos := uint32(position)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], relOffset)
	binary.BigEndian.PutUint32(buf[4:8], pos)

	if _, err := i.file.WriteAt(buf, i.size); err != nil {
		return err
	}

	i.size += 8

	// remap
	if err := munmapFile(i.mmap); err != nil {
		return err
	}
	m, err := mapFile(i.file, int(i.size))
	if err != nil {
		return err
	}
	i.mmap = m

	return nil
}

func (i *Index) Lookup(targetOffset uint64) (uint64, error) {
	if len(i.mmap) == 0 || targetOffset < i.baseOffset {
		return 0, nil
	}

	targetRel := uint32(targetOffset - i.baseOffset)
	numEntries := len(i.mmap) / 8

	low, high := 0, numEntries-1
	bestPos := uint32(0)

	for low <= high {
		mid := low + (high-low)/2
		offsetBytes := i.mmap[mid*8 : mid*8+4]
		rel := binary.BigEndian.Uint32(offsetBytes)

		if rel <= targetRel {
			posBytes := i.mmap[mid*8+4 : mid*8+8]
			bestPos = binary.BigEndian.Uint32(posBytes)
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return uint64(bestPos), nil
}

func (i *Index) Close() error {
	if err := munmapFile(i.mmap); err != nil {
		return err
	}
	return i.file.Close()
}
