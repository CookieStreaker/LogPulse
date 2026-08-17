package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var ErrNotFound = errors.New("message not found")

type Message struct {
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
}

type Segment struct {
	file       *os.File
	index      *Index
	baseOffset uint64
	nextOffset uint64
	size       int64
	dir        string
}

func NewSegment(dir string, baseOffset uint64) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	idx, err := NewIndex(indexPath, baseOffset)
	if err != nil {
		f.Close()
		return nil, err
	}

	s := &Segment{
		file:       f,
		index:      idx,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       0,
		dir:        dir,
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	s.size = info.Size()

	if s.size > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		var offsetCount uint64 = 0
		var pos int64 = 0
		for pos < s.size {
			var totalLen uint32
			if err := binary.Read(f, binary.BigEndian, &totalLen); err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			if _, err := f.Seek(int64(totalLen), io.SeekCurrent); err != nil {
				return nil, err
			}
			offsetCount++
			pos += 4 + int64(totalLen)
		}
		s.nextOffset = baseOffset + offsetCount
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Segment) Append(msg *Message) error {
	msg.Offset = s.nextOffset
	msg.Timestamp = time.Now().UnixMilli()

	keyLen := uint16(len(msg.Key))
	valLen := uint32(len(msg.Value))
	totalLen := uint32(8 + 2 + len(msg.Key) + 4 + len(msg.Value))

	buf := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint64(buf[4:12], uint64(msg.Timestamp))
	binary.BigEndian.PutUint16(buf[12:14], keyLen)
	copy(buf[14:14+keyLen], msg.Key)
	binary.BigEndian.PutUint32(buf[14+keyLen:18+keyLen], valLen)
	copy(buf[18+keyLen:], msg.Value)

	pos := uint64(s.size)

	if _, err := s.file.Write(buf); err != nil {
		return err
	}

	if err := s.index.Write(msg.Offset, pos); err != nil {
		return err
	}

	s.nextOffset++
	s.size += int64(len(buf))

	return nil
}

func (s *Segment) Read(offset uint64) (*Message, error) {
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, ErrNotFound
	}
	msgs, err := s.ReadFrom(offset, 1)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, ErrNotFound
	}
	return msgs[0], nil
}

func (s *Segment) ReadFrom(offset uint64, maxCount int) ([]*Message, error) {
	if offset < s.baseOffset {
		offset = s.baseOffset
	}
	if offset >= s.nextOffset {
		return nil, nil // no messages
	}

	pos, err := s.index.Lookup(offset)
	if err != nil {
		return nil, err
	}

	if _, err := s.file.Seek(int64(pos), io.SeekStart); err != nil {
		return nil, err
	}

	// The index Lookup returns the byte position of the message with the
	// largest offset <= target. Since we index every message, this is exact
	// for offsets that exist. We scan forward from there, skipping any
	// messages before our target offset (which can happen when pos=0 and
	// the target is after the first message).
	currentOff := s.baseOffset
	if pos > 0 {
		// Non-zero position means the index found an entry. Since we write
		// one entry per message and Lookup finds the largest offset <= target,
		// the message at this position has offset == target (or very close).
		currentOff = offset
	}

	var msgs []*Message

	for len(msgs) < maxCount && currentOff < s.nextOffset {
		var totalLen uint32
		if err := binary.Read(s.file, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		payload := make([]byte, totalLen)
		if _, err := io.ReadFull(s.file, payload); err != nil {
			return nil, err
		}

		if currentOff >= offset {
			timestamp := int64(binary.BigEndian.Uint64(payload[0:8]))
			keyLen := binary.BigEndian.Uint16(payload[8:10])
			key := make([]byte, keyLen)
			copy(key, payload[10:10+keyLen])

			valLen := binary.BigEndian.Uint32(payload[10+keyLen : 14+keyLen])
			val := make([]byte, valLen)
			copy(val, payload[14+keyLen:14+uint32(keyLen)+valLen])

			msgs = append(msgs, &Message{
				Offset:    currentOff,
				Timestamp: timestamp,
				Key:       key,
				Value:     val,
			})
		}
		currentOff++
	}

	return msgs, nil
}

func (s *Segment) IsFull(maxBytes int64) bool {
	return s.size >= maxBytes
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.file.Close()
}
