package storage

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type CommitLog struct {
	dir             string
	segments        []*Segment
	activeSegment   *Segment
	mu              sync.RWMutex
	maxSegmentBytes int64
}

func NewCommitLog(dir string, maxSegmentBytes int64) (*CommitLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}

	var baseOffsets []uint64
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".log")
		offset, err := strconv.ParseUint(name, 10, 64)
		if err == nil {
			baseOffsets = append(baseOffsets, offset)
		}
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	cl := &CommitLog{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
	}

	for _, off := range baseOffsets {
		seg, err := NewSegment(dir, off)
		if err != nil {
			return nil, err
		}
		cl.segments = append(cl.segments, seg)
	}

	if len(cl.segments) == 0 {
		seg, err := NewSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		cl.segments = append(cl.segments, seg)
	}
	cl.activeSegment = cl.segments[len(cl.segments)-1]

	return cl, nil
}

func (cl *CommitLog) Append(key, value []byte) (uint64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.activeSegment.IsFull(cl.maxSegmentBytes) {
		seg, err := NewSegment(cl.dir, cl.activeSegment.nextOffset)
		if err != nil {
			return 0, err
		}
		cl.segments = append(cl.segments, seg)
		cl.activeSegment = seg
	}

	msg := &Message{
		Key:   key,
		Value: value,
	}
	if err := cl.activeSegment.Append(msg); err != nil {
		return 0, err
	}

	return msg.Offset, nil
}

func (cl *CommitLog) Read(offset uint64) (*Message, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	var targetSeg *Segment
	for i := len(cl.segments) - 1; i >= 0; i-- {
		if cl.segments[i].baseOffset <= offset {
			targetSeg = cl.segments[i]
			break
		}
	}
	if targetSeg == nil {
		return nil, ErrNotFound
	}
	return targetSeg.Read(offset)
}

func (cl *CommitLog) ReadFrom(offset uint64, maxCount int) ([]*Message, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	var result []*Message
	currentOff := offset

	for i := 0; i < len(cl.segments) && len(result) < maxCount; i++ {
		seg := cl.segments[i]
		if seg.nextOffset <= currentOff {
			continue // skip segments that end before currentOff
		}
		
		msgs, err := seg.ReadFrom(currentOff, maxCount-len(result))
		if err != nil {
			return nil, err
		}
		result = append(result, msgs...)
		currentOff += uint64(len(msgs))
	}

	return result, nil
}

func (cl *CommitLog) NewestOffset() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if cl.activeSegment.nextOffset == 0 {
		return 0
	}
	return cl.activeSegment.nextOffset - 1
}

func (cl *CommitLog) OldestOffset() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.segments[0].baseOffset
}

func (cl *CommitLog) MessageCount() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.activeSegment.nextOffset
}

func (cl *CommitLog) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, seg := range cl.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}
