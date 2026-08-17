package topic

import (
	"fmt"
	"mini-kafka/storage"
	"os"
	"path/filepath"
)

type Partition struct {
	ID  int
	Log *storage.CommitLog
	dir string
}

func NewPartition(dir string, id int, maxSegSize int64) (*Partition, error) {
	pDir := filepath.Join(dir, fmt.Sprintf("partition-%d", id))
	if err := os.MkdirAll(pDir, 0755); err != nil {
		return nil, err
	}
	log, err := storage.NewCommitLog(pDir, maxSegSize)
	if err != nil {
		return nil, err
	}
	return &Partition{
		ID:  id,
		Log: log,
		dir: pDir,
	}, nil
}

func (p *Partition) Append(key, value []byte) (uint64, error) {
	return p.Log.Append(key, value)
}

func (p *Partition) Read(offset uint64) (*storage.Message, error) {
	return p.Log.Read(offset)
}

func (p *Partition) ReadFrom(offset uint64, maxCount int) ([]*storage.Message, error) {
	return p.Log.ReadFrom(offset, maxCount)
}

func (p *Partition) NewestOffset() uint64 {
	return p.Log.NewestOffset()
}

func (p *Partition) MessageCount() uint64 {
	return p.Log.MessageCount()
}

func (p *Partition) Close() error {
	return p.Log.Close()
}
