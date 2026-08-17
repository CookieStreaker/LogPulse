package topic

import (
	"errors"
	"hash/fnv"
	"mini-kafka/storage"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Topic struct {
	Name       string
	Partitions []*Partition
	rrCounter  atomic.Uint64
}

type Manager struct {
	dataDir    string
	topics     map[string]*Topic
	mu         sync.RWMutex
	maxSegSize int64
}

func NewManager(dataDir string, maxSegSize int64) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		dataDir:    dataDir,
		topics:     make(map[string]*Topic),
		maxSegSize: maxSegSize,
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			topicName := entry.Name()
			tDir := filepath.Join(dataDir, topicName)
			
			pEntries, err := os.ReadDir(tDir)
			if err != nil {
				continue
			}
			
			t := &Topic{
				Name: topicName,
			}
			
			for _, pe := range pEntries {
				if pe.IsDir() && strings.HasPrefix(pe.Name(), "partition-") {
					idStr := strings.TrimPrefix(pe.Name(), "partition-")
					id, err := strconv.Atoi(idStr)
					if err == nil {
						p, err := NewPartition(tDir, id, maxSegSize)
						if err == nil {
							t.Partitions = append(t.Partitions, p)
						}
					}
				}
			}
			m.topics[topicName] = t
		}
	}

	return m, nil
}

func (m *Manager) CreateTopic(name string, numPartitions int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return errors.New("topic exists")
	}

	tDir := filepath.Join(m.dataDir, name)
	if err := os.MkdirAll(tDir, 0755); err != nil {
		return err
	}

	t := &Topic{
		Name: name,
	}

	for i := 0; i < numPartitions; i++ {
		p, err := NewPartition(tDir, i, m.maxSegSize)
		if err != nil {
			return err
		}
		t.Partitions = append(t.Partitions, p)
	}

	m.topics[name] = t
	return nil
}

func (m *Manager) GetTopic(name string) (*Topic, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[name]
	return t, ok
}

func (m *Manager) ListTopics() []*Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var list []*Topic
	for _, t := range m.topics {
		list = append(list, t)
	}
	return list
}

func (m *Manager) Produce(topicName string, key, value []byte) (int, uint64, error) {
	t, ok := m.GetTopic(topicName)
	if !ok {
		return 0, 0, errors.New("unknown topic")
	}

	if len(t.Partitions) == 0 {
		return 0, 0, errors.New("no partitions")
	}

	var pID int
	if len(key) > 0 {
		h := fnv.New32a()
		h.Write(key)
		pID = int(h.Sum32()) % len(t.Partitions)
	} else {
		c := t.rrCounter.Add(1)
		pID = int(c) % len(t.Partitions)
	}

	offset, err := t.Partitions[pID].Append(key, value)
	return pID, offset, err
}

func (m *Manager) Consume(topicName string, partitionID int, offset uint64, maxCount int) ([]*storage.Message, error) {
	t, ok := m.GetTopic(topicName)
	if !ok {
		return nil, errors.New("unknown topic")
	}
	if partitionID < 0 || partitionID >= len(t.Partitions) {
		return nil, errors.New("invalid partition")
	}
	return t.Partitions[partitionID].ReadFrom(offset, maxCount)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.topics {
		for _, p := range t.Partitions {
			p.Close()
		}
	}
	return nil
}
