package consumer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type GroupManager struct {
	dataDir string
	groups  map[string]*Group
	mu      sync.RWMutex
}

type Group struct {
	ID      string                   `json:"id"`
	Offsets map[string]map[int]uint64 `json:"offsets"` // topic -> partition -> offset
}

type groupJSON struct {
	ID      string                      `json:"id"`
	Offsets map[string]map[string]uint64 `json:"offsets"`
}

func NewGroupManager(dataDir string) (*GroupManager, error) {
	offsetsDir := filepath.Join(dataDir, ".offsets")
	if err := os.MkdirAll(offsetsDir, 0755); err != nil {
		return nil, err
	}

	gm := &GroupManager{
		dataDir: offsetsDir,
		groups:  make(map[string]*Group),
	}

	files, err := os.ReadDir(offsetsDir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".json") {
			path := filepath.Join(offsetsDir, f.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}

			var gj groupJSON
			if err := json.Unmarshal(data, &gj); err != nil {
				continue
			}

			g := &Group{
				ID:      gj.ID,
				Offsets: make(map[string]map[int]uint64),
			}

			for topic, pMap := range gj.Offsets {
				g.Offsets[topic] = make(map[int]uint64)
				for pStr, off := range pMap {
					pID, err := strconv.Atoi(pStr)
					if err == nil {
						g.Offsets[topic][pID] = off
					}
				}
			}

			gm.groups[g.ID] = g
		}
	}

	return gm, nil
}

func (gm *GroupManager) CommitOffset(groupID, topic string, partition int, offset uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	g, ok := gm.groups[groupID]
	if !ok {
		g = &Group{
			ID:      groupID,
			Offsets: make(map[string]map[int]uint64),
		}
		gm.groups[groupID] = g
	}

	if g.Offsets[topic] == nil {
		g.Offsets[topic] = make(map[int]uint64)
	}
	g.Offsets[topic][partition] = offset

	gj := groupJSON{
		ID:      g.ID,
		Offsets: make(map[string]map[string]uint64),
	}
	for t, pMap := range g.Offsets {
		gj.Offsets[t] = make(map[string]uint64)
		for pID, off := range pMap {
			gj.Offsets[t][strconv.Itoa(pID)] = off
		}
	}

	data, err := json.Marshal(gj)
	if err != nil {
		return err
	}

	path := filepath.Join(gm.dataDir, groupID+".json")
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func (gm *GroupManager) GetOffset(groupID, topic string, partition int) (uint64, bool) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	if g, ok := gm.groups[groupID]; ok {
		if pMap, ok2 := g.Offsets[topic]; ok2 {
			if off, ok3 := pMap[partition]; ok3 {
				return off, true
			}
		}
	}
	return 0, false
}

func (gm *GroupManager) ListGroups() []*Group {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	var list []*Group
	for _, g := range gm.groups {
		list = append(list, g)
	}
	return list
}
