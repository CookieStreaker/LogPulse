package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"mini-kafka/consumer"
	"mini-kafka/storage"
	"mini-kafka/topic"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type HTTPServer struct {
	topicMgr    *topic.Manager
	consumerMgr *consumer.GroupManager
	server      *http.Server
	startedAt   time.Time
	msgCounter  atomic.Int64
	lastCount   int64
	lastTime    time.Time
	mu          sync.Mutex
}

func NewHTTPServer(addr string, tm *topic.Manager, cm *consumer.GroupManager) *HTTPServer {
	mux := http.NewServeMux()
	
	hs := &HTTPServer{
		topicMgr:    tm,
		consumerMgr: cm,
		startedAt:   time.Now(),
		lastTime:    time.Now(),
	}

	mux.HandleFunc("/api/stats", hs.handleStats)
	mux.HandleFunc("/api/topics", hs.handleTopics)
	mux.HandleFunc("/api/messages/", hs.handleMessages)
	mux.HandleFunc("/api/consumers", hs.handleConsumers)
	mux.HandleFunc("/api/produce", hs.handleProduce)

	hs.server = &http.Server{
		Addr:    addr,
		Handler: corsMiddleware(mux),
	}

	return hs
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) Start() error {
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	now := time.Now()
	currCount := s.msgCounter.Load()
	deltaCount := currCount - s.lastCount
	deltaSecs := now.Sub(s.lastTime).Seconds()
	
	var mps float64
	if deltaSecs > 0 {
		mps = float64(deltaCount) / deltaSecs
	}
	
	s.lastCount = currCount
	s.lastTime = now
	s.mu.Unlock()

	topics := s.topicMgr.ListTopics()
	var totalPartitions int
	var totalMessages uint64

	for _, t := range topics {
		totalPartitions += len(t.Partitions)
		for _, p := range t.Partitions {
			totalMessages += p.MessageCount()
		}
	}

	stats := map[string]interface{}{
		"broker_id":        "broker-1",
		"uptime_seconds":   int(time.Since(s.startedAt).Seconds()),
		"total_topics":     len(topics),
		"total_partitions": totalPartitions,
		"total_messages":   totalMessages,
		"messages_per_sec": math.Round(mps*100) / 100,
		"started_at":       s.startedAt.UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *HTTPServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var req struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		
		if err := s.topicMgr.CreateTopic(req.Name, req.Partitions); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "created"})
		return
	} else if r.Method == "GET" {
		topics := s.topicMgr.ListTopics()
		res := make([]map[string]interface{}, 0, len(topics))
		
		for _, t := range topics {
			var parts []map[string]interface{}
			for _, p := range t.Partitions {
				parts = append(parts, map[string]interface{}{
					"id":            p.ID,
					"messages":      p.MessageCount(),
					"newest_offset": p.NewestOffset(),
					"oldest_offset": p.Log.OldestOffset(),
				})
			}
			res = append(res, map[string]interface{}{
				"name":       t.Name,
				"partitions": parts,
			})
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func bytesToString(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func (s *HTTPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		http.Error(w, "Invalid path format. Expected /api/messages/{topic}/{partition}", http.StatusBadRequest)
		return
	}

	topicName := parts[0]
	partitionStr := parts[1]
	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	var offset uint64 = 0
	if offsetStr != "" {
		if o, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			offset = o
		}
	}

	limitStr := r.URL.Query().Get("limit")
	var limit int = 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}

	msgs, err := s.topicMgr.Consume(topicName, partitionID, offset, limit)
	if err != nil {
		if err == storage.ErrNotFound {
			msgs = []*storage.Message{}
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	type msgResp struct {
		Offset    uint64 `json:"offset"`
		Timestamp int64  `json:"timestamp"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	var msgsOut []msgResp
	for _, m := range msgs {
		msgsOut = append(msgsOut, msgResp{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       bytesToString(m.Key),
			Value:     bytesToString(m.Value),
		})
	}
	
	if msgsOut == nil {
		msgsOut = []msgResp{}
	}

	resp := map[string]interface{}{
		"topic":     topicName,
		"partition": partitionID,
		"messages":  msgsOut,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleConsumers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groups := s.consumerMgr.ListGroups()
	var res []map[string]interface{}

	for _, g := range groups {
		groupOffsets := make(map[string]map[string]interface{})
		for topicName, parts := range g.Offsets {
			topicOffsets := make(map[string]interface{})
			t, ok := s.topicMgr.GetTopic(topicName)
			
			for pID, off := range parts {
				var latest uint64 = 0
				if ok && pID >= 0 && pID < len(t.Partitions) {
					latest = t.Partitions[pID].NewestOffset()
				}
				lag := latest - off
				if off > latest {
					lag = 0
				}
				
				topicOffsets[strconv.Itoa(pID)] = map[string]interface{}{
					"committed": off,
					"latest":    latest,
					"lag":       lag,
				}
			}
			groupOffsets[topicName] = topicOffsets
		}
		
		res = append(res, map[string]interface{}{
			"group_id": g.ID,
			"offsets":  groupOffsets,
		})
	}
	
	if res == nil {
		res = []map[string]interface{}{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (s *HTTPServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading body", http.StatusBadRequest)
		return
	}

	var req struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var keyBytes []byte
	if req.Key != "" {
		keyBytes = []byte(req.Key)
	}

	pID, offset, err := s.topicMgr.Produce(req.Topic, keyBytes, []byte(req.Value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.msgCounter.Add(1)

	resp := map[string]interface{}{
		"partition": pID,
		"offset":    offset,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
