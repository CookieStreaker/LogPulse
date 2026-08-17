package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	workers  = flag.Int("workers", 25, "Number of concurrent workers")
	messages = flag.Int("messages", 10000, "Total number of messages to send")
	payload  = flag.Int("payload", 1024, "Payload size in bytes")
	protocol = flag.String("protocol", "http", "Protocol to test: 'http' or 'tcp'")
	httpAddr = flag.String("http-addr", "http://localhost:8080", "Broker HTTP Base URL")
	tcpAddr  = flag.String("tcp-addr", "localhost:9092", "TCP server address")
	topic    = flag.String("topic", "orders", "Target topic name")
)

type ProduceReq struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	// Normalize URL: remove any accidental /api/produce suffix
	baseURL := strings.TrimRight(*httpAddr, "/")
	baseURL = strings.TrimSuffix(baseURL, "/api/produce")
	baseURL = strings.TrimSuffix(baseURL, "/api/topics")
	produceURL := baseURL + "/api/produce"

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Starting Mini-Kafka Benchmark Suite\n")
	fmt.Printf("Protocol: %s\n", strings.ToUpper(*protocol))
	fmt.Printf("Workers : %d\n", *workers)
	fmt.Printf("Messages: %d\n", *messages)
	fmt.Printf("Payload : %d bytes\n", *payload)
	fmt.Printf("Target  : %s\n", func() string {
		if *protocol == "http" {
			return produceURL
		}
		return *tcpAddr
	}())
	fmt.Println(strings.Repeat("=", 60))

	// Pre-generate a safe JSON payload
	valString := strings.Repeat("M", *payload)
	reqObj := ProduceReq{
		Topic: *topic,
		Key:   "bench_key",
		Value: valString,
	}
	jsonBytes, err := json.Marshal(reqObj)
	if err != nil {
		fmt.Printf("Failed to marshal JSON: %v\n", err)
		return
	}

	jobs := make(chan int, *messages)
	for i := 0; i < *messages; i++ {
		jobs <- i
	}
	close(jobs)

	latencies := make([]time.Duration, *messages)
	var successCount int64
	var errorCount int64

	tr := &http.Transport{
		MaxIdleConns:        *workers * 4,
		MaxIdleConnsPerHost: *workers * 4,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
	}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var conn net.Conn
			if *protocol == "tcp" {
				var dialErr error
				conn, dialErr = net.DialTimeout("tcp", *tcpAddr, 3*time.Second)
				if dialErr != nil {
					atomic.AddInt64(&errorCount, int64(*messages / *workers))
					return
				}
				defer conn.Close()
			}

			for idx := range jobs {
				start := time.Now()
				var sendErr error

				if *protocol == "http" {
					req, _ := http.NewRequest("POST", produceURL, bytes.NewReader(jsonBytes))
					req.Header.Set("Content-Type", "application/json")
					resp, doErr := client.Do(req)
					if doErr == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode >= 200 && resp.StatusCode < 300 {
							sendErr = nil
						} else {
							sendErr = fmt.Errorf("status %d", resp.StatusCode)
						}
					} else {
						sendErr = doErr
					}
				} else if *protocol == "tcp" {
					if conn == nil {
						sendErr = fmt.Errorf("no tcp conn")
					} else {
						sendErr = sendTCPMessage(conn, *topic, []byte(valString), uint32(idx))
					}
				}

				dur := time.Since(start)

				if sendErr == nil {
					latencies[idx] = dur
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&errorCount, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	totalBytes := int64(*payload) * successCount
	sec := elapsed.Seconds()
	if sec == 0 {
		sec = 0.0001
	}
	throughputMsg := float64(successCount) / sec
	throughputMB := (float64(totalBytes) / (1024 * 1024)) / sec

	validLatencies := make([]time.Duration, 0, successCount)
	var totalLatency time.Duration
	for i := 0; i < *messages; i++ {
		if latencies[i] > 0 {
			validLatencies = append(validLatencies, latencies[i])
			totalLatency += latencies[i]
		}
	}

	var avg, p50, p90, p99 time.Duration
	if len(validLatencies) > 0 {
		sort.Slice(validLatencies, func(i, j int) bool { return validLatencies[i] < validLatencies[j] })
		avg = time.Duration(int64(totalLatency) / int64(len(validLatencies)))
		p50 = validLatencies[int(float64(len(validLatencies))*0.50)]
		p90 = validLatencies[int(float64(len(validLatencies))*0.90)]
		p99 = validLatencies[int(float64(len(validLatencies))*0.99)]
	}

	fmt.Printf("\n+------------------------------------------------------+\n")
	fmt.Printf("| %-52s |\n", "BENCHMARK RESULTS (MINI-KAFKA BROKER)")
	fmt.Printf("+------------------------------------------------------+\n")
	fmt.Printf("| %-25s | %-24s |\n", "Metric", "Value")
	fmt.Printf("+------------------------------------------------------+\n")
	fmt.Printf("| %-25s | %-24d |\n", "Total Messages Sent", *messages)
	fmt.Printf("| %-25s | %-24d |\n", "Successful Deliveries", successCount)
	fmt.Printf("| %-25s | %-24d |\n", "Failed Deliveries", errorCount)
	fmt.Printf("| %-25s | %-24.2fs |\n", "Total Elapsed Time", sec)
	fmt.Printf("+------------------------------------------------------+\n")
	fmt.Printf("| %-25s | %-24s |\n", "Throughput (Msgs/sec)", fmt.Sprintf("%.2f", throughputMsg))
	fmt.Printf("| %-25s | %-24s |\n", "Throughput (MB/sec)", fmt.Sprintf("%.2f", throughputMB))
	fmt.Printf("+------------------------------------------------------+\n")
	fmt.Printf("| %-25s | %-24s |\n", "Average Latency", avg.String())
	fmt.Printf("| %-25s | %-24s |\n", "p50 Latency", p50.String())
	fmt.Printf("| %-25s | %-24s |\n", "p90 Latency", p90.String())
	fmt.Printf("| %-25s | %-24s |\n", "p99 Latency", p99.String())
	fmt.Printf("+------------------------------------------------------+\n")
}

func sendTCPMessage(conn net.Conn, topicName string, payload []byte, corrID uint32) error {
	var buf bytes.Buffer
	key := []byte("bench_key")

	_ = binary.Write(&buf, binary.BigEndian, uint16(0))
	_ = binary.Write(&buf, binary.BigEndian, corrID)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(topicName)))
	buf.WriteString(topicName)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(key)))
	buf.Write(key)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(payload)))
	buf.Write(payload)

	var frame bytes.Buffer
	_ = binary.Write(&frame, binary.BigEndian, uint32(buf.Len()))
	frame.Write(buf.Bytes())

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(frame.Bytes()); err != nil {
		return err
	}

	var respLen uint32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return err
	}
	respBody := make([]byte, respLen)
	_, err := io.ReadFull(conn, respBody)
		return err
	}