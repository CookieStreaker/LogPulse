package network

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"mini-kafka/consumer"
	"mini-kafka/topic"
	"net"
	"sync"
)

type Server struct {
	addr        string
	listener    net.Listener
	topicMgr    *topic.Manager
	consumerMgr *consumer.GroupManager
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func NewServer(addr string, tm *topic.Manager, cm *consumer.GroupManager) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:        addr,
		topicMgr:    tm,
		consumerMgr: cm,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					log.Printf("Accept error: %v", err)
					continue
				}
			}
			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		apiKey, corrID, payload, err := ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("ReadRequest error: %v", err)
			}
			return
		}

		switch apiKey {
		case APIKeyProduce:
			s.handleProduce(corrID, payload, conn)
		case APIKeyFetch:
			s.handleFetch(corrID, payload, conn)
		case APIKeyCreateTopic:
			s.handleCreateTopic(corrID, payload, conn)
		case APIKeyListTopics:
			s.handleListTopics(corrID, payload, conn)
		case APIKeyCommitOffset:
			s.handleCommitOffset(corrID, payload, conn)
		case APIKeyFetchOffset:
			s.handleFetchOffset(corrID, payload, conn)
		default:
			WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		}
	}
}

func (s *Server) handleProduce(corrID uint32, payload []byte, conn net.Conn) {
	topicName, _, key, value, ok := DecodeProduceRequest(payload)
	if !ok {
		WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		return
	}

	partID, offset, err := s.topicMgr.Produce(topicName, key, value)
	if err != nil {
		WriteResponse(conn, corrID, ErrInternal, nil)
		return
	}

	resp := EncodeProduceResponse(int32(partID), offset)
	WriteResponse(conn, corrID, ErrNone, resp)
}

func (s *Server) handleFetch(corrID uint32, payload []byte, conn net.Conn) {
	topicName, pID, offset, maxCount, ok := DecodeFetchRequest(payload)
	if !ok {
		WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		return
	}

	msgs, err := s.topicMgr.Consume(topicName, int(pID), offset, int(maxCount))
	if err != nil {
		WriteResponse(conn, corrID, ErrUnknownTopic, nil)
		return
	}

	resp := EncodeFetchResponse(msgs)
	WriteResponse(conn, corrID, ErrNone, resp)
}

func (s *Server) handleCreateTopic(corrID uint32, payload []byte, conn net.Conn) {
	topicName, numPartitions, ok := DecodeCreateTopicRequest(payload)
	if !ok {
		WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		return
	}

	err := s.topicMgr.CreateTopic(topicName, int(numPartitions))
	if err != nil {
		WriteResponse(conn, corrID, ErrTopicExists, nil)
		return
	}

	WriteResponse(conn, corrID, ErrNone, nil)
}

func (s *Server) handleListTopics(corrID uint32, payload []byte, conn net.Conn) {
	topics := s.topicMgr.ListTopics()
	
	var totalSize int = 4
	for _, t := range topics {
		totalSize += 2 + len(t.Name) + 4 + 8
	}
	
	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(topics)))
	curr := 4
	
	for _, t := range topics {
		binary.BigEndian.PutUint16(buf[curr:curr+2], uint16(len(t.Name)))
		curr += 2
		copy(buf[curr:curr+len(t.Name)], t.Name)
		curr += len(t.Name)
		binary.BigEndian.PutUint32(buf[curr:curr+4], uint32(len(t.Partitions)))
		curr += 4
		
		var totalMsgs uint64
		for _, p := range t.Partitions {
			totalMsgs += p.MessageCount()
		}
		binary.BigEndian.PutUint64(buf[curr:curr+8], totalMsgs)
		curr += 8
	}
	
	WriteResponse(conn, corrID, ErrNone, buf)
}

func (s *Server) handleCommitOffset(corrID uint32, payload []byte, conn net.Conn) {
	group, topicName, pID, offset, ok := DecodeCommitOffsetRequest(payload)
	if !ok {
		WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		return
	}

	err := s.consumerMgr.CommitOffset(group, topicName, int(pID), offset)
	if err != nil {
		WriteResponse(conn, corrID, ErrInternal, nil)
		return
	}

	WriteResponse(conn, corrID, ErrNone, nil)
}

func (s *Server) handleFetchOffset(corrID uint32, payload []byte, conn net.Conn) {
	group, topicName, pID, ok := DecodeFetchOffsetRequest(payload)
	if !ok {
		WriteResponse(conn, corrID, ErrInvalidRequest, nil)
		return
	}

	offset, found := s.consumerMgr.GetOffset(group, topicName, int(pID))
	
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[0:8], offset)
	if found {
		buf[8] = 1
	} else {
		buf[8] = 0
	}

	WriteResponse(conn, corrID, ErrNone, buf)
}

func (s *Server) Stop() error {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}
