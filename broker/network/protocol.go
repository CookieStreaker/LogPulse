package network

import (
	"encoding/binary"
	"io"
	"mini-kafka/storage"
)

const (
	APIKeyProduce      uint16 = 0
	APIKeyFetch        uint16 = 1
	APIKeyCreateTopic  uint16 = 2
	APIKeyListTopics   uint16 = 3
	APIKeyCommitOffset uint16 = 4
	APIKeyFetchOffset  uint16 = 5
)

const (
	ErrNone           int16 = 0
	ErrUnknownTopic   int16 = 1
	ErrInvalidRequest int16 = 2
	ErrInternal       int16 = 3
	ErrTopicExists    int16 = 4
)

func ReadRequest(r io.Reader) (uint16, uint32, []byte, error) {
	var totalLen uint32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return 0, 0, nil, err
	}

	buf := make([]byte, totalLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, 0, nil, err
	}

	apiKey := binary.BigEndian.Uint16(buf[0:2])
	correlationID := binary.BigEndian.Uint32(buf[2:6])
	payload := buf[6:]

	return apiKey, correlationID, payload, nil
}

func WriteResponse(w io.Writer, correlationID uint32, errCode int16, payload []byte) error {
	totalLen := uint32(4 + 2 + len(payload))
	
	header := make([]byte, 4+4+2)
	binary.BigEndian.PutUint32(header[0:4], totalLen)
	binary.BigEndian.PutUint32(header[4:8], correlationID)
	binary.BigEndian.PutUint16(header[8:10], uint16(errCode))

	if _, err := w.Write(header); err != nil {
		return err
	}
	
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

func DecodeProduceRequest(payload []byte) (topic string, partition int32, key, value []byte, ok bool) {
	if len(payload) < 2 { return }
	tLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < int(2+tLen+4+2) { return }
	topic = string(payload[2 : 2+tLen])
	curr := 2 + int(tLen)
	partition = int32(binary.BigEndian.Uint32(payload[curr : curr+4]))
	curr += 4
	kLen := binary.BigEndian.Uint16(payload[curr : curr+2])
	curr += 2
	if len(payload) < curr+int(kLen)+4 { return }
	key = payload[curr : curr+int(kLen)]
	curr += int(kLen)
	vLen := binary.BigEndian.Uint32(payload[curr : curr+4])
	curr += 4
	if len(payload) < curr+int(vLen) { return }
	value = payload[curr : curr+int(vLen)]
	ok = true
	return
}

func EncodeProduceResponse(partition int32, offset uint64) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], uint32(partition))
	binary.BigEndian.PutUint64(buf[4:12], offset)
	return buf
}

func DecodeFetchRequest(payload []byte) (topic string, partition int32, offset uint64, maxCount uint32, ok bool) {
	if len(payload) < 2 { return }
	tLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < int(2+tLen+4+8+4) { return }
	topic = string(payload[2 : 2+tLen])
	curr := 2 + int(tLen)
	partition = int32(binary.BigEndian.Uint32(payload[curr : curr+4]))
	curr += 4
	offset = binary.BigEndian.Uint64(payload[curr : curr+8])
	curr += 8
	maxCount = binary.BigEndian.Uint32(payload[curr : curr+4])
	ok = true
	return
}

func EncodeFetchResponse(msgs []*storage.Message) []byte {
	var totalSize int = 4
	for _, m := range msgs {
		totalSize += 8 + 8 + 2 + len(m.Key) + 4 + len(m.Value)
	}
	
	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msgs)))
	curr := 4
	
	for _, m := range msgs {
		binary.BigEndian.PutUint64(buf[curr:curr+8], m.Offset)
		curr += 8
		binary.BigEndian.PutUint64(buf[curr:curr+8], uint64(m.Timestamp))
		curr += 8
		binary.BigEndian.PutUint16(buf[curr:curr+2], uint16(len(m.Key)))
		curr += 2
		copy(buf[curr:curr+len(m.Key)], m.Key)
		curr += len(m.Key)
		binary.BigEndian.PutUint32(buf[curr:curr+4], uint32(len(m.Value)))
		curr += 4
		copy(buf[curr:curr+len(m.Value)], m.Value)
		curr += len(m.Value)
	}
	return buf
}

func DecodeCreateTopicRequest(payload []byte) (name string, numPartitions int32, ok bool) {
	if len(payload) < 2 { return }
	nLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < int(2+nLen+4) { return }
	name = string(payload[2 : 2+nLen])
	curr := 2 + int(nLen)
	numPartitions = int32(binary.BigEndian.Uint32(payload[curr : curr+4]))
	ok = true
	return
}

func DecodeCommitOffsetRequest(payload []byte) (group, topic string, partition int32, offset uint64, ok bool) {
	if len(payload) < 2 { return }
	gLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < int(2+gLen+2) { return }
	group = string(payload[2 : 2+gLen])
	curr := 2 + int(gLen)
	
	tLen := binary.BigEndian.Uint16(payload[curr : curr+2])
	curr += 2
	if len(payload) < curr+int(tLen)+4+8 { return }
	topic = string(payload[curr : curr+int(tLen)])
	curr += int(tLen)
	
	partition = int32(binary.BigEndian.Uint32(payload[curr : curr+4]))
	curr += 4
	offset = binary.BigEndian.Uint64(payload[curr : curr+8])
	ok = true
	return
}

func DecodeFetchOffsetRequest(payload []byte) (group, topic string, partition int32, ok bool) {
	if len(payload) < 2 { return }
	gLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < int(2+gLen+2) { return }
	group = string(payload[2 : 2+gLen])
	curr := 2 + int(gLen)
	
	tLen := binary.BigEndian.Uint16(payload[curr : curr+2])
	curr += 2
	if len(payload) < curr+int(tLen)+4 { return }
	topic = string(payload[curr : curr+int(tLen)])
	curr += int(tLen)
	
	partition = int32(binary.BigEndian.Uint32(payload[curr : curr+4]))
	ok = true
	return
}
