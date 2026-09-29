package delivery

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type TopicState struct {
	Name     string    `json:"name"`
	Messages []Message `json:"messages"`
}

type SubscriptionState struct {
	Name    string                  `json:"name"`
	NextSeq int64                   `json:"next_seq"`
	Items   map[int64]*MessageState `json:"items"`
}

type Data struct {
	Version       int                                      `json:"version"`
	Topics        map[string]*TopicState                   `json:"topics"`
	Subscriptions map[string]map[string]*SubscriptionState `json:"subscriptions"`
}

type SnapshotMessage struct {
	Message
	Status      string           `json:"status"`
	ConsumerID  string           `json:"consumer_id"`
	Attempt     int              `json:"attempt"`
	AckedAt     time.Time        `json:"acked_at"`
	LeasedUntil time.Time        `json:"leased_until"`
	Deliveries  []DeliveryRecord `json:"deliveries"`
}

type SnapshotSubscription struct {
	Name     string            `json:"name"`
	NextSeq  int64             `json:"next_seq"`
	Messages []SnapshotMessage `json:"messages"`
}

type SnapshotTopic struct {
	Name          string                 `json:"name"`
	Subscriptions []SnapshotSubscription `json:"subscriptions"`
}

type Snapshot struct {
	Topics []SnapshotTopic `json:"topics"`
}

type Store struct {
	mu   sync.Mutex
	path string
	data *Data
	now  func() time.Time
}

func openStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, "broker.json")
	s := &Store{path: p, now: time.Now}
	content, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		s.data = &Data{Version: 1, Topics: map[string]*TopicState{}, Subscriptions: map[string]map[string]*SubscriptionState{}}
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		s.data = &Data{Version: 1, Topics: map[string]*TopicState{}, Subscriptions: map[string]map[string]*SubscriptionState{}}
		return s, nil
	}
	if err := json.Unmarshal(content, &s.data); err != nil {
		return nil, fmt.Errorf("decode broker state: %w", err)
	}
	if s.data.Version != 1 {
		return nil, fmt.Errorf("unsupported broker state version %d", s.data.Version)
	}
	if s.data.Topics == nil {
		s.data.Topics = map[string]*TopicState{}
	}
	if s.data.Subscriptions == nil {
		s.data.Subscriptions = map[string]map[string]*SubscriptionState{}
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked()
}

func (s *Store) saveLocked() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked()
}

func (s *Store) writeLocked() error {
	content, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, content, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (s *Store) snapshot() Snapshot {
	result := Snapshot{}
	for topicName, topic := range s.data.Topics {
		topicSnapshot := SnapshotTopic{Name: topicName}
		for subName, sub := range s.data.Subscriptions[topicName] {
			ss := SnapshotSubscription{Name: subName, NextSeq: sub.NextSeq}
			for _, msg := range topic.Messages {
				state := sub.Items[msg.Seq]
				if state == nil {
					continue
				}
				item := SnapshotMessage{
					Message: msg, Status: state.Status, ConsumerID: state.ConsumerID,
					Attempt: state.Attempt, AckedAt: state.AckedAt, LeasedUntil: state.LeasedUntil,
					Deliveries: append([]DeliveryRecord(nil), state.Deliveries...),
				}
				ss.Messages = append(ss.Messages, item)
			}
			sort.Slice(ss.Messages, func(i, j int) bool { return ss.Messages[i].Seq < ss.Messages[j].Seq })
			topicSnapshot.Subscriptions = append(topicSnapshot.Subscriptions, ss)
		}
		sort.Slice(topicSnapshot.Subscriptions, func(i, j int) bool {
			return topicSnapshot.Subscriptions[i].Name < topicSnapshot.Subscriptions[j].Name
		})
		result.Topics = append(result.Topics, topicSnapshot)
	}
	sort.Slice(result.Topics, func(i, j int) bool { return result.Topics[i].Name < result.Topics[j].Name })
	return result
}
