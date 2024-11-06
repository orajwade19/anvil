package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type event struct {
	origin            string
	msgId             string
	msgData           int
	vectorClockBefore map[string]int
}

type server struct {
	n                *maelstrom.Node
	initialized      atomic.Bool
	receivedTopology map[string]any
	vectorClock      map[string]int
	vectorClockLock  sync.RWMutex

	sentEvents     map[string]int
	sentEventsLock sync.RWMutex
	vClocks        map[string]map[string]int
	vClocksLock    sync.RWMutex

	events     []event
	eventsLock sync.RWMutex

	receivedMessages     map[string]int
	receivedMessagesLock sync.RWMutex
	appliedEvents        int
	newApplyEventTrigger chan bool
	newSendEventTrigger  chan bool
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msgFrom := msg.Src
	msgDest := msg.Dest

	if strings.Contains(msgFrom, "c") {
		log.Printf("[LOCK] Acquiring eventsLock for write in handleBroadcast")
		s.eventsLock.Lock()
		s.events = append(s.events, event{msgDest, msgFrom + strconv.Itoa(int(body["msg_id"].(float64))), int(body["message"].(float64)), copyMap(s.vectorClock)})
		log.Printf("[UNLOCK] Releasing eventsLock in handleBroadcast")
		s.eventsLock.Unlock()
		log.Println("New event added")
		s.newApplyEventTrigger <- true
		s.newSendEventTrigger <- true

		log.Printf("[LOCK] Acquiring vectorClockLock for write in handleBroadcast")
		s.vectorClockLock.Lock()
		s.vectorClock[msgDest] += 1
		log.Printf("[UNLOCK] Releasing vectorClockLock in handleBroadcast")
		s.vectorClockLock.Unlock()
	}

	body["type"] = "broadcast_ok"
	delete(body, "message")
	return s.n.Reply(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["messages"] = make([]any, 0)

	log.Printf("[LOCK] Acquiring receivedMessagesLock for read in handleRead")
	s.receivedMessagesLock.RLock()
	for _, aMsg := range s.receivedMessages {
		body["messages"] = append(body["messages"].([]any), aMsg)
	}
	log.Printf("[UNLOCK] Releasing receivedMessagesLock in handleRead")
	s.receivedMessagesLock.RUnlock()

	body["type"] = "read_ok"
	return s.n.Reply(msg, body)
}

func (s *server) handleSync(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	syncMsgId := body["sync_msg_id"].(string)
	log.Println("Received sync message with SyncMSGID ", syncMsgId)

	// Check both receivedMessages and events
	messageExists := false

	// First check receivedMessages with read lock
	log.Printf("[LOCK] Acquiring receivedMessagesLock for read in handleSync")
	s.receivedMessagesLock.RLock()
	_, exists := s.receivedMessages[syncMsgId]
	log.Printf("[UNLOCK] Releasing receivedMessagesLock in handleSync")
	s.receivedMessagesLock.RUnlock()

	// If not in receivedMessages, check events from appliedEvents forward
	if !exists {
		log.Printf("[LOCK] Acquiring eventsLock for read in handleSync")
		s.eventsLock.RLock()
		// Only check from appliedEvents index forward
		for i := s.appliedEvents; i < len(s.events); i++ {
			if s.events[i].msgId == syncMsgId {
				messageExists = true
				break
			}
		}
		log.Printf("[UNLOCK] Releasing eventsLock in handleSync")
		s.eventsLock.RUnlock()
	} else {
		messageExists = true
	}

	if !messageExists {
		log.Println("Processing new message")
		msgOrigin := body["origin"].(string)
		rawVectorClock := body["event_vector_clock"]
		vectorClockBytes, err := json.Marshal(rawVectorClock)
		if err != nil {
			log.Println("Error marshalling vector clock")
			return err
		}
		var receivedVectorClock map[string]int
		err = json.Unmarshal(vectorClockBytes, &receivedVectorClock)
		if err != nil {
			log.Println("Error unmarshalling vector clock")
			return err
		}
		log.Println("Before applying sync message with msgId", syncMsgId)

		log.Printf("[LOCK] Acquiring eventsLock for write in handleSync")
		s.eventsLock.Lock()
		s.events = append(s.events, event{msgOrigin, syncMsgId, int(body["message"].(float64)), receivedVectorClock})
		log.Printf("[UNLOCK] Releasing eventsLock in handleSync")
		s.eventsLock.Unlock()

		log.Printf("[LOCK] Acquiring vectorClockLock for write in handleSync")
		s.vectorClockLock.Lock()
		s.vectorClock[msgOrigin] += 1
		log.Printf("[UNLOCK] Releasing vectorClockLock in handleSync")
		s.vectorClockLock.Unlock()

		s.newApplyEventTrigger <- true
		s.newSendEventTrigger <- true
	}

	log.Printf("Sending sync_ok message with body -- 1: %v", body)
	body["type"] = "sync_ok"
	body["vector_clock"] = copyMap(s.vectorClock)
	delete(body, "message")
	delete(body, "event_vector_clock")
	delete(body, "sync_msg_id")
	delete(body, "origin")
	log.Printf("Sending sync_ok message with body: %v", body)
	return s.n.Reply(msg, body)
}

func (s *server) applyEvents() error {
	log.Printf("[LOCK] Acquiring eventsLock for read in applyEvents")
	s.eventsLock.RLock()
	defer func() {
		log.Printf("[UNLOCK] Releasing eventsLock in applyEvents")
		s.eventsLock.RUnlock()
	}()

	for i := s.appliedEvents; i < len(s.events); i++ {
		s.appliedEvents++
		msgId := s.events[i].msgId
		msgData := s.events[i].msgData

		log.Printf("[LOCK] Acquiring receivedMessagesLock for write in applyEvents")
		s.receivedMessagesLock.Lock()
		s.receivedMessages[msgId] = msgData
		log.Printf("[UNLOCK] Releasing receivedMessagesLock in applyEvents")
		s.receivedMessagesLock.Unlock()
	}
	return nil
}

func (s *server) handleVectorClockSync(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "vector_clock_sync_ok"
	body["vector_clock"] = copyMap(s.vectorClock)
	return s.n.Reply(msg, body)
}

func (s *server) getUpdatedVectorClock(n string, i int, wg *sync.WaitGroup) error {
	defer wg.Done()
	body := map[string]any{
		"type": "vector_clock_sync",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	reply, err := s.n.SyncRPC(ctx, n, body)
	cancel()
	if err != nil {
		return err
	}

	var replyBody map[string]any
	if err := json.Unmarshal(reply.Body, &replyBody); err != nil {
		return err
	}

	if rawVectorClock, ok := replyBody["vector_clock"].(map[string]any); ok {
		replyVectorClock := make(map[string]int)
		for k, v := range rawVectorClock {
			if intVal, ok := v.(float64); ok {
				replyVectorClock[k] = int(intVal)
			} else {
				return fmt.Errorf("unexpected type for vector clock value: %v", v)
			}
		}

		log.Printf("[LOCK] Acquiring vClocksLock for write in getUpdatedVectorClock")
		s.vClocksLock.Lock()
		s.vClocks[n] = replyVectorClock
		log.Printf("[UNLOCK] Releasing vClocksLock in getUpdatedVectorClock")
		s.vClocksLock.Unlock()
	} else {
		return fmt.Errorf("vector_clock field not found or invalid")
	}
	return nil
}

func (s *server) sendEvent(eventsCopy []event, n string, i int, wg *sync.WaitGroup) error {
	msgId := eventsCopy[i].msgId
	msgData := eventsCopy[i].msgData
	msgOrigin := eventsCopy[i].origin
	eventVectorClockBefore := eventsCopy[i].vectorClockBefore
	msgDest := n

	body := map[string]any{
		"type":               "sync",
		"message":            msgData,
		"sync_msg_id":        msgId,
		"origin":             msgOrigin,
		"event_vector_clock": eventVectorClockBefore,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	reply, err := s.n.SyncRPC(ctx, msgDest, body)
	cancel()
	if err != nil {
		wg.Done()
		return err
	} else {
		var body map[string]any
		if err := json.Unmarshal(reply.Body, &body); err != nil {
			wg.Done()
			return err
		}
		if body["type"] == "sync_ok" {
			s.sentEventsLock.Lock()
			s.sentEvents[msgDest] = i + 1
			s.sentEventsLock.Unlock()
			if rawVectorClock, ok := body["vector_clock"].(map[string]any); ok {
				replyVectorClock := make(map[string]int)
				for k, v := range rawVectorClock {
					if intVal, ok := v.(float64); ok {
						replyVectorClock[k] = int(intVal)
					} else {
						return fmt.Errorf("unexpected type for vector clock value: %v", v)
					}
				}

				log.Printf("[LOCK] Acquiring vClocksLock for write in sendEvent")
				s.vClocksLock.Lock()
				s.vClocks[msgDest] = replyVectorClock
				log.Printf("[UNLOCK] Releasing vClocksLock in sendEvent")
				s.vClocksLock.Unlock()
			} else {
				return fmt.Errorf("vector_clock field not found or invalid")
			}
		}
	}
	wg.Done()
	return nil
}

func (s *server) getUpdatedVectorClocks() error {
	log.Printf("[LOCK] Acquiring eventsLock for read in getUpdatedVectorClocks")
	s.eventsLock.RLock()
	eventsLength := len(s.events)
	log.Printf("[UNLOCK] Releasing eventsLock in getUpdatedVectorClocks")
	s.eventsLock.RUnlock()

	wg := sync.WaitGroup{}
	for _, n := range s.n.NodeIDs() {
		if n == s.n.ID() {
			continue
		} else {
			s.sentEventsLock.RLock()
			i := s.sentEvents[n]
			s.sentEventsLock.RUnlock()
			if eventsLength > i {
				wg.Add(1)
				go s.getUpdatedVectorClock(n, i, &wg)
			}
		}
	}
	wg.Wait()
	return nil
}

func (s *server) sendEvents() error {
	defer log.Println("Finished sendEvents")
	log.Println("Starting sendEvents")
	log.Printf("[LOCK] Acquiring eventsLock for read in sendEvents")
	s.eventsLock.RLock()
	eventsCopy := deepCopyEvents(s.events)
	log.Printf("[UNLOCK] Releasing eventsLock in sendEvents")
	s.eventsLock.RUnlock()

	wg := sync.WaitGroup{}
	for _, n := range s.n.NodeIDs() {
		if n == s.n.ID() {
			continue
		} else {
			s.sentEventsLock.RLock()
			i := s.sentEvents[n]
			s.sentEventsLock.RUnlock()
			if len(eventsCopy) > i {
				log.Println("For node and index: ", n, i)
				nodeHasPreviousEvents := compareVClock(eventsCopy[i].vectorClockBefore, s.vClocks[n])
				if nodeHasPreviousEvents {
					wg.Add(1)
					go s.sendEvent(eventsCopy, n, i, &wg)
				} else {
					log.Println("--getUpdatedVectorClock--")
					wg.Add(1)
					go s.getUpdatedVectorClock(n, i, &wg)
				}
			}
		}
	}
	wg.Wait()
	return nil
}

func compareVClock(eventVectorClockBefore map[string]int, nodeVectorClock map[string]int) bool {
	log.Println("EVENT VECTOR CLOCK BEFORE: ", eventVectorClockBefore)
	log.Println("NODE VECTOR CLOCK: ", nodeVectorClock)
	for node, count := range eventVectorClockBefore {
		if nodeCount, exists := nodeVectorClock[node]; !exists || nodeCount < count {
			return false
		}
	}
	return true
}

func (s *server) handleInit(msg maelstrom.Message) error {
	for _, aNode := range s.n.NodeIDs() {
		s.vectorClock[aNode] = 0
		s.sentEvents[aNode] = 0
		s.vClocks[aNode] = make(map[string]int)

		for _, bNode := range s.n.NodeIDs() {
			s.vClocks[aNode][bNode] = 0
		}
	}
	s.initialized.Store(true)
	return nil
}

func (s *server) periodicApplyEvents() {
	for {
		if s.initialized.Load() {
			<-s.newApplyEventTrigger
			s.applyEvents()
		}
	}
}

func (s *server) periodicgetUpdatedVectorClock() {
	for {
		if s.initialized.Load() {
			<-time.After(time.Duration(1000) * time.Millisecond)
			s.getUpdatedVectorClocks()
		}
	}
}

func (s *server) periodicSendEvents() {
	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()

	for {
		if s.initialized.Load() {
			select {
			case <-timer.C:
				log.Println("Sending events due to timer")
				s.sendEvents()
			case <-s.newSendEventTrigger:
				s.sendEvents()
			}
		}
	}
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.receivedTopology = body["topology"].(map[string]any)
	body["type"] = "topology_ok"
	delete(body, "topology")
	return s.n.Reply(msg, body)
}

func main() {
	s := server(
		server{
			maelstrom.NewNode(),
			atomic.Bool{},
			make(map[string]any),
			make(map[string]int),
			sync.RWMutex{},
			make(map[string]int),
			sync.RWMutex{},
			make(map[string]map[string]int),
			sync.RWMutex{},
			make([]event, 0),
			sync.RWMutex{},
			make(map[string]int),
			sync.RWMutex{},
			0,
			make(chan bool, 10),
			make(chan bool, 10),
		})

	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("init", s.handleInit)
	s.n.Handle("sync", s.handleSync)
	s.n.Handle("vector_clock_sync", s.handleVectorClockSync)

	go s.periodicApplyEvents()
	go s.periodicSendEvents()
	go s.periodicgetUpdatedVectorClock()

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
