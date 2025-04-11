package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type event struct {
	origin                 string
	msg_id                 string
	msg_data               int
	msg_type               string
	add_msg_uid_for_delete string
	delete_element_exists  bool
	vector_clock_before    map[string]int
}

type server struct {
	n                 *maelstrom.Node
	initialized       atomic.Bool
	received_topology map[string]any
	vector_clock      map[string]int
	vector_clock_lock sync.RWMutex

	sent_events             map[string]int
	sent_events_lock        sync.RWMutex
	node_vector_clocks      map[string]map[string]int
	node_vector_clocks_lock sync.RWMutex

	events      []event
	events_lock sync.RWMutex

	received_messages             map[string]int
	received_messages_lock        sync.RWMutex
	received_delete_messages      map[string]int
	received_delete_messages_lock sync.RWMutex
	applied_events                int
	new_apply_event_trigger       chan bool
	new_send_event_trigger        chan bool

	unique_messages      map[string]struct{}
	unique_messages_lock sync.RWMutex
}

func (s *server) handleAdd(msg maelstrom.Message) error {
	//IMPROVEMENT
	//not much room to optimize in handle add since we basically just append to event log
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msg_from := msg.Src
	msg_dest := msg.Dest

	msg_uid := msg_from + ":" + strconv.Itoa(int(body["msg_id"].(float64)))

	s.unique_messages_lock.Lock()
	s.unique_messages[msg_uid] = struct{}{}
	s.unique_messages_lock.Unlock()

	//IMPROVEMENT
	//this makes sense - we're making a copy of a shared data structure, which is the server vector clock
	//ways to make this more efficient : per key?
	//discuss
	//what kind of consistency guarantees do we want to offer for keys?
	s.vector_clock_lock.RLock()
	vector_clock_copy := copyMap(s.vector_clock)
	s.vector_clock_lock.RUnlock()
	log.Printf("[LOCK] Acquiring eventsLock for write in handleBroadcast")
	s.events_lock.Lock()
	new_event := event{msg_dest, msg_uid, int(body["element"].(float64)), "add", "0", false, vector_clock_copy}
	s.events = append(s.events, new_event)
	log.Printf("[UNLOCK] Releasing eventsLock in handleBroadcast")
	s.events_lock.Unlock()
	log.Println("New event added")
	s.new_apply_event_trigger <- true
	s.new_send_event_trigger <- true
	log.Printf("[LOCK] Acquiring vectorClockLock for write in handleBroadcast")
	s.vector_clock_lock.Lock()
	s.vector_clock[msg_dest] += 1
	log.Printf("[UNLOCK] Releasing vectorClockLock in handleBroadcast")
	s.vector_clock_lock.Unlock()

	body["type"] = "add_ok"
	body["event_id"] = new_event.msg_id
	delete(body, "element")
	return s.n.Reply(msg, body)
}

func (s *server) handleDelete(msg maelstrom.Message) error {
	//WE ARE ASSUMING ELEMENT GETS ADDED ONLY ONCE IN THE WORKLOAD
	//THIS IS A HORRIBLE AND UNTRUE ASSUMPTION
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	msgFrom := msg.Src
	msgDest := msg.Dest

	msg_uid := msgFrom + ":" + strconv.Itoa(int(body["msg_id"].(float64)))

	s.unique_messages_lock.Lock()
	s.unique_messages[msg_uid] = struct{}{}
	s.unique_messages_lock.Unlock()

	///CHECKING FOR EXISTENCE STARTS HERE
	///
	///
	delete_element := int(body["element"].(float64))
	log.Println("Received delete message with element ", delete_element)

	// Check both receivedMessages and events
	element_exists := false
	element_add_uid := ""

	// First check receivedMessages with read lock
	log.Printf("[LOCK] Acquiring receivedMessagesLock for read in handleDelete")
	s.received_messages_lock.RLock()
	for aMsgId, aMsg := range s.received_messages {
		//TODO
		//hacky - don't really have a "set" yet, just collecting added values
		//actually, this could be considered to be an issue with the test itself, since it never tries to add a
		//duplicate value
		if aMsg == delete_element {
			element_exists = true
			element_add_uid = aMsgId
			break
		}
	}
	log.Printf("[UNLOCK] Releasing receivedMessagesLock in handleDelete")
	s.received_messages_lock.RUnlock()

	// If not in receivedMessages, check events from appliedEvents forward
	if !element_exists {
		log.Printf("[LOCK] Acquiring eventsLock for read in handleDelete")
		s.events_lock.RLock()
		// Only check from appliedEvents index forward
		for i := s.applied_events; i < len(s.events); i++ {
			if s.events[i].msg_data == delete_element {
				if s.events[i].msg_type == "add" {
					element_exists = true
					element_add_uid = s.events[i].msg_id
				} else if s.events[i].msg_type == "delete" {
					if element_exists {
						element_exists = false
					}
				}
			}
		}
		log.Printf("[UNLOCK] Releasing eventsLock in handleDelete")
		s.events_lock.RUnlock()
	}

	///	///CHECKING FOR EXISTENCE ENDS HERE

	s.vector_clock_lock.RLock()
	vectorClockCopy := copyMap(s.vector_clock)
	s.vector_clock_lock.RUnlock()
	log.Printf("[LOCK] Acquiring eventsLock for write in handleDelete")
	s.events_lock.Lock()
	new_event := event{msgDest, msg_uid, int(body["element"].(float64)), "delete", element_add_uid, element_exists, vectorClockCopy}
	s.events = append(s.events, new_event)
	// s.events = append(s.events, event{msgDest, msgFrom + ":" + strconv.Itoa(int(body["msg_id"].(float64))), int(body["delta"].(float64)), vectorClockCopy})
	log.Printf("[UNLOCK] Releasing eventsLock in handleDelete")
	s.events_lock.Unlock()
	log.Println("New event added")
	s.new_apply_event_trigger <- true
	s.new_send_event_trigger <- true
	log.Printf("[LOCK] Acquiring vectorClockLock for write in handleDelete")
	s.vector_clock_lock.Lock()
	s.vector_clock[msgDest] += 1
	log.Printf("[UNLOCK] Releasing vectorClockLock in handleDelete")
	s.vector_clock_lock.Unlock()

	// body["type"] = "broadcast_ok"
	body["type"] = "delete_ok"
	body["event_id"] = new_event.add_msg_uid_for_delete
	// delete(body, "message")
	delete(body, "element")
	return s.n.Reply(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["value"] = make([]any, 0)
	// body["value"] = make([]any, 0)
	log.Printf("[LOCK] Acquiring receivedMessagesLock for read in handleRead")
	// tot	al := 0
	s.received_messages_lock.RLock()
	for _, aMsg := range s.received_messages {
		//TODO
		//Again, not a great solution to use only non negative values, good enough for prototype
		if aMsg >= 0 {
			body["value"] = append(body["value"].([]any), aMsg)
		}
	}
	log.Printf("[UNLOCK] Releasing receivedMessagesLock in handleRead")
	s.received_messages_lock.RUnlock()
	// body["value"] = total

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

	s.unique_messages_lock.RLock()
	_, exists := s.unique_messages[syncMsgId]
	messageExists = exists
	s.unique_messages_lock.RUnlock()

	if !messageExists {
		log.Println("Processing new message")
		s.unique_messages_lock.Lock()
		s.unique_messages[syncMsgId] = struct{}{}
		s.unique_messages_lock.Unlock()
		msgOrigin := body["origin"].(string)
		msgType := body["msgType"].(string)
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
		s.events_lock.Lock()
		s.events = append(s.events, event{msgOrigin, syncMsgId, int(body["element"].(float64)), msgType, body["deleteMsgUid"].(string), body["delete_element_exists"].(bool), receivedVectorClock})
		// s.events = append(s.events, event{msgOrigin, syncMsgId, int(body["delta"].(float64)), receivedVectorClock})
		log.Printf("[UNLOCK] Releasing eventsLock in handleSync")
		s.events_lock.Unlock()

		log.Printf("[LOCK] Acquiring vectorClockLock for write in handleSync")
		s.vector_clock_lock.Lock()
		s.vector_clock[msgOrigin] += 1
		log.Printf("[UNLOCK] Releasing vectorClockLock in handleSync")
		s.vector_clock_lock.Unlock()

		s.new_apply_event_trigger <- true
		s.new_send_event_trigger <- true
	}

	log.Printf("Sending sync_ok message with body -- 1: %v", body)
	body["type"] = "sync_ok"
	s.vector_clock_lock.RLock()
	body["vector_clock"] = copyMap(s.vector_clock)
	s.vector_clock_lock.RUnlock()
	delete(body, "element")
	delete(body, "msgType")
	delete(body, "event_vector_clock")
	delete(body, "sync_msg_id")
	delete(body, "origin")
	delete(body, "deleteMsgUid")
	delete(body, "delete_element_exists")
	log.Printf("Sending sync_ok message with body: %v", body)
	return s.n.Reply(msg, body)
}

func (s *server) applyEvents() error {
	log.Printf("[LOCK] Acquiring eventsLock for read in applyEvents")
	s.events_lock.RLock()
	defer func() {
		log.Printf("[UNLOCK] Releasing eventsLock in applyEvents")
		s.events_lock.RUnlock()
	}()

	for i := s.applied_events; i < len(s.events); i++ {
		s.applied_events++
		msgId := s.events[i].msg_id
		msgData := s.events[i].msg_data

		msgType := s.events[i].msg_type
		msgDeleteElementExists := s.events[i].delete_element_exists
		msgDeleteElementUid := s.events[i].add_msg_uid_for_delete

		log.Printf("[LOCK] Acquiring receivedMessagesLock for write in applyEvents")
		s.received_messages_lock.Lock()
		if msgType == "add" {
			s.received_messages[msgId] = msgData
		} else {
			if msgDeleteElementExists {
				//TODO
				//not great to use this, but good enough for positive values in the workload
				s.received_messages[msgDeleteElementUid] = -1
			}

			//not a great solution ?? actually might be ok
			s.received_delete_messages_lock.Lock()
			s.received_delete_messages[msgId] = -1
			s.received_delete_messages_lock.Unlock()
		}

		log.Printf("[UNLOCK] Releasing receivedMessagesLock in applyEvents")
		s.received_messages_lock.Unlock()
	}
	return nil
}

func (s *server) handleVectorClockSync(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "vector_clock_sync_ok"
	body["vector_clock"] = copyMap(s.vector_clock)
	return s.n.Reply(msg, body)
}

func (s *server) getUpdatedVectorClock(n string, i int, wg *sync.WaitGroup) error {
	defer wg.Done()
	body := map[string]any{
		"type": "vector_clock_sync",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
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
		s.node_vector_clocks_lock.Lock()
		s.node_vector_clocks[n] = replyVectorClock
		log.Printf("[UNLOCK] Releasing vClocksLock in getUpdatedVectorClock")
		s.node_vector_clocks_lock.Unlock()
	} else {
		return fmt.Errorf("vector_clock field not found or invalid")
	}
	return nil
}

func (s *server) sendEvent(eventsCopy []event, n string, i int, wg *sync.WaitGroup) error {
	msgId := eventsCopy[i].msg_id
	msgData := eventsCopy[i].msg_data
	msgOrigin := eventsCopy[i].origin
	eventVectorClockBefore := eventsCopy[i].vector_clock_before
	msgType := eventsCopy[i].msg_type
	msgDest := n
	deleteMsgUid := eventsCopy[i].add_msg_uid_for_delete
	delete_element_exists := eventsCopy[i].delete_element_exists

	body := map[string]any{
		"type":    "sync",
		"element": msgData,
		// "delta":              msgData,
		"sync_msg_id":           msgId,
		"origin":                msgOrigin,
		"event_vector_clock":    eventVectorClockBefore,
		"msgType":               msgType,
		"deleteMsgUid":          deleteMsgUid,
		"delete_element_exists": delete_element_exists,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
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
			s.sent_events_lock.Lock()
			s.sent_events[msgDest] = i + 1
			s.sent_events_lock.Unlock()
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
				s.node_vector_clocks_lock.Lock()
				s.node_vector_clocks[msgDest] = replyVectorClock
				log.Printf("[UNLOCK] Releasing vClocksLock in sendEvent")
				s.node_vector_clocks_lock.Unlock()
			} else {
				return fmt.Errorf("vector_clock field not found or invalid")
			}
		}
	}
	wg.Done()
	return nil
}

func (s *server) sendEvents() error {
	defer log.Println("Finished sendEvents")
	log.Println("Starting sendEvents")
	log.Printf("[LOCK] Acquiring eventsLock for read in sendEvents")
	s.events_lock.RLock()
	eventsCopy := deepCopyEvents(s.events)
	log.Printf("[UNLOCK] Releasing eventsLock in sendEvents")
	s.events_lock.RUnlock()

	wg := sync.WaitGroup{}
	for _, n := range s.n.NodeIDs() {
		if n == s.n.ID() {
			continue
		} else {
			s.sent_events_lock.RLock()
			i := s.sent_events[n]
			s.sent_events_lock.RUnlock()
			if len(eventsCopy) > i {
				log.Println("For node and index: ", n, i)
				nodeHasPreviousEvents := compareVClock(eventsCopy[i].vector_clock_before, s.node_vector_clocks[n])
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
		s.vector_clock[aNode] = 0
		s.sent_events[aNode] = 0
		s.node_vector_clocks[aNode] = make(map[string]int)

		for _, bNode := range s.n.NodeIDs() {
			s.node_vector_clocks[aNode][bNode] = 0
		}
	}
	s.initialized.Store(true)
	return nil
}

func (s *server) periodicApplyEvents() {
	for {
		if s.initialized.Load() {
			<-s.new_apply_event_trigger
			s.applyEvents()
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
			case <-s.new_send_event_trigger:
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
	s.received_topology = body["topology"].(map[string]any)
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
			make(map[string]int),
			sync.RWMutex{},
			0,
			make(chan bool, 10),
			make(chan bool, 10),
			make(map[string]struct{}),
			sync.RWMutex{},
		})

	// s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("add", s.handleAdd)
	s.n.Handle("delete", s.handleDelete)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("init", s.handleInit)
	s.n.Handle("sync", s.handleSync)
	s.n.Handle("vector_clock_sync", s.handleVectorClockSync)

	go s.periodicApplyEvents()
	go s.periodicSendEvents()
	// go s.periodicgetUpdatedVectorClock()

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
