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
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

//Listing general assumptions here
//the MSGIDs are unique

type event struct {
	origin            string
	msgId             string //this is the "data id"
	msgData           int    //this is the "data", shouldnt be int but for now it is
	vectorClockBefore map[string]int
}

type server struct {
	n                *maelstrom.Node
	receivedTopology map[string]any
	vectorClock      map[string]int

	//node sync state
	sentEvents map[string]int
	vClocks    map[string]map[string]int

	events []event
	//mutex is the simplest possible solution for now. Iterate!!
	eventsLock sync.RWMutex

	//underlying data structure to store all received messages
	receivedMessages     map[string]int
	appliedEvents        int
	newApplyEventTrigger chan bool
	newSendEventTrigger  chan bool
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	//store event in slice of events
	msgFrom := msg.Src
	msgDest := msg.Dest

	//increment vector clock
	if strings.Contains(msgFrom, "c") {
		s.eventsLock.Lock()
		s.events = append(s.events, event{msgDest, msgFrom + strconv.Itoa(int(body["msg_id"].(float64))), int(body["message"].(float64)), copyMap(s.vectorClock)})
		s.eventsLock.Unlock()
		log.Println("New event added")
		s.newApplyEventTrigger <- true
		s.newSendEventTrigger <- true
		s.vectorClock[msgDest] += 1
	} else {
		// do nothing for now, and for the echo case
	}

	//actual echo reply with echo_ok
	body["type"] = "broadcast_ok"
	delete(body, "message")
	return s.n.Reply(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	//collect all messages
	body["messages"] = make([]any, 0)
	for _, aMsg := range s.receivedMessages {
		body["messages"] = append(body["messages"].([]any), aMsg)
	}
	body["type"] = "read_ok"
	return s.n.Reply(msg, body)
}

func (s *server) handleSync(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	syncMsgId := body["sync_msg_id"].(string)
	_, exists := s.receivedMessages[syncMsgId]
	if !exists {
		msgOrigin := body["origin"].(string)
		rawVectorClock := body["event_vector_clock"]
		vectorClockBytes, err := json.Marshal(rawVectorClock)
		if err != nil {
			return err
		}
		var receivedVectorClock map[string]int
		err = json.Unmarshal(vectorClockBytes, &receivedVectorClock)
		if err != nil {
			return err
		}

		s.eventsLock.Lock()
		s.events = append(s.events, event{msgOrigin, body["sync_msg_id"].(string), int(body["message"].(float64)), receivedVectorClock})
		s.eventsLock.Unlock()

		s.vectorClock[msgOrigin] += 1
		s.newApplyEventTrigger <- true
		s.newSendEventTrigger <- true

	}
	body["type"] = "sync_ok"
	body["vector_clock"] = s.vectorClock
	delete(body, "message")
	delete(body, "event_vector_clock")
	delete(body, "sync_msg_id")
	delete(body, "origin")
	return s.n.Reply(msg, body)
}

func (s *server) applyEvents() error {
	s.eventsLock.RLock()
	defer s.eventsLock.RUnlock()
	for i := s.appliedEvents; i < len(s.events); i++ {
		s.appliedEvents++
		msgId := s.events[i].msgId
		msgData := s.events[i].msgData
		s.receivedMessages[msgId] = msgData
	}
	log.Println("APPLIED EVENTS : ", s.receivedMessages)
	return nil
}

func (s *server) sendEvent(n string, i int, wg *sync.WaitGroup) error {
	msgId := s.events[i].msgId
	msgData := s.events[i].msgData
	msgOrigin := s.events[i].origin
	eventVectorClockBefore := s.events[i].vectorClockBefore
	msgDest := n

	body := map[string]any{
		"type":               "sync",
		"message":            msgData,
		"sync_msg_id":        msgId,
		"origin":             msgOrigin,
		"event_vector_clock": eventVectorClockBefore,
	}

	wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	reply, err := s.n.SyncRPC(ctx, msgDest, body)
	cancel()
	if err != nil {
		return err
	} else {
		var body map[string]any
		if err := json.Unmarshal(reply.Body, &body); err != nil {
			return err
		}
		if body["type"] == "sync_ok" {
			s.sentEvents[msgDest] = i + 1
			// Safely extract the vector_clock field as a map[string]any
			if rawVectorClock, ok := body["vector_clock"].(map[string]any); ok {
				// Initialize a new map to hold the vector clock as map[string]int
				replyVectorClock := make(map[string]int)

				// Iterate over rawVectorClock and convert values to int
				for k, v := range rawVectorClock {
					if intVal, ok := v.(float64); ok {
						replyVectorClock[k] = int(intVal)
					} else {
						return fmt.Errorf("unexpected type for vector clock value: %v", v)
					}
				}
				// Assign the converted vectorClock map
				s.vClocks[msgDest] = replyVectorClock
			} else {
				return fmt.Errorf("vector_clock field not found or invalid")
			}
		}
	}
	wg.Done()
	return nil
}

func (s *server) sendEvents() error {
	s.eventsLock.RLock()
	defer s.eventsLock.RUnlock()
	wg := sync.WaitGroup{}
	for _, n := range s.n.NodeIDs() {
		if n == s.n.ID() {
			continue
		} else {
			i := s.sentEvents[n]
			// the node's vector clock should AT LEAST be equal to or greater than the event's preReq vector clock
			if len(s.events) > i {
				//this is most likely buggy for now, because it relies on stored vector clock. Will not work for more than 2 nodes
				nodeHasPreviousEvents := compareVClock(s.events[i].vectorClockBefore, s.vClocks[n])
				if nodeHasPreviousEvents {
					go s.sendEvent(n, i, &wg)
				}
			}
		}
	}
	wg.Wait()
	return nil
}

// not the best way to do it, because order of passing in the values matters :(
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
	return nil
}

func (s *server) periodicApplyEvents() {
	for {
		// <-time.After(time.Duration(500) * time.Millisecond)
		<-s.newApplyEventTrigger
		s.applyEvents()
	}
}

func (s *server) periodicSendEvents() {
	for {
		<-s.newSendEventTrigger
		log.Println("Calling periodicSendEvents")
		s.sendEvents()
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
			make(map[string]any),
			make(map[string]int),
			make(map[string]int),
			make(map[string]map[string]int),
			make([]event, 0),
			sync.RWMutex{},
			make(map[string]int),
			0,
			make(chan bool),
			make(chan bool),
		})

	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("init", s.handleInit)
	s.n.Handle("sync", s.handleSync)

	go s.periodicApplyEvents()
	go s.periodicSendEvents()

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

}
