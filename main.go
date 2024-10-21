package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
	events           []event
	//mutex is the simplest possible solution for now. Iterate!!
	eventsLock sync.RWMutex

	//underlying data structure to store all received messages
	receivedMessages map[string]int
	appliedEvents    int
	newEvent         chan bool
}

func (s *server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	//store event in slice of events
	msgFrom := msg.Src
	msgDest := msg.Dest
	log.Println("EVENT LIST: ", s.events)

	//increment vector clock
	if strings.Contains(msgFrom, "c") {
		s.vectorClock[msgDest] += 1
		s.eventsLock.Lock()
		s.events = append(s.events, event{msgDest, msgFrom + body["msg_id"].(string), 0, s.vectorClock})
		s.eventsLock.Unlock()
	} else {
		// do nothing for now, and for the echo case
	}

	//actual echo reply with echo_ok
	body["type"] = "echo_ok"
	return s.n.Reply(msg, body)
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
		s.vectorClock[msgDest] += 1
		s.eventsLock.Lock()
		s.events = append(s.events, event{msgDest, msgFrom + strconv.Itoa(int(body["msg_id"].(float64))), int(body["message"].(float64)), s.vectorClock})
		s.eventsLock.Unlock()
	} else {
		// do nothing for now, and for the echo case
	}

	//actual echo reply with echo_ok
	body["type"] = "broadcast_ok"
	delete(body, "message")
	s.newEvent <- true
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

func (s *server) handleInit(msg maelstrom.Message) error {
	for _, aNode := range s.n.NodeIDs() {
		s.vectorClock[aNode] = 0
	}
	return nil
}

func (s *server) periodicApplyEvents() {
	for {
		// <-time.After(time.Duration(500) * time.Millisecond)
		<-s.newEvent
		s.applyEvents()
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
			make([]event, 0),
			sync.RWMutex{},
			make(map[string]int),
			0,
			make(chan bool),
		})

	s.n.Handle("echo", s.handleEcho)
	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	// Execute the node's message loop. This will run until STDIN is closed.
	// n.Handle("topology", func(msg maelstrom.Message) error {
	// 	// Unmarshal the message body as an loosely-typed map.
	// 	var body map[string]any
	// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
	// 		return err
	// 	}

	// 	receivedTopology = body["topology"].(map[string]any)
	// 	println(receivedTopology)
	// 	// Update the message type.
	// 	delete(body, "topology")
	// 	body["type"] = "topology_ok"
	// 	// Echo the original message back with the updated message type.
	// 	return n.Reply(msg, body)
	// })

	s.n.Handle("init", s.handleInit)

	go s.periodicApplyEvents()

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

}
