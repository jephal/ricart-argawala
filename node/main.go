package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	pb "ricart-argawala"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedRicartAgrawalaServer
	id           int32
	port         string
	lamportClock int64
	clockMutex   sync.Mutex

	// Mutual exclusion state
	requesting    bool
	requestTime   int64
	inCS          bool
	deferredQueue []int32
	replies       map[int32]bool

	//nodes
	peers   map[int32]string // node_id -> address
	clients map[int32]pb.RicartAgrawalaClient

	mutex sync.Mutex
}

func NewNode(id int32, port string) *Node {
	return &Node{
		id:            id,
		port:          port,
		lamportClock:  0,
		requesting:    false,
		inCS:          false,
		deferredQueue: make([]int32, 0),
		replies:       make(map[int32]bool),
		peers:         make(map[int32]string),
		clients:       make(map[int32]pb.RicartAgrawalaClient),
	}
}

func (n *Node) updateLamportClock(receivedTime int64) {
	n.clockMutex.Lock()
	defer n.clockMutex.Unlock()

	if receivedTime > n.lamportClock {
		n.lamportClock = receivedTime
	}
	n.lamportClock++
}

func (n *Node) getLamportClock() int64 {
	n.clockMutex.Lock()
	defer n.clockMutex.Unlock()
	n.lamportClock++
	return n.lamportClock
}

// gRPC service implementations
func (n *Node) RequestAccess(ctx context.Context, msg *pb.Message) (*pb.Ack, error) {
	n.updateLamportClock(msg.Timestamp)

	log.Printf("Node %d received REQUEST from Node %d with timestamp %d", n.id, msg.ProcessId, msg.Timestamp)

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// Reply immediately if not requesting or in CS
	shouldReply := !n.requesting && !n.inCS

	// If we are requesting, only reply if the incoming request has higher priority
	if n.requesting {
		// Higher priority means: lower timestamp, or same timestamp but lower node ID
		if msg.Timestamp < n.requestTime ||
			(msg.Timestamp == n.requestTime && msg.ProcessId < n.id) {
			shouldReply = true
		} else {
			shouldReply = false
		}
	}

	if shouldReply {
		// Send reply
		go n.sendReply(msg.ProcessId)
		log.Printf("Node %d sent REPLY to Node %d", n.id, msg.ProcessId)
	} else {
		// Defer the request
		n.deferredQueue = append(n.deferredQueue, msg.ProcessId)
		log.Printf("Node %d deferred request from Node %d", n.id, msg.ProcessId)
	}

	return &pb.Ack{Success: true}, nil
}

func (n *Node) ReplyAccess(ctx context.Context, msg *pb.Message) (*pb.Ack, error) {
	n.updateLamportClock(msg.Timestamp)

	log.Printf("Node %d received REPLY from Node %d", n.id, msg.ProcessId)

	n.mutex.Lock()
	n.replies[msg.ProcessId] = true
	n.mutex.Unlock()

	return &pb.Ack{Success: true}, nil
}

func (n *Node) sendReply(toNode int32) {
	client := n.clients[toNode]
	if client == nil {
		return
	}

	timestamp := n.getLamportClock()
	msg := &pb.Message{
		Type:      1, // REPLY
		ProcessId: n.id,
		Timestamp: timestamp,
	}

	client.ReplyAccess(context.Background(), msg)
}

func (n *Node) requestCriticalSection() {
	n.mutex.Lock()
	n.requesting = true
	n.replies = make(map[int32]bool)
	timestamp := n.getLamportClock()
	n.requestTime = timestamp
	n.mutex.Unlock()

	log.Printf("Node %d requesting Critical Section with timestamp %d", n.id, timestamp)

	// Send request to all other nodes
	requestsSent := 0
	for peerID := range n.peers {
		if peerID != n.id {
			client := n.clients[peerID]
			if client != nil {
				go func(pid int32) {
					msg := &pb.Message{
						Type:      0, // REQUEST
						ProcessId: n.id,
						Timestamp: timestamp,
					}
					_, err := client.RequestAccess(context.Background(), msg)
					if err != nil {
						log.Printf("Node %d failed to send REQUEST to Node %d: %v", n.id, pid, err)
					} else {
						log.Printf("Node %d sent REQUEST to Node %d with timestamp %d", n.id, pid, timestamp)
					}
				}(peerID)
				requestsSent++
			} else {
				log.Printf("Node %d has no client connection to Node %d", n.id, peerID)
			}
		}
	}

	if requestsSent == 0 {
		log.Printf("Node %d sent no requests - no connected peers", n.id)
		n.mutex.Lock()
		n.requesting = false
		n.mutex.Unlock()
		return
	}

	log.Printf("Node %d waiting for replies from %d nodes", n.id, requestsSent)

	for {
		n.mutex.Lock()
		allReplied := len(n.replies) >= requestsSent
		replyCount := len(n.replies)
		n.mutex.Unlock()

		if allReplied {
			log.Printf("Node %d received all %d replies, entering critical section", n.id, replyCount)
			n.enterCriticalSection()
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) enterCriticalSection() {
	n.mutex.Lock()
	n.inCS = true
	n.requesting = false
	n.mutex.Unlock()

	log.Printf("Node %d ENTERED Critical Section", n.id)

	// Simulate critical section work
	time.Sleep(2 * time.Second)
	fmt.Printf("Node %d is in CRITICAL SECTION - doing important work!\n", n.id)

	n.exitCriticalSection()
}

func (n *Node) exitCriticalSection() {
	n.mutex.Lock()
	n.inCS = false
	deferredCopy := make([]int32, len(n.deferredQueue))
	copy(deferredCopy, n.deferredQueue)
	n.deferredQueue = n.deferredQueue[:0] // Clear queue
	n.mutex.Unlock()

	log.Printf("Node %d EXITED Critical Section", n.id)

	// Send replies to all deferred requests
	for _, nodeID := range deferredCopy {
		go n.sendReply(nodeID)
		log.Printf("Node %d sent deferred REPLY to Node %d", n.id, nodeID)
	}
}

func setupLogging(nodeID int32) *os.File {
	if _, err := os.Stat("/home/jeppe/itu/ds/ricart-argawala/logs"); os.IsNotExist(err) {
		os.Mkdir("/home/jeppe/itu/ds/ricart-argawala/logs", 0755)
	}

	logFile, err := os.OpenFile(
		fmt.Sprintf("/home/jeppe/itu/ds/ricart-argawala/logs/node_%d.log", nodeID),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0666,
	)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	return logFile
}

func (n *Node) startServer() {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterRicartAgrawalaServer(s, n)

	log.Printf("Node %d gRPC server listening on port %s", n.id, n.port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (n *Node) connectToPeers() {
	for peerID, address := range n.peers {
		if peerID != n.id {
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to node %d: %v", peerID, err)
				continue
			}

			n.clients[peerID] = pb.NewRicartAgrawalaClient(conn)
			log.Printf("Node %d connected to Node %d at %s", n.id, peerID, address)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <node_id>")
	}

	nodeID, err := strconv.ParseInt(os.Args[1], 10, 0)
	if err != nil {
		log.Fatal("Invalid node ID")
	}

	// Setup file logging
	logFile := setupLogging(int32(nodeID))
	defer logFile.Close()

	port := fmt.Sprintf("500%d", nodeID)
	node := NewNode(int32(nodeID), port)

	node.peers[1] = "localhost:5001"
	node.peers[2] = "localhost:5002"
	node.peers[3] = "localhost:5003"

	// Start gRPC server
	go node.startServer()

	// Give time for all nodes to start
	time.Sleep(30 * time.Second)

	// Connect to other nodes
	node.connectToPeers()

	// Simulate random requests for critical section - make timing more varied
	go func() {
		// Give different initial delays but make them closer together
		time.Sleep(time.Duration(5+nodeID) * time.Second)
		for {
			node.requestCriticalSection()
			// Random delay between 3-8 seconds
			time.Sleep(time.Duration(3+nodeID) * time.Second)
		}
	}()

	select {}
}
