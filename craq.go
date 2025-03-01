package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// DataItem represents a single piece of data.
type DataItem struct {
	Key   string
	Value string
}

// Node represents a replica node in the CRAQ chain.
type Node struct {
	ID           int
	Address      string
	NextNode     *rpc.Client
	PrevNode     *rpc.Client
	Data         map[string]string // In-memory data store
	Mutex        sync.RWMutex
	IsHead       bool
	IsTail       bool
	ChainLength  int
	OtherNodes   map[int]string //Addresses of other nodes in chain
}

// CRAQArgs are the arguments for RPC calls.
type CRAQArgs struct {
	Key       string
	Value     string
	NodeId    int
	ChainLen  int
	Operation string // "read" or "write"
	Timestamp int64 //for conflict
}

// CRAQReply is the reply from RPC calls.
type CRAQReply struct {
	Value   string
	Success bool
	Error   string
}

// NewNode creates a new Node instance.
func NewNode(id int, address string, chainLength int) *Node {
	return &Node{
		ID:          id,
		Address:     address,
		Data:        make(map[string]string),
		IsHead:      false,
		IsTail:      false,
		ChainLength: chainLength,
		OtherNodes: make(map[int]string),
	}
}

// SetNextNode sets the next node in the chain.
func (n *Node) SetNextNode(nextNodeAddress string) error {
	client, err := rpc.Dial("tcp", nextNodeAddress)
	if err != nil {
		return err
	}
	n.NextNode = client
	return nil
}

// SetPrevNode sets the previous node in the chain.
func (n *Node) SetPrevNode(prevNodeAddress string) error {
	client, err := rpc.Dial("tcp", prevNodeAddress)
	if err != nil {
		return err
	}
	n.PrevNode = client
	return nil
}

// Read handles read requests.
func (n *Node) Read(args *CRAQArgs, reply *CRAQReply) error {
	log.Printf("Node %d: Received Read request for key: %s", n.ID, args.Key)

	n.Mutex.RLock()
	defer n.Mutex.RUnlock()

	value, ok := n.Data[args.Key]
	if ok {
		reply.Value = value
		reply.Success = true
	} else {
		reply.Value = ""
		reply.Success = false
		reply.Error = "Key not found"
	}
	return nil
}

// Write handles write requests.
func (n *Node) Write(args *CRAQArgs, reply *CRAQReply) error {
	log.Printf("Node %d: Received Write request for key: %s, value: %s", n.ID, args.Key, args.Value)
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	//Forward to next node if it's not the tail
	if !n.IsTail {
		var nextReply CRAQReply
		err := n.NextNode.Call("Node.Write", args, &nextReply)
		if err != nil {
			reply.Error = fmt.Sprintf("Node %d: forward write to next error %s", n.ID, err.Error())
			reply.Success = false
			return err
		}
		if !nextReply.Success {
			reply.Error = fmt.Sprintf("Node %d: next node write error %s", n.ID, nextReply.Error)
			reply.Success = false
			return errors.New(reply.Error)
		}
		reply.Success = true
		return nil
	}

	// Is Tail node.
	n.Data[args.Key] = args.Value
	reply.Success = true

	return nil
}

// Serve starts the RPC server.
func (n *Node) Serve() {
	rpc.Register(n)
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		log.Fatalf("Node %d: Failed to start server: %v", n.ID, err)
	}
	log.Printf("Node %d: Serving on %s", n.ID, n.Address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Node %d: Accept error: %v", n.ID, err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (n *Node) ReadChain(args *CRAQArgs, reply *CRAQReply) error {
	log.Printf("Node %d: Received Read request for key: %s", n.ID, args.Key)
	if args.ChainLen != n.ChainLength {
		reply.Success = false
		reply.Error = fmt.Sprintf("request chain length:%d, node chain length:%d", args.ChainLen, n.ChainLength)
		return errors.New(reply.Error)
	}

	if n.ID == args.NodeId {
		n.Mutex.RLock()
		defer n.Mutex.RUnlock()
		value, ok := n.Data[args.Key]
		if ok {
			reply.Value = value
			reply.Success = true
		} else {
			reply.Value = ""
			reply.Success = false
			reply.Error = "Key not found"
		}
		return nil
	}

	if n.PrevNode != nil {
		var prevReply CRAQReply
		err := n.PrevNode.Call("Node.ReadChain", args, &prevReply)
		if err != nil {
			reply.Error = fmt.Sprintf("Node %d: forward read to prev node error %s", n.ID, err.Error())
			reply.Success = false
			return err
		}
		reply.Success = prevReply.Success
		reply.Value = prevReply.Value
		reply.Error = prevReply.Error
		return nil
	}

	reply.Success = false
	reply.Error = fmt.Sprintf("Node %d: should not reach here", n.ID)
	return errors.New(reply.Error)
}

func main() {
	// Example usage
	// Create 3 nodes
	chainLength := 3
	node1 := NewNode(1, ":8081", chainLength)
	node2 := NewNode(2, ":8082", chainLength)
	node3 := NewNode(3, ":8083", chainLength)

	node1.IsHead = true
	node3.IsTail = true

	node1.OtherNodes = map[int]string{2:":8082", 3:":8083"}
	node2.OtherNodes = map[int]string{1:":8081", 3:":8083"}
	node3.OtherNodes = map[int]string{1:":8081", 2:":8082"}

	// Set up the chain
	node1.SetNextNode(node2.Address)
	node2.SetNextNode(node3.Address)
	node2.SetPrevNode(node1.Address)
	node3.SetPrevNode(node2.Address)

	// Start the servers
	go node1.Serve()
	go node2.Serve()
	go node3.Serve()

	// Wait for servers to start
	time.Sleep(2 * time.Second)

	// Example usage: Write to the tail
	client3, err := rpc.Dial("tcp", node3.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	writeArgs := &CRAQArgs{Key: "key1", Value: "value1"}
	var writeReply CRAQReply
	err = client3.Call("Node.Write", writeArgs, &writeReply)
	if err != nil {
		log.Fatalf("write err %v", err)
	}

	if writeReply.Success {
		log.Println("Write operation successful")
	} else {
		log.Printf("Write operation failed: %s", writeReply.Error)
	}

	// Example usage: Read from different nodes, with apportioned queries
	readArgs1 := &CRAQArgs{Key: "key1", NodeId: 1, ChainLen: chainLength}
	var readReply1 CRAQReply
	client1, err := rpc.Dial("tcp", node1.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	err = client1.Call("Node.ReadChain", readArgs1, &readReply1)
	if err != nil {
		log.Fatalf("read err %v", err)
	}
	if readReply1.Success {
		log.Printf("Read from node1: key1 value is %s", readReply1.Value)
	} else {
		log.Printf("Read from node1 failed: %s", readReply1.Error)
	}

	//random read from node 1,2,3
	for i := 0; i < 5; i++ {
		readNodeId := rand.Intn(chainLength) + 1
		var client *rpc.Client
		switch readNodeId {
		case 1:
			client = client1
		case 2:
			client, _ = rpc.Dial("tcp", node2.Address)
		case 3:
			client = client3
		}
		readArgs := &CRAQArgs{Key: "key1", NodeId: readNodeId, ChainLen: chainLength}
		var readReply CRAQReply
		err = client.Call("Node.ReadChain", readArgs, &readReply)
		if err != nil {
			log.Fatalf("read err %v", err)
		}
		if readReply.Success {
			log.Printf("Read from node%d: key1 value is %s", readNodeId, readReply.Value)
		} else {
			log.Printf("Read from node%d failed: %s", readNodeId, readReply.Error)
		}
		time.Sleep(time.Second)
	}

	// Example usage: Write to the tail
	writeArgs2 := &CRAQArgs{Key: "key2", Value: "value2"}
	var writeReply2 CRAQReply
	err = client3.Call("Node.Write", writeArgs2, &writeReply2)
	if err != nil {
		log.Fatalf("write err %v", err)
	}

	if writeReply2.Success {
		log.Println("Write operation successful")
	} else {
		log.Printf("Write operation failed: %s", writeReply2.Error)
	}

	//Example usage: Read from different nodes, with apportioned queries
	readArgs2 := &CRAQArgs{Key: "key2", NodeId: 2, ChainLen: chainLength}
	var readReply2 CRAQReply

	err = client1.Call("Node.ReadChain", readArgs2, &readReply2)
	if err != nil {
		log.Fatalf("read err %v", err)
	}
	if readReply2.Success {
		log.Printf("Read from node2: key2 value is %s", readReply2.Value)
	} else {
		log.Printf("Read from node2 failed: %s", readReply2.Error)
	}

	// Keep the program running
	select {}
}
