package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var nodeId string
var hasBegun bool
var serverConnPool map[string]net.Conn

func main() {
	configAndConnectServers()
	processTransactions()
}

var transactionState map[string]map[string]int

// wait for response from server or ABORT from stdin
func sendRequestAndWait(serverName string, msg Request) {
	encoder := json.NewEncoder(serverConnPool[serverName])
	err := encoder.Encode(msg)
	// msgSerialized, err := json.Marshal(msg)
	// msgSerializedText := string(msgSerialized) + "\n"
	if err != nil {
		panic("Cannot encode the request msg.")
	}
	// fmt.Fprint(serverConnPool[serverName], msgSerializedText)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func(ctx context.Context) {

	}(ctx)

	decoder := json.NewDecoder(serverConnPool[serverName])
	var replyMsg Response
	err = decoder.Decode(&replyMsg)
	if err != nil {
		panic("Failed to receive and decode json")
	}
}

func processTransactions() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	for {
		scanner.Scan()
		oneLine := scanner.Text()
		line := strings.Split(oneLine, " ")
		operation := line[0]
		if operation == "BEGIN" {
			hasBegun = true
			transactionState = make(map[string]map[string]int)
			fmt.Println("OK")
			continue
		}
		if !hasBegun {
			continue
		}
		var serverName string
		var account string
		var target string
		var amount int
		if len(line) > 1 {
			target = line[1]
			tmp := strings.Split(target, ".")
			serverName = tmp[0]
			account = tmp[1]
			if len(line) > 2 {
				amount, _ = strconv.Atoi(line[2])
			}
		}

		var msg Request
		msg.ClientId = nodeId
		switch operation {
		case "DEPOSIT":
			msg.Operation = Deposit
			msg.Account = account
			msg.Amount = amount
			sendRequestAndWait(serverName, msg)
		case "BALANCE":
			msg.Operation = Balance
			msg.Account = account
			sendRequestAndWait(serverName, msg)
		case "WITHDRAW":
			msg.Operation = Withdraw
			msg.Account = account
			msg.Amount = amount
			sendRequestAndWait(serverName, msg)
		case "COMMIT":
			msg.Operation = Commit
			sendRequestAndWait(serverName, msg)
		}

		switch replyMsg.Status {
		case Success:
			fmt.Println("OK")
		case AccountNotExist:
			fmt.Println("NOT FOUND, ABORTED")
			hasBegun = false
		}

	}
}

func configAndConnectServers() {
	//example command: ./client {node id} {config file}
	args := os.Args
	if len(args) != 3 {
		panic("Not enough arguments.")
	}

	nodeId = args[1]
	configFile := args[2]

	serverConnPool = make(map[string]net.Conn)
	// reading config file
	file, err := os.Open(configFile)
	if err != nil {
		panic("Cannot open config file")
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		// example: node2 fa21-cs425-g01-02.cs.illinois.edu 1234
		line := strings.Split(scanner.Text(), " ")
		serverName := line[0]
		serverAddr := line[1]
		serverPort := line[2]
		serverAddr = serverAddr + ":" + serverPort
		tcpAddr, err := net.ResolveTCPAddr("tcp4", serverAddr)
		if err != nil {
			errStr := fmt.Sprintf("Cannot resolve TCP %s", serverAddr)
			panic(errStr)
		}
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			errStr := fmt.Sprintf("Cannot connect to server %s", serverAddr)
			panic(errStr)
		}
		err = conn.SetKeepAlive(true)
		if err != nil {
			errStr := fmt.Sprintf("Cannot keep TCP connection to server %s alive", serverAddr)
			panic(errStr)
		}
		serverConnPool[serverName] = conn
	}

	file.Close()
}
