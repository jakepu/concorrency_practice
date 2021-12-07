package main

import (
	"bufio"
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
var scanner *bufio.Scanner // stdin scanner

type transactionState struct {
	currValues   map[string]map[string]int
	backupValues map[string]map[string]int
	serverNames  map[string]bool
}

var currState transactionState

func main() {
	configAndConnectServers()
	processTransactions()
}

func sendRequest(serverName string, msg Request) {
	encoder := json.NewEncoder(serverConnPool[serverName])
	err := encoder.Encode(msg)
	// msgSerialized, err := json.Marshal(msg)
	// msgSerializedText := string(msgSerialized) + "\n"
	if err != nil {
		panic("Cannot encode the request msg.")
	}
	// fmt.Fprint(serverConnPool[serverName], msgSerializedText)
}

func getResponse(serverName string) Response {
	decoder := json.NewDecoder(serverConnPool[serverName])
	var replyMsg Response
	err := decoder.Decode(&replyMsg)
	if err != nil {
		panic("Failed to receive and decode json")
	}
	return replyMsg
}

func sendRequestAndGetResponse(serverName string, req Request, responseChan chan<- Response) {
	sendRequest(serverName, req)
	replyMsg := getResponse(serverName)
	printResponse(serverName, req, replyMsg)
	responseChan <- replyMsg

}

func initTransactionState() {
	hasBegun = false
	currState = transactionState{make(map[string]map[string]int),
		make(map[string]map[string]int),
		make(map[string]bool)}
}

func printResponse(serverName string, req Request, resp Response) {
	switch resp.Status {
	case Success:
		if req.Operation == Balance {
			fmt.Printf("%s.%s = %d\n", serverName, req.Account, resp.Amount)
		} else {
			fmt.Println("OK")
		}

	case AccountNotExist:
		fmt.Println("NOT FOUND, ABORTED")
	case Aborted:
		fmt.Println("ABORTED")
	case Unknown:
		panic("Received status 'Unknown' from server")
	}
}

func isCurrBalancesValid() bool {
	for _, accountMap := range currState.currValues {
		for _, value := range accountMap {
			if value < 0 {
				return false
			}
		}
	}
	return true
}

func processResponse(operation int, serverName string, account string, amount int, resp Response, shouldScan *bool, lineBuf *string) {
	switch resp.Status {
	case Success:
		if _, found := currState.currValues[serverName]; !found {
			currState.currValues[serverName] = make(map[string]int)
		}
		if _, found := currState.currValues[serverName][account]; !found {
			// If this transaction has not modified this account's states yet
			// we should save its value before we modify it
			if _, found := currState.backupValues[serverName]; !found {
				currState.backupValues[serverName] = make(map[string]int)
			}
			switch operation {
			case Balance:
				currState.backupValues[serverName][account] = resp.Amount
			case Deposit:
				currState.backupValues[serverName][account] = resp.Amount - amount
			case Withdraw:
				currState.backupValues[serverName][account] = resp.Amount + amount
			}
		}
		currState.currValues[serverName][account] = resp.Amount
		currState.serverNames[serverName] = true
	case Aborted:
		sendSilentAbortBut(serverName)
		initTransactionState()
	default:
		sendSilentAbort()
		initTransactionState()
	}

}

func processTransactions() {
	shouldScan := true
	var lineBuf string
	for {
		var oneLine string
		if shouldScan {
			scanner.Scan()
			oneLine = scanner.Text()
		} else {
			oneLine = lineBuf
		}
		line := strings.Split(oneLine, " ")
		operation := line[0]
		if operation == "BEGIN" {
			hasBegun = true
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
			responseChan := make(chan Response)
			go sendRequestAndGetResponse(serverName, msg, responseChan)
			// scan next line
			scanner.Scan()
			lineBuf = scanner.Text()
			switch lineBuf {
			case "ABORT":
				msg.Operation = Abort
				sendRequest(serverName, msg)
				fmt.Println("ABORTED")
				shouldScan = true
			default:
				shouldScan = false
			}
			resp := <-responseChan
			processResponse(Deposit, serverName, account, amount, resp, &shouldScan, &lineBuf)
			if msg.Operation == Abort && resp.Status != Aborted {
				resp = getResponse(serverName)
				processResponse(Deposit, serverName, account, amount, resp, &shouldScan, &lineBuf)
			}
		case "BALANCE":
			msg.Operation = Balance
			msg.Account = account
			responseChan := make(chan Response)
			go sendRequestAndGetResponse(serverName, msg, responseChan)
			// scan next line
			scanner.Scan()
			lineBuf = scanner.Text()
			switch lineBuf {
			case "ABORT":
				msg.Operation = Abort
				sendRequest(serverName, msg)
				fmt.Println("ABORTED")
				shouldScan = true
			default:
				shouldScan = false
			}
			resp := <-responseChan
			processResponse(Balance, serverName, account, 0, resp, &shouldScan, &lineBuf)
			if msg.Operation == Abort && resp.Status != Aborted {
				getResponse(serverName)
				processResponse(Balance, serverName, account, 0, resp, &shouldScan, &lineBuf)
			}
		case "WITHDRAW":
			msg.Operation = Withdraw
			msg.Account = account
			msg.Amount = amount
			responseChan := make(chan Response)
			go sendRequestAndGetResponse(serverName, msg, responseChan)
			// scan next line
			scanner.Scan()
			lineBuf = scanner.Text()
			switch lineBuf {
			case "ABORT":
				msg.Operation = Abort
				sendRequest(serverName, msg)
				fmt.Println("ABORTED")
				shouldScan = true
			default:
				shouldScan = false
			}
			resp := <-responseChan
			processResponse(Withdraw, serverName, account, amount, resp, &shouldScan, &lineBuf)
			if msg.Operation == Abort && resp.Status != Aborted {
				getResponse(serverName)
				processResponse(Withdraw, serverName, account, amount, resp, &shouldScan, &lineBuf)
			}
		case "COMMIT":
			if isCurrBalancesValid() {
				msg.Operation = Commit
				for server := range currState.serverNames {
					msg.Values = currState.currValues[server]
					sendRequest(server, msg)
					getResponse(server)
				}
				fmt.Println("COMMIT OK")
			} else {
				sendSilentAbort()
				fmt.Println("ABORTED")
			}
			shouldScan = true
			initTransactionState()

		case "ABORT":
			sendSilentAbort()
			fmt.Println("ABORTED")
			shouldScan = true
			initTransactionState()
		}

	}
}
func sendSilentAbortBut(sentServer string) {
	var msg Request
	msg.Operation = Abort
	for server := range currState.serverNames {
		if server == sentServer {
			continue
		}
		msg.Values = currState.backupValues[server]
		sendRequest(server, msg)
		getResponse(server)
	}
}
func sendSilentAbort() {
	var msg Request
	msg.Operation = Abort
	for server := range currState.serverNames {
		msg.Values = currState.backupValues[server]
		for account, val := range currState.backupValues[server] {
			fmt.Printf("Server %s Account %s val %d\n", server, account, val)
		}
		sendRequest(server, msg)
		getResponse(server)
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

func init() {
	scanner = bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	initTransactionState()
}
