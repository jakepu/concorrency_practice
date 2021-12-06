package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

var nodeId string
var isBegin bool
var serverConnPool map[string]net.Conn

const (
	numServers int = 5
)

func main() {
	configAndConnectServers()
	go processTransactions()
}

func processTransactions() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	for {
		scanner.Scan()
		line := strings.Split(scanner.Text(), " ")
		operation := line[0]
		var serverName string
		var account string
		target := line[1]
		amount := line[2]
		switch operation {

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

	// reading config file
	file, err := os.Open(configFile)
	if err != nil {
		panic("Cannot open config file")
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for i := 0; i < numServers; i++ {
		// example: node2 fa21-cs425-g01-02.cs.illinois.edu 1234
		scanner.Scan()
		line := strings.Split(scanner.Text(), " ")
		serverName := line[0]
		serverAddr := line[1]
		serverPort := line[2]
		serverAddr = serverAddr + ":" + serverPort
		tcpAddr, _ := net.ResolveTCPAddr("tcp4", serverAddr)
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
