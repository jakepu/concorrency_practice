package main

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type account struct {
	balance        int
	readLockOwner  list.List
	writeLockOwner string
	committed      bool // used to deal with new account scenario
}

// global variable for server instance
var acctMap map[string]*account // accountId -> account. store all account information, using map for quicker lookup.

func main() {
	acctMap = make(map[string]*account)

	port := processConfigFile()
	// listen on port on localhost
	//fmt.Println(port)
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Cannot listen on port", err.Error())
		os.Exit(1)
	}
	defer ln.Close()

	//fmt.Println(ln.Addr())
	// continuous handle all incoming message till ctrl+c
	for {
		// accept connection
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Cannot accept incoming connection: ", err.Error())
			os.Exit(1)
		}

		//fmt.Println(conn.RemoteAddr())
		//fmt.Println(conn.LocalAddr())
		// handle request in a goroutine.
		go eventLoop(conn)
	}
}

func processConfigFile() string {
	args := os.Args
	serverId := args[1]
	fileName := args[2]

	// reading config file
	file, err := os.Open(fileName)

	if err != nil {
		fmt.Println("Error: ", err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		config := strings.Split(scanner.Text(), " ")
		if config[0] == serverId {
			file.Close()
			return config[2]
		}
	}

	file.Close()
	return ""
}

func eventLoop(conn net.Conn) {
	defer conn.Close()

	// scanner := bufio.NewScanner(conn)

	for {
		// process incoming message

		var req Request
		// req := Request{}
		// incoming := []byte(scanner.Text())
		// err := json.Unmarshal(incoming, &req)
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&req)
		if err != nil {
			return
		}

		resp := handleRequest(req)
		// outgoing, _ := json.Marshal(resp)
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(resp)
		if err != nil {
			return
		}
		// sending reply message
		// fmt.Fprint(conn, string(outgoing)+"\n")
	}
}

func handleRequest(req Request) Response {
	resp := Response{}
	switch req.Operation {
	case Deposit:
		acct, found := acctMap[req.Account]
		if !found {
			acct = &account{balance: req.Amount, writeLockOwner: req.ClientId}
			acctMap[req.Account] = acct
		} else {
			requestWL(acct, req.ClientId)
			acct.balance += req.Amount
		}
		resp.Status = Success
	case Balance:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else {
			requestRL(acct, req.ClientId)
			resp.Status = Success
			resp.Amount = acct.balance
		}
	case Withdraw:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else {
			requestWL(acct, req.ClientId)
			acct.balance -= req.Amount
		}
	case Commit:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else {
			releaseAllLock(acct, req.ClientId)
			resp.Status = Success
		}
	}

	return resp
}

func requestRL(acct *account, clientId string) {
	for {
		// check if client already has write lock
		if acct.writeLockOwner == clientId {
			return
		}
		// check if client already aquired read lock
		for e := acct.readLockOwner.Front(); e != nil; e = e.Next() {
			if e.Value == clientId {
				return
			}
		}
		// check if there is no writeLockOwn
		if acct.writeLockOwner == "" {
			acct.readLockOwner.PushBack(clientId)
			return
		}
		// wait and sleep 500 ms then check again
		time.Sleep(time.Millisecond * 500)
	}
}

func requestWL(acct *account, clientId string) {
	// check if client already has write lock
	if acct.writeLockOwner == clientId {
		return
	}
	// check if there is no writeLock
	if acct.writeLockOwner == "" {
		if acct.readLockOwner.Len() == 0 {
			acct.writeLockOwner = clientId
			return
		} else if acct.readLockOwner.Len() == 1 && acct.readLockOwner.Front().Value == clientId {
			acct.writeLockOwner = clientId
			acct.readLockOwner.Remove(acct.readLockOwner.Front())
			return
		}
	}
	// wait and sleep 500 ms then check again
	time.Sleep(time.Millisecond * 500)
}

func releaseAllLock(acct *account, clientId string) {
	if acct.writeLockOwner == clientId {
		acct.writeLockOwner = ""
		return
	}

	for e := acct.readLockOwner.Front(); e != nil; e = e.Next() {
		if e.Value == clientId {
			acct.readLockOwner.Remove(e)
			return
		}
	}
}
