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
	established    bool // used to deal with new account scenario
}

// global variable for server instance
var acctMap map[string]*account         // accountId -> account. store all account information, using map for quicker lookup.
var clientLockMap map[string][]*account // clientId -> account. store a list of lock that client holds. used for releasing locks

func main() {
	acctMap = make(map[string]*account)
	clientLockMap = make(map[string][]*account)

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

		fmt.Println(conn.RemoteAddr())
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

	for {
		// process incoming message
		var req Request
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&req)
		if err != nil {
			return
		}
		fmt.Print(req.ClientId, ",", req.Operation, ",", req.Account, "|")
		resp := handleRequest(req)
		// sending reply message
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(resp)
		if err != nil {
			return
		}
	}
}

func handleRequest(req Request) Response {
	resp := Response{Status: Unknown}
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
		updateClientLockMap(req.ClientId, acct)
		fmt.Print(", balance:", acct.balance, ", ")
		printLock(acct)
		resp.Status = Success
		resp.Amount = acct.balance
	case Balance:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else {
			requestRL(acct, req.ClientId)
			updateClientLockMap(req.ClientId, acct)
			resp.Status = Success
			resp.Amount = acct.balance
			fmt.Print(", balance:", acct.balance, ", ")
			printLock(acct)
		}
	case Withdraw:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else {
			requestWL(acct, req.ClientId)
			updateClientLockMap(req.ClientId, acct)
			acct.balance -= req.Amount
			resp.Status = Success
			resp.Amount = acct.balance
			fmt.Print(", balance:", acct.balance, ", ")
			printLock(acct)
		}
	case Commit:
		// TODO: set all new created acct to be established
		releaseAllLock(req.ClientId)
		delete(clientLockMap, req.ClientId)
		resp.Status = Success
	case Abort:

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
	for {
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
}

func releaseAllLock(clientId string) {
	defer fmt.Println(clientId, " release clock")

	l := clientLockMap[clientId]
	for _, v := range l {
		if v.writeLockOwner == clientId {
			v.writeLockOwner = ""
			continue
		}

		for e := v.readLockOwner.Front(); e != nil; e = e.Next() {
			if e.Value == clientId {
				v.readLockOwner.Remove(e)
				return
			}
		}
	}
}

func updateClientLockMap(clientId string, acct *account) {
	l, found := clientLockMap[clientId]
	if !found {
		l = make([]*account, 0)
		clientLockMap[clientId] = l
	}

	// check if the acct is already in the slice
	for _, v := range l {
		if v == acct {
			return
		}
	}

	clientLockMap[clientId] = append(l, acct)
}

func printLock(acct *account) {
	fmt.Print("lock: ")
	for e := acct.readLockOwner.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, ",")
	}
	fmt.Print("|")
	fmt.Println(acct.writeLockOwner)
}
