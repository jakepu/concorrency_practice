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
var clientLockMap map[string][]*account // clientId -> account. store a list of read/write lock that client holds. used for releasing locks when user commit or abort

func main() {
	acctMap = make(map[string]*account)
	clientLockMap = make(map[string][]*account)

	port := processConfigFile()
	// listen on port on localhost
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Cannot listen on port", err.Error())
		os.Exit(1)
	}
	defer ln.Close()

	// continuous handle all incoming message till ctrl+c
	for {
		// accept connection
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Cannot accept incoming connection: ", err.Error())
			os.Exit(1)
		}

		// for debug
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

// each connection from clients will be handled in separate gorouting
func eventLoop(conn net.Conn) {
	defer conn.Close()

	for {
		// receiving request message
		var req Request
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&req)
		if err != nil {
			return
		}
		fmt.Print(req.ClientId, ",", req.Operation, ",", req.Account, "|")
		// processing request message and generating response message
		resp := handleRequest(req)
		// sending response message
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
			// create a new account and assign the write lock to the client
			acct = &account{balance: req.Amount, writeLockOwner: req.ClientId}
			acctMap[req.Account] = acct
		} else if acct.established {
			requestWL(acct, req.ClientId)
			acct.balance += req.Amount
			updateClientLockMap(req.ClientId, acct)
			resp.Status = Success
			resp.Amount = acct.balance
		} else {
			// this account is created by another active txn, we need to wait for the lock to be release to check again if this account is committed or aborted
			requestWL(acct, req.ClientId)
			acct, found := acctMap[req.Account]
			if !found {
				acct = &account{balance: req.Amount, writeLockOwner: req.ClientId}
				acctMap[req.Account] = acct
			} else {
				// this account must have been committed
				requestWL(acct, req.ClientId)
				acct.balance += req.Amount
				updateClientLockMap(req.ClientId, acct)
				resp.Status = Success
				resp.Amount = acct.balance
			}
		}

		// for debug
		fmt.Print(", balance:", acct.balance, ", ")
		printLock(acct)
	case Balance:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else if acct.established {
			requestRL(acct, req.ClientId)
			updateClientLockMap(req.ClientId, acct)
			resp.Status = Success
			resp.Amount = acct.balance

			// for debug
			fmt.Print(", balance:", acct.balance, ", ")
			printLock(acct)
		} else {
			requestRL(acct, req.ClientId)
			acct, found := acctMap[req.Account]
			if !found {
				resp.Status = AccountNotExist
			} else {
				requestRL(acct, req.ClientId)
				updateClientLockMap(req.ClientId, acct)
				resp.Status = Success
				resp.Amount = acct.balance

				// for debug
				fmt.Print(", balance:", acct.balance, ", ")
				printLock(acct)
			}
		}
	case Withdraw:
		acct, found := acctMap[req.Account]
		if !found {
			resp.Status = AccountNotExist
		} else if acct.established {
			requestWL(acct, req.ClientId)
			acct.balance -= req.Amount
			updateClientLockMap(req.ClientId, acct)
			resp.Status = Success
			resp.Amount = acct.balance

			// for debug
			fmt.Print(", balance:", acct.balance, ", ")
			printLock(acct)
		} else {
			requestWL(acct, req.ClientId)
			acct, found := acctMap[req.Account]
			if !found {
				resp.Status = AccountNotExist
			} else {
				requestWL(acct, req.ClientId)
				acct.balance -= req.Amount
				updateClientLockMap(req.ClientId, acct)
				resp.Status = Success
				resp.Amount = acct.balance

				// for debug
				fmt.Print(", balance:", acct.balance, ", ")
				printLock(acct)
			}
		}
	case Commit:
		confirmNewValues(req.Values)
		releaseAllLock(req.ClientId)
		resp.Status = Success
	case Abort:
		resetToOldValues(req.Values)
		releaseAllLock(req.ClientId)
		resp.Status = Aborted
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
	// for debug
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

	delete(clientLockMap, clientId)
}

func updateClientLockMap(clientId string, acct *account) {
	l, found := clientLockMap[clientId]
	if !found {
		l = make([]*account, 0)
		clientLockMap[clientId] = l
	}

	// check if the this client already has the lock
	for _, v := range l {
		if v == acct {
			return
		}
	}

	clientLockMap[clientId] = append(l, acct)
}

func confirmNewValues(newValues map[string]int) {
	// set all accounts to be established
	for acct := range newValues {
		acctMap[acct].established = true
	}
}

func resetToOldValues(oldValues map[string]int) {
	for acct, initValue := range oldValues {
		if acctMap[acct].established {
			// reset modified account to old value
			acctMap[acct].balance = initValue
		} else {
			// remove new created account
			delete(acctMap, acct)
		}
	}
}

// for debug
func printLock(acct *account) {
	fmt.Print("lock: ")
	for e := acct.readLockOwner.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, ",")
	}
	fmt.Print("|")
	fmt.Println(acct.writeLockOwner)
}
