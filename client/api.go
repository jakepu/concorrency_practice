package main

const (
	// client action
	Deposit int = iota
	Balance
	Withdraw
	Commit
	Abort
	// server status
	Success
	AccountNotExist
	Unknown
)

const ()

type Request struct {
	Operation     int
	Account       string
	Amount        int
	ReadLockHeld  []string
	UpdatedValues map[string]int
	ClientId      string
}

type Response struct {
	Status int
	Amount int
}
