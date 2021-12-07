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
	Aborted
	Unknown
)

const ()

type Request struct {
	Operation    int
	Account      string
	Amount       int
	ReadLockHeld []string
	Values       map[string]map[string]int // example: Values[Server][Account] is balance
	ClientId     string
}

type Response struct {
	Status int
	Amount int
}
