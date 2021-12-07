package main

const (
	// client action
	Deposit int = iota
	Balance
	Withdraw
	Commit
	PreAbort // what a hack
	Abort
	// server status
	Success
	AccountNotExist
	PreAborted
	Aborted
	Unknown
)

const ()

type Request struct {
	Operation    int
	Account      string
	Amount       int
	ReadLockHeld []string
	Values       map[string]int
	ClientId     string
}

type Response struct {
	Status int
	Amount int
}
