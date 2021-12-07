package main

const (
	Deposit int = iota
	Balance
	Withdraw
	Commit
	Abort
)

const (
	Success int = iota
	AccountNotExist
	Unknown
)

type Request struct {
	clientId      string
	Operation     int
	Account       string
	Amount        int
	ReadLockHeld  []string
	UpdatedValues map[string]int
}

type Response struct {
	status int
	Amount int
}
