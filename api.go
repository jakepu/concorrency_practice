package main

const (
	Deposit int = 0
	Balance
	Withdraw
	Commit
	Abort
)

const (
	AccountNotExist int = 0
	Success
)

type Request struct {
	Operation     string
	Account       string
	Amount        int
	ReadLockHeld  []string
	UpdatedValues map[string]int
}

type Response struct {
	status int
}
