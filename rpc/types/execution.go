package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/davecgh/go-spew/spew"
)

type GethTraceCallResultWrapper struct {
	Result *GethTraceCallResult
}

type GethTraceCallResult struct {
	TransactionPosition int
	Time                string
	GasUsed             string
	From                common.Address
	To                  common.Address
	Value               string
	Gas                 string
	Input               string
	Output              string
	Error               string
	Type                string
	Calls               []*GethTraceCallResult
}

type ParityTraceResult struct {
	Action struct {
		CallType      string `json:"callType"`
		From          string `json:"from"`
		Gas           string `json:"gas"`
		Input         string `json:"input"`
		To            string `json:"to"`
		Value         string `json:"value"`
		Init          string `json:"init"`
		Address       string `json:"address"`
		Balance       string `json:"balance"`
		RefundAddress string `json:"refundAddress"`
		Author        string `json:"author"`
		RewardType    string `json:"rewardType"`
	} `json:"action"`
	BlockHash   string `json:"blockHash"`
	BlockNumber int    `json:"blockNumber"`
	Error       string `json:"error"`
	Result      struct {
		GasUsed string `json:"gasUsed"`
		Code    string `json:"code"`
		Output  string `json:"output"`
		Address string `json:"address"`
	} `json:"result"`

	Subtraces           int     `json:"subtraces"`
	TraceAddress        []int64 `json:"traceAddress"`
	TransactionHash     string  `json:"transactionHash"`
	TransactionPosition int     `json:"transactionPosition"`
	Type                string  `json:"type"`
}

func (trace *ParityTraceResult) ConvertFields() ([]byte, []byte, []byte, string) {
	var from, to, value []byte
	txType := trace.Type

	switch trace.Type {
	case "create":
		from = common.FromHex(trace.Action.From)
		to = common.FromHex(trace.Result.Address)
		value = common.FromHex(trace.Action.Value)
	case "suicide":
		from = common.FromHex(trace.Action.Address)
		to = common.FromHex(trace.Action.RefundAddress)
		value = common.FromHex(trace.Action.Balance)
	case "call":
		from = common.FromHex(trace.Action.From)
		to = common.FromHex(trace.Action.To)
		value = common.FromHex(trace.Action.Value)
		txType = trace.Action.CallType
	default:
		spew.Dump(trace)
	}
	return from, to, value, txType
}