package component

import "context"

//构造TCC的请求参数
type TCCReq struct {
	ComponentID string `json:"componentID"`
	TXID string `json:"txID"`
	Data map[string]interface{} `json:"data"`

}

//构造TCC返回值
type TCCResp struct {
	ComponentID string `json:"componentID"`
	ACK bool `json:"ack"`
	TXID string `json:"txID"`
}

type TCCComponent interface {
	ID() string
	Try(ctx context.Context, req *TCCReq)(*TCCResp, error)
	Confirm(ctx context.Context, txID string)(*TCCResp, error)
	Cancel(ctx context.Context, txID string)(*TCCResp, error)
}
