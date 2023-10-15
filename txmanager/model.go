package txmanager

import (
	"golang_tcc/component"
	"time"
)

//txmanager 与 tcc 交互时原始的数据，需要在进行一次封装变为TCCReq
type RequestEntity struct {
	ComponentID string 	`json:"componentName"`
	Request map[string]interface{} `json:"request"`
}

type ComponentEntity struct {
	Request map[string]interface{}
	Component  component.TCCComponent
}



type ComponentEntities []*ComponentEntity
func (c ComponentEntities)ToComponents() []component.TCCComponent{
	components := make([]component.TCCComponent, 0, len(c))
	for _,entity := range c{
		components = append(components, entity.Component)
	}
	return components
}

//TX状态
type TXStatus string

const (
	//冻结态
	TXHanging TXStatus = "hanging"

	//成功
	TXSuccess TXStatus = "success"

	//失败
	TXFailure TXStatus = "failure"
)

func (t TXStatus)String()string{
	return string(t)
}


//事务Try之后的状态
type ComponentTryStatus string
const (
	TryHanging ComponentTryStatus = "hanging"
	TrySuccess ComponentTryStatus = "success"
	TryFailure ComponentTryStatus = "failure"
)

func (c ComponentTryStatus) String() string {
	return string(c)
}

//todo 这个我还不知道有什么用
type ComponentTryEntity struct {
	ComponentID string
	TryStatus ComponentTryStatus
}


//事务
type Transaction struct {
	TXID string `json:"txID"`
	Components []*ComponentTryEntity
	Status TXStatus `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

func NewTransaction(txID string, componentEntities ComponentEntities)*Transaction{
	entities := make([]*ComponentTryEntity, 0, len(componentEntities))
	for _,entity := range componentEntities{
		entities = append(entities, &ComponentTryEntity{
			ComponentID: entity.Component.ID(),
		})
	}
	return &Transaction{
		TXID: txID,
		Components: entities,
	}
}

func (t *Transaction)getStatus(createdBefore time.Time)TXStatus{

	//TODO 我觉得有问题
	if t.CreatedAt.Before(createdBefore){
		return TXFailure
	}

	var hangingExist bool
	for _,component := range t.Components{
		if component.TryStatus == TryFailure{
			return TXFailure
		}
		hangingExist = hangingExist || (component.TryStatus == TryHanging)
	}

	if hangingExist{
		return TXHanging
	}
	return TXSuccess
}







