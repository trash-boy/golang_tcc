package example

import (
	"context"
	"errors"
	"fmt"
	"github.com/demdxx/gocast"
	"github.com/xiaoxuxiansheng/redis_lock"
	"golang_tcc/component"
	"golang_tcc/example/pkg"
)

type TXStatus string
func(t TXStatus)String() string{
	return string(t)
}

const (
	TXTried TXStatus = "tried"
	TXConfirmed TXStatus = "confirmed"
	TXCanceled TXStatus = "canceled"
)

type DataStatus string

func (d DataStatus) String() string {
	return string(d)
}

const (
	DataFrozen     DataStatus = "frozen"     // 冻结态
	DataSuccessful DataStatus = "successful" // 成功态
)


type MockComponent struct {
	id string
	client *redis_lock.Client
}

func NewMockComponent(id string, client *redis_lock.Client)*MockComponent{
	return &MockComponent{
		id: id,
		client: client,
	}
}

func (m *MockComponent)ID()string{
	return m.id
}

func (m *MockComponent)Try(ctx context.Context, req *component.TCCReq)(*component.TCCResp, error){
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, req.TXID), m.client)
	if err := lock.Lock(ctx);err != nil{
		return nil,err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	txStatus,err := m.client.Get(ctx, pkg.BuildTXKey(m.id, req.TXID))
	if err != nil && !errors.Is(err,redis_lock.ErrNil){
		return nil,err
	}

	res := component.TCCResp{
		ComponentID: m.id,
		TXID: req.TXID,
	}

	switch txStatus {
	case TXTried.String(), TXConfirmed.String():
		res.ACK = true
		return &res,nil
	case TXCanceled.String():
		return &res,nil
	default:

	}

	bizID := gocast.ToString(req.Data["biz_id"])

	if _, err = m.client.Set(ctx, pkg.BuildTXDetailKey(m.id, req.TXID), bizID); err != nil {
		return nil, err
	}

	reply, err := m.client.SetNX(ctx, pkg.BuildDataKey(m.id, req.TXID, bizID), DataFrozen.String())
	if err != nil {
		return nil, err
	}
	if reply != 1 {
		return &res, nil
	}

	_, err = m.client.Set(ctx, pkg.BuildTXKey(m.id, req.TXID), TXTried.String())
	if err != nil {
		return nil, err
	}

	// try 请求执行成功
	res.ACK = true
	return &res, nil
}


func (m *MockComponent) Confirm(ctx context.Context, txID string) (*component.TCCResp, error) {
	// 基于 txID 维度加锁
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, txID), m.client)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	// 1. 要求 txID 此前状态为 tried
	txStatus, err := m.client.Get(ctx, pkg.BuildTXKey(m.id, txID))
	if err != nil {
		return nil, err
	}

	res := component.TCCResp{
		ComponentID: m.id,
		TXID:        txID,
	}
	switch txStatus {
	case TXConfirmed.String(): // 已 confirm，直接幂等响应为成功
		res.ACK = true
		return &res, nil
	case TXTried.String(): // 只有状态为 try 放行
	default: // 其他情况直接拒绝
		return &res, nil
	}

	// 获取事务对应的 bizID
	bizID, err := m.client.Get(ctx, pkg.BuildTXDetailKey(m.id, txID))
	if err != nil {
		return nil, err
	}

	// 2. 要求对应的数据状态此前为 frozen
	dataStatus, err := m.client.Get(ctx, pkg.BuildDataKey(m.id, txID, bizID))
	if err != nil {
		return nil, err
	}
	if dataStatus != DataFrozen.String() {
		// 非法的数据状态，拒绝
		return &res, nil
	}

	// 把对应数据处理状态置为 successful
	if _, err = m.client.Set(ctx, pkg.BuildDataKey(m.id, txID, bizID), DataSuccessful.String()); err != nil {
		return nil, err
	}

	// 把事务状态更新为成功，这一步哪怕失败了也不阻塞主流程
	_, _ = m.client.Set(ctx, pkg.BuildTXKey(m.id, txID), TXConfirmed.String())

	// 处理成功，给予成功的响应
	res.ACK = true
	return &res, nil
}

func (m *MockComponent) Cancel(ctx context.Context, txID string) (*component.TCCResp, error) {
	// 基于 txID 维度加锁
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, txID), m.client)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	// 查看事务的状态，只要不是 confirmed，就无脑置为 canceld
	txStatus, err := m.client.Get(ctx, pkg.BuildTXKey(m.id, txID))
	if err != nil && !errors.Is(err, redis_lock.ErrNil) {
		return nil, err
	}
	// 先 confirm 后 cancel，属于非法的状态扭转链路
	if txStatus == TXConfirmed.String() {
		return nil, fmt.Errorf("invalid tx status: %s, txid: %s", txStatus, txID)
	}

	// 根据事务获取对应的 bizID
	bizID, err := m.client.Get(ctx, pkg.BuildTXDetailKey(m.id, txID))
	if err != nil && errors.Is(err, redis_lock.ErrNil) {
		return nil, err
	}

	if bizID != "" {
		// 删除对应的 frozen 冻结记录
		if err = m.client.Del(ctx, pkg.BuildDataKey(m.id, txID, bizID)); err != nil {
			return nil, err
		}
	}

	// 把事务状态更新为 canceld
	_, err = m.client.Set(ctx, pkg.BuildTXKey(m.id, txID), TXCanceled.String())
	if err != nil {
		return nil, err
	}

	return &component.TCCResp{
		ACK:         true,
		ComponentID: m.id,
		TXID:        txID,
	}, nil
}

