package txmanager

import (
	"context"
	"golang_tcc/component"
	"time"
)

type TXStore interface {
	//创建一条事务明细记录
	CreateTX(ctx context.Context, components ...component.TCCComponent)(txID string, err error)
	//更新事务进度
	TXUpdate(ctx context.Context, txID string, componentID string, accept bool)error
	//提交事务的最终状态
	TXSubmit(ctx context.Context, txID string, success bool)error
	//获取所有未完成的事务
	GetHangingTXs(ctx context.Context)([]*Transaction, error)
	//获取指定事务
	GetTX(ctx context.Context, txID string)(*Transaction,error)
	//分布式锁
	Lock(ctx context.Context, expireDuration time.Duration)error
	//解锁
	Unlock(ctx context.Context)error
}
