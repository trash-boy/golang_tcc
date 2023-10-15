package example

import (
	"context"
	"testing"
	"time"

	"golang_tcc/example/dao"
	"golang_tcc/example/pkg"
	"golang_tcc/txmanager"
)

const (
	dsn      = "root:123456@tcp(127.0.0.1:3306)/bubble"
	network  = "tcp"
	address  = "127.0.0.1:6379"
	password = "123456"
)

func Test_TCC(t *testing.T) {
	redisClient := pkg.NewRedisClient(network, address, password)
	t.Log(redisClient)
	mysqlDB, err := pkg.NewDB(dsn)
	if err != nil {
		t.Error(err)
		return
	}

	componentAID := "componentA"
	componentBID := "componentB"
	componentCID := "componentC"

	// 构造出对应的 tcc component
	componentA := NewMockComponent(componentAID, redisClient)
	componentB := NewMockComponent(componentBID, redisClient)
	componentC := NewMockComponent(componentCID, redisClient)

	// 构造出事务日志存储模块
	txRecordDAO := dao.NewTXRecordDAO(mysqlDB)
	txStore := NewMockTXStore(txRecordDAO, redisClient)

	txManager := txmanager.NewTXManager(txStore, txmanager.WithMonitorTick(time.Second))
	defer txManager.Stop()

	// 完成各组件的注册
	if err := txManager.Register(componentA); err != nil {
		t.Error(err)
		return
	}

	if err := txManager.Register(componentB); err != nil {
		t.Error(err)
		return
	}

	if err := txManager.Register(componentC); err != nil {
		t.Error(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	success, err := txManager.Transaction(ctx, []*txmanager.RequestEntity{
		{ComponentID: componentAID,
			Request: map[string]interface{}{
				"biz_id": componentAID + "_biz",
			},
		},
		{ComponentID: componentBID,
			Request: map[string]interface{}{
				"biz_id": componentBID + "_biz",
			},
		},
		{ComponentID: componentCID,
			Request: map[string]interface{}{
				"biz_id": componentCID + "_biz",
			},
		},
	}...)
	if err != nil {
		t.Errorf("tx failed, err: %v", err)
		return
	}
	if !success {
		t.Error("tx failed")
		return
	}

	<-time.After(2 * time.Second)

	t.Log("success")
}
