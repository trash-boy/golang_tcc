package txmanager

import (
	"context"
	"errors"
	"fmt"
	"golang_tcc/component"
	"golang_tcc/log"
	"sync"
	"time"
)

type TXManager struct {
	ctx context.Context
	stop context.CancelFunc
	opts *Options
	txStore TXStore
	registerCenter *registerCenter
}

func NewTXManager(txStore TXStore, opts ...Option)*TXManager{
	ctx, cancel := context.WithCancel(context.Background())
	txManager := TXManager{
		opts: &Options{},
		txStore: txStore,
		registerCenter: newRegisterCenter(),
		ctx: ctx,
		stop: cancel,
	}
	for _,opt := range opts{
		opt(txManager.opts)
	}
	repair(txManager.opts)
	go txManager.run()
	return &txManager
}

func (t *TXManager)Stop(){
	t.stop()
}
func (t *TXManager)Register(component component.TCCComponent)error{
	return t.registerCenter.register(component)
}

func (t *TXManager)Transaction(ctx context.Context, reqs ...*RequestEntity)(bool,error){
	tctx,cancel := context.WithTimeout(ctx, t.opts.Timeout)
	defer cancel()

	//获得所有组件
	componentEntities,err := t.getComponents(tctx,reqs...)
	if err != nil{
		return false,err
	}

	//1.创建事务明细
	txID,err := t.txStore.CreateTX(tctx, componentEntities.ToComponents()...)
	if err != nil{
		return false,err
	}
	return t.twoPhaseCommit(ctx, txID, componentEntities)
}

func (t *TXManager) getComponents(ctx context.Context,reqs ...*RequestEntity) (ComponentEntities, error) {
	if len(reqs) == 0{
		return nil,errors.New("empty task")
	}

	//检查是否有相同的TCC组件
	idToReq := make(map[string]*RequestEntity, len(reqs))
	compontentIDs := make([]string, 0, len(reqs))
	for _,req := range reqs{
		if _,ok := idToReq[req.ComponentID];ok{
			return nil,fmt.Errorf("repeat component: %s", req.ComponentID)
		}
		idToReq[req.ComponentID] = req
		compontentIDs = append(compontentIDs, req.ComponentID)
	}

	//判断需要的TCC组件是否已经注册
	components,err := t.registerCenter.getComponents(compontentIDs...)
	if err != nil{
		return nil,err
	}
	if len(compontentIDs) != len(components){
		return  nil,errors.New("invalid componentIDs")
	}

	//将ReqEntitiy组装为ComponentEntities
	entities := make(ComponentEntities,0, len(compontentIDs))
	for _,component := range components{
		entities = append(entities, &ComponentEntity{
			Request: idToReq[component.ID()].Request,
			Component: component,
		})
	}
	return entities,nil
}


func (t *TXManager)backoffTick(tick time.Duration)time.Duration{
	tick <<= 1
	if threshold := t.opts.MonitorTick << 3; tick > threshold{
		tick = threshold
	}
	return tick
}

func (t *TXManager)run(){
	var tick time.Duration
	var err error
	for{
		if err == nil{
			tick = t.opts.MonitorTick
		}else{
			tick = t.backoffTick(tick)
		}
		select {
		case <- t.ctx.Done():
			return
		case <- time.After(tick):
			if err = t.txStore.Lock(t.ctx, t.opts.Timeout);err != nil{
				err = nil
				continue
			}

			var txs []*Transaction
			if txs,err = t.txStore.GetHangingTXs(t.ctx);err != nil{
				_ = t.txStore.Unlock(t.ctx)
				continue
			}

			err = t.batchAdvanceProgress(txs)
			_ = t.txStore.Unlock(t.ctx)

		}
	}
}

func (t *TXManager) batchAdvanceProgress(txs []*Transaction) error {
	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		for _,tx := range txs{
			tx := tx
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := t.advanceProgress(tx);err != nil{
					errCh <- err
				}
			}()
		}

		wg.Wait()
		close(errCh)
	}()

	var firstError error
	for err := range errCh{
		if firstError != nil{
			continue
		}
		firstError = err
	}
	return firstError
}

func (t *TXManager)advanceProgressByTXID(txID string)error{
	tx,err := t.txStore.GetTX(t.ctx, txID)
	if err != nil{
		return err
	}
	return t.advanceProgress(tx)

}

func (t *TXManager) advanceProgress(tx *Transaction) error{
	txStatus := tx.getStatus(time.Now().Add(-t.opts.Timeout))

	if txStatus == TXHanging{
		return nil
	}

	success := txStatus == TXSuccess
	var confirmOrCancel func(ctx context.Context, component component.TCCComponent)(*component.TCCResp,error)
	var txAdvanceProgress func(ctx context.Context)error

	if success{
		confirmOrCancel = func(ctx context.Context, component component.TCCComponent) (*component.TCCResp, error) {
			return component.Confirm(ctx, tx.TXID)
		}

		txAdvanceProgress = func(ctx context.Context) error {
			return t.txStore.TXSubmit(ctx, tx.TXID, true)
		}
	}else{
		confirmOrCancel = func(ctx context.Context, component component.TCCComponent) (*component.TCCResp, error) {
			return component.Cancel(ctx, tx.TXID)
		}

		txAdvanceProgress = func(ctx context.Context) error {
			return t.txStore.TXSubmit(ctx, tx.TXID, false)
		}
	}

	for _, component := range tx.Components{
		components,err := t.registerCenter.getComponents(component.ComponentID)
		if err != nil || len(components) == 0{
			return errors.New("get gcc component failed")
		}

		resp,err := confirmOrCancel(t.ctx, components[0])
		if err != nil{
			return err
		}
		if !resp.ACK {
			return fmt.Errorf("component: %s ack failed", component.ComponentID)
		}

	}
	return txAdvanceProgress(t.ctx)
}

func (t *TXManager) twoPhaseCommit(ctx context.Context, txID string, componentEntities ComponentEntities) (bool, error) {
	cctx,cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		for _,componentEntity := range componentEntities{
			componentEntity := componentEntity
			wg.Add(1)
			go func() {
				defer  wg.Done()
				resp,err := componentEntity.Component.Try(cctx, &component.TCCReq{
					ComponentID: componentEntity.Component.ID(),
					TXID: txID,
					Data: componentEntity.Request,
				})
				if err != nil || !resp.ACK{
					log.ErrorContextf(cctx, "tx try failed, tx id: %s, comonent id: %s, err: %v", txID, componentEntity.Component.ID(), err)

					if _err := t.txStore.TXUpdate(cctx,txID,componentEntity.Component.ID(),false);_err != nil{
						log.ErrorContextf(cctx, "tx updated failed, tx id: %s, component id: %s, err: %v", txID, componentEntity.Component.ID(), _err)
					}
					errCh <- fmt.Errorf("component: %s try failed", componentEntity.Component.ID())
					return
				}

				if err = t.txStore.TXUpdate(cctx, txID, componentEntity.Component.ID(), true); err != nil {
					log.ErrorContextf(cctx, "tx updated failed, tx id: %s, component id: %s, err: %v", txID, componentEntity.Component.ID(), err)
					errCh <- err
				}
			}()
		}
		wg.Wait()
		close(errCh)
	}()

	success := true
	if err := <- errCh;err != nil{
		success = false
		cancel()
	}

	go t.advanceProgressByTXID(txID)
	return success,nil

}



