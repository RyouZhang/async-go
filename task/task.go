package task

import (
	"context"
	"fmt"
	"sync"
)

var (
	taskGroupDic map[string]*taskGroup
	taskGroupMux sync.RWMutex
)

func init() {
	taskGroupDic = make(map[string]*taskGroup)
}

type Task interface {
	UniqueId() string //must unique id
}
type MergeTask interface {
	MergeBy() string
}
type GroupTask interface {
	GroupBy() string
}

type result struct {
	key string
	mkey string
	val interface{}
	err error
}

type request struct {
	id       string
	tasks    []Task
	count    int
	callback chan *result
}

func RegisterTaskGroup(taskGroup string, maxWorker int, batchSize int, method func(...Task) (map[string]interface{}, error)) {
	taskGroupMux.Lock()
	defer taskGroupMux.Unlock()
	taskGroupDic[taskGroup] = newTaskGroup(taskGroup, batchSize, maxWorker, method)
}

func Exec(ctx context.Context, taskGroup string, in Task) (interface{}, error) {
	taskGroupMux.RLock()
	tg, ok := taskGroupDic[taskGroup]
	taskGroupMux.RUnlock()

	if !ok {
		return nil, fmt.Errorf("invalid task group %s", taskGroup)
	}

	req := &request{
		tasks:    []Task{in},
		count:    1,
		callback: make(chan *result, 1),
	}
	tg.requestQueue <- req

	res := <-req.callback
	if res.err != nil {
		return nil, res.err
	}
	return res.val, nil
}
