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
	key  string
	mkey string
	val  interface{}
	err  error
}

type request struct {
	id       string
	tasks    []Task
	count    int
	callback chan *result
}

type TaskCacheProvider interface {
	Put(string, interface{})
	Get(string) (interface{}, error)
}

type Option struct {
	maxWorker int
	batchSize int
	cp        TaskCacheProvider
}

func DefaultOption() *Option {
	return &Option{maxWorker: 8, batchSize: 32, cp: nil}
}

func (opt *Option) WithMaxWoker(max int) *Option {
	opt.maxWorker = max
	return opt
}

func (opt *Option) WithBatchSize(batchSize int) *Option {
	opt.batchSize = batchSize
	return opt
}

func (opt *Option) WithCacheProvider(cp TaskCacheProvider) *Option {
	opt.cp = cp
	return opt
}

func RegisterTaskGroup(taskGroup string, method func(...Task) (map[string]interface{}, error), opt *Option) {
	taskGroupMux.Lock()
	defer taskGroupMux.Unlock()
	if opt == nil {
		taskGroupDic[taskGroup] = newTaskGroup(taskGroup, 32, 8, method, nil)
	} else {
		taskGroupDic[taskGroup] = newTaskGroup(taskGroup, opt.batchSize, opt.maxWorker, method, opt.cp)
	}
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

func BatchExec(ctx context.Context, taskGroup string, in ...Task) map[string]interface{} {
	taskGroupMux.RLock()
	tg, ok := taskGroupDic[taskGroup]
	taskGroupMux.RUnlock()

	if !ok {
		return nil
	}

	count := len(in)

	req := &request{
		tasks:    in,
		count:    count,
		callback: make(chan *result, count),
	}
	tg.requestQueue <- req

	result := make(map[string]interface{})

	for res := range req.callback {
		if res.err != nil {
			result[res.key] = res.err
		} else {
			result[res.key] = res.val
		}
	}
	return result
}
