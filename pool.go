package async

import (
	"fmt"
	"sync"
)

var (
	poolDicMux sync.RWMutex
	poolDic    map[string]*pool
)

type pool struct {
	queue chan bool
}

func init() {
	poolDic = make(map[string]*pool)
}

func RegisterPool(name string, maxWorkers int) error {
	poolDicMux.Lock()
	defer poolDicMux.Unlock()
	_, ok := poolDic[name]
	if ok {
		return fmt.Errorf("duplicate pool name:%s", name)
	}
	poolDic[name] = &pool{
		queue: make(chan bool, maxWorkers),
	}
	return nil
}

func Pool(poolName string, count int, method func(int) (any, error)) []any {
	poolDicMux.RLock()
	p, ok := poolDic[poolName]
	poolDicMux.RUnlock()
	if false == ok {
		panic(fmt.Errorf("invalid pool name:%s", poolName))
	}

	var wg sync.WaitGroup
	results := make([]any, count)
	for index := 0; index < count; index++ {
		p.queue <- true
		wg.Add(1)
		go func(i int, method func(int) (any, error)) {
			defer func() {
				<-p.queue
				wg.Done()
			}()
			res, err := Safety(func() (any, error) {
				return method(i)
			})
			if err != nil {
				results[i] = err
			} else {
				results[i] = res
			}
		}(index, method)
	}
	wg.Wait()
	return results
}
