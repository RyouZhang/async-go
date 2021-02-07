package async

import (
	"time"
)

type Query struct {
	key             interface{}
	successCallback chan interface{}
	errorCallback   chan error
}

type GQuery struct {
	keys []interface{}
}

type QResult struct {
	keys  []interface{}
	pairs map[interface{}]interface{}
	err   error
}

type Batch struct {
	kernal     func([]interface{}) (map[interface{}]interface{}, error)
	branchSize int
	interval   time.Duration
	shutdown   chan bool
	input      chan Query
	output     chan QResult
}

func NewBatch(
	kernal func([]interface{}) (map[interface{}]interface{}, error),
	branchSize int,
	interval time.Duration) *Batch {
	p := &Batch{
		kernal:     kernal,
		shutdown:   make(chan bool),
		branchSize: branchSize,
		interval:   interval,
		input:      make(chan Query, 128),
		output:     make(chan QResult, 32)}
	go p.run()
	return p
}

func (p *Batch) doing(gq GQuery) {
	defer func() {
		if e := recover(); e != nil {
			p.output <- QResult{err: e.(error), keys: gq.keys}
		}
	}()
	pairs, err := p.kernal(gq.keys)
	p.output <- QResult{err: err, pairs: pairs, keys: gq.keys}
}

func (p *Batch) run() {
	mqDic := make(map[interface{}][]Query)
	missKeys := make(map[interface{}]bool)
	timer := time.NewTimer(p.interval)
	for {
		select {
		case <-p.shutdown:
			{
				if timer != nil {
					timer.Stop()
				}
				return
			}
		case q := <-p.input:
			{
				target, ok := mqDic[q.key]
				if ok {
					mqDic[q.key] = append(target, q)
					continue
				}
				mqDic[q.key] = []Query{q}
				missKeys[q.key] = true

				if len(missKeys) >= p.branchSize {
					keys := make([]interface{}, len(missKeys))
					index := 0
					for key, _ := range missKeys {
						keys[index] = key
						index = index + 1
					}
					gq := GQuery{keys: keys}
					missKeys = make(map[interface{}]bool)
					go p.doing(gq)
					timer.Reset(p.interval)
				}
			}
		case r := <-p.output:
			{
				if r.err != nil {
					for _, key := range r.keys {
						querys, ok := mqDic[key]
						if ok {
							for _, q := range querys {
								q.errorCallback <- r.err
							}
						}
						delete(mqDic, key)
					}
					continue
				}
				for _, key := range r.keys {
					querys, ok := mqDic[key]
					if ok {
						for _, q := range querys {
							q.successCallback <- r.pairs[key]
						}
					} else {
						err := fmt.Errorf("invalid key:%s", key)
						for _, q := range querys {
							q.errorCallback <- err
						}
					}
					delete(mqDic, key)
				}
			}
		case <-timer.C:
			{
				if len(missKeys) > 0 {
					keys := make([]interface{}, len(missKeys))
					index := 0
					for key, _ := range missKeys {
						keys[index] = key
						index = index + 1
					}
					gq := GQuery{keys: keys}
					missKeys = make(map[interface{}]bool)
					go p.doing(gq)
				}
				timer.Reset(p.interval)
			}
		}
	}
}

func (p *Batch) MGet(keys []interface{}) (map[interface{}]interface{}, error) {
	res := Foreach(keys, func(index int) (interface{}, error) {
		successCallback := make(chan interface{}, 1)
		errorCallback := make(chan error, 1)

		defer close(successCallback)
		defer close(errorCallback)

		p.input <- Query{key: keys[index], successCallback: successCallback, errorCallback: errorCallback}
		select {
		case err := <-errorCallback:
			{
				return nil, err
			}
		case result := <-successCallback:
			{
				return result, nil
			}
		}
	}, Unlimit)

	result := make(map[interface{}]interface{})
	for index, _ := range res {
		switch res[index].(type) {
		case error:
			return nil, res[index].(error)
		default:
			result[keys[index]] = res[index]
		}
	}
	return result, nil
}

func (p *Batch) Get(key interface{}) (interface{}, error) {
	successCallback := make(chan interface{}, 1)
	errorCallback := make(chan error, 1)

	defer close(successCallback)
	defer close(errorCallback)

	p.input <- Query{key: key, successCallback: successCallback, errorCallback: errorCallback}
	select {
	case err := <-errorCallback:
		{
			return nil, err
		}
	case result := <-successCallback:
		{
			return result, nil
		}
	}
}
