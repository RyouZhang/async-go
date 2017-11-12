package async

import (
	"time"
)

type Query struct {
	keys            []interface{}
	successCallback chan map[interface{}]interface{}
	errorCallback   chan error
}

type GQuery struct {
	keys   []interface{}
	querys []Query
}

type QResult struct {
	querys []Query
	pairs  map[interface{}]interface{}
	err    error
}

type Merge struct {
	kernal func([]interface{}) (map[interface{}]interface{}, error)
	branchSize int
	interval	time.Duration
	shutdown   chan bool
	input      chan Query
	output     chan QResult
}

func NewMerge(
	name string,
	kernal func([]interface{}) (map[interface{}]interface{}, error),
	branchSize int, 
	interval time.Duration) *Merge {
	p := &Merge{
		kernal:     kernal,
		shutdown:   make(chan bool),
		branchSize: branchSize,
		interval: interval,
		input:      make(chan Query, 64),
		output:     make(chan QResult, 64)}
	go p.run()
	return p
}

func (p *Merge) doing(gq GQuery) {
	defer func() {
		if e := recover(); e != nil {
			p.output <- QResult{err: e.(error), querys: gq.querys}
		}
	}()
	pairs, err := p.kernal(gq.keys)
	p.output <- QResult{err: err, querys: gq.querys, pairs: pairs}
}


func (p *Merge) run() {
	querys := []Query{}
	missKeys := make(map[interface{}]bool)
	timer := time.NewTimer(50 * time.Millisecond)
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
				for _, key := range q.keys {
					missKeys[key] = true
				}
				querys = append(querys, q)
				if len(missKeys) >= p.branchSize {
					keys := make([]interface{}, len(missKeys))
					index := 0
					for key, _ := range missKeys {
						keys[index] = key
						index = index + 1
					}
					gq := GQuery{keys: keys, querys: querys}
					querys = []Query{}
					missKeys = make(map[interface{}]bool)
					go p.doing(gq)
					timer.Reset(50 * time.Millisecond)
				}
			}
		case r := <-p.output:
			{
				if r.err != nil {
					for _, q := range r.querys {
						q.errorCallback <- r.err
					}
				} else {
					for _, q := range r.querys {
						result := make(map[interface{}]interface{})
						for _, key := range q.keys {
							value, ok := r.pairs[key]
							if ok {
								result[key] = value
							}
						}
						q.successCallback <- result
					}
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
					gq := GQuery{keys: keys, querys: querys}
					querys = []Query{}
					missKeys = make(map[interface{}]bool)
					go p.doing(gq)
				}
				timer.Reset(50 * time.Millisecond)
			}
		}
	}
}

func (p *Merge) MGet(keys []interface{}) (map[interface{}]interface{}, error) {
	successCallback := make(chan map[interface{}]interface{})
	errorCallback := make(chan error)

	defer close(successCallback)
	defer close(errorCallback)

	p.input <- Query{
		keys:            keys,
		successCallback: successCallback,
		errorCallback:   errorCallback}

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
