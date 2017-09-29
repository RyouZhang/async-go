package async

import (
	"sync"
	"errors"
)

type safeMapAction struct {
	action   int8 //0 get, 1 put, 2 del
	keys     []string
	values   []interface{}
	callback chan map[string]interface{}
}

type SafeMap struct {
	data      map[string]interface{}
	input     chan safeMapAction
	desLock   sync.RWMutex
	isDestory bool
}

func NewSafeMap() *SafeMap {
	sm := &SafeMap{
		data:      make(map[string]interface{}),
		input:     make(chan safeMapAction, 16),
		isDestory: false}
	go sm.run()
	return sm
}

func (sm *SafeMap) run() {
	for msg := range sm.input {
		switch msg.action {
		case 0:
			{
				res := make(map[string]interface{})
				for _, key := range msg.keys {
					value, ok := sm.data[key]
					if ok {
						res[key] = value
					}
				}
				msg.callback <- res
				break
			}
		case 1:
			{
				for i, key := range msg.keys {
					value := msg.values[i]
					sm.data[key] = value
				}
				break
			}
		case 2:
			{
				for _, key := range msg.keys {
					delete(sm.data, key)
				}
				break
			}
		}
	}
}

func (sm *SafeMap) Get(key string) (interface{}, error) {
	res := sm.MGet([]string{key})
	if res == nil {
		return nil, errors.New("SafeMap is destory")
	}
	value, ok := res[key]
	if ok {
		return value, nil
	} else {
		return nil, errors.New("Invalid Key")
	}
}

func (sm *SafeMap) MGet(keys []string) map[string]interface{} {
	sm.desLock.RLock()
	if sm.isDestory == false {
		callback := make(chan map[string]interface{}, 1)
		sm.input <- safeMapAction{
			action:   0,
			keys:     keys,
			callback: callback}
		sm.desLock.RUnlock()

		defer close(callback)
		select {
		case res := <-callback:
			{
				return res
			}
		}
	} else {
		sm.desLock.RUnlock()
		return nil
	}
}

func (sm *SafeMap) Set(key string, value interface{}) {
	sm.MSet([]string{key}, []interface{}{value})
}

func (sm *SafeMap) MSet(keys []string, values []interface{}) {
	sm.desLock.RLock()
	defer sm.desLock.RUnlock()
	if sm.isDestory == false {
		sm.input <- safeMapAction{
			action: 1,
			keys:   keys,
			values: values}
	}
}

func (sm *SafeMap) Del(key string) {
	sm.Mdel([]string{key})
}

func (sm *SafeMap) Mdel(keys []string) {
	sm.desLock.RLock()
	defer sm.desLock.RUnlock()
	if sm.isDestory == false {
		sm.input <- safeMapAction{
			action: 2,
			keys:   keys}
	}
}

func (sm *SafeMap) Close() {
	sm.desLock.Lock()
	defer sm.desLock.Unlock()
	if sm.isDestory == false {
		sm.isDestory = true
		close(sm.input)
		sm.data = nil
	}
}
