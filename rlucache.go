package async


import (
	"container/list"
	"errors"
	"sync"
)

type RLUCache struct {
	data map[interface{}]interface{}
	keys *list.List
	index map[interface{}]*list.Element
	queue chan command
	max uint32
	shutdown  chan bool
	desLock   sync.RWMutex
	isDestory bool	
}

func NewRLUCache(maxSize uint32) *RLUCache {
	r := &RLUCache{
		data: make(map[interface{}]interface{}),
		keys: list.New(),
		index: make(map[interface{}]*list.Element),
		queue: make(chan command, 32),
		shutdown: make(chan bool),
		isDestory: false,
		max: maxSize}
	go r.run()
	return r
}

func (r *RLUCache) Destory() {
	r.desLock.Lock()
	if r.isDestory {
		return
	}
	r.isDestory = true
	defer r.desLock.Unlock()
	r.shutdown <- true
	close(r.shutdown)
}

func (r *RLUCache) processCommand(cmd command) {
	switch cmd.action {
	case 0: //mset
		{
			for i, k := range cmd.keys {
				_, ok := r.data[k]
				if ok {
					ele, ele_ok := r.index[k]
					if ele_ok {
						r.keys.Remove(ele)
						delete(r.index, k)
					}
				}
				r.data[k] = cmd.values[i]
				r.keys.PushBack(k)
				r.index[k] = r.keys.Back()
			}
			for r.max > 0 && uint32(r.keys.Len()) > r.max {
				ele := r.keys.Front()
				k := ele.Value
				delete(r.data, k)
				delete(r.index, k)
				r.keys.Remove(ele)				
			}
		}
	case 1: //mget
		{
			result := make([]interface{}, len(cmd.keys))
			for i, k := range cmd.keys {
				v, ok := r.data[k]
				if ok {
					ele, ele_ok := r.index[k]
					if ele_ok {
						r.keys.MoveToBack(ele)
					}					
					result[i] = v
				} else {
					result[i] = errors.New("Invalid_Key")
				}
			}
			cmd.callback <- result
		}
	case 2: //mdel
		{
			for _, k := range cmd.keys {
				_, ok := r.data[k]
				if ok {
					ele, ele_ok := r.index[k]
					if ele_ok {
						r.keys.Remove(ele)
						delete(r.index, k)
					}
					delete(r.data, k)
				}
			}
		}
	case 3: //keys
		{
			result := make([]interface{}, len(r.data))
			index := 0
			for k, _ := range r.data {
				result[index] = k
				index++
			}
			cmd.callback <- result
		}
	}
}

func (r *RLUCache) run() {
	for {
		select {
		case <-r.shutdown:
			{
				goto End
			}
		case cmd := <-r.queue:
			{
				r.processCommand(cmd)
			}
		}
	}
End:
	r.data = nil
	r.index = nil
	r.keys = nil
	close(r.queue)
}

func (r *RLUCache) Set(key interface{}, value interface{}) {
	r.MSet([]interface{}{key}, []interface{}{value})
}

func (r *RLUCache) MSet(keys []interface{}, values []interface{}) {
	r.desLock.RLock()
	defer r.desLock.RUnlock()
	r.queue <- command{action: 0, keys: keys, values: values}
}

func (r *RLUCache) Get(key interface{}) (interface{}, error) {
	result := r.MGet([]interface{}{key})
	switch result[0].(type) {
	case error:
		{
			return nil, result[0].(error)
		}
	default:
		{
			return result[0], nil
		}
	}
}

func (r *RLUCache) MGet(keys []interface{}) []interface{} {
	callback := make(chan []interface{})
	defer close(callback)
	r.desLock.RLock()
	r.queue <- command{action: 1, keys: keys, callback: callback}
	r.desLock.RUnlock()
	return <-callback
}

func (r *RLUCache) Del(key interface{}) {
	r.MDel([]interface{}{key})
}

func (r *RLUCache) MDel(keys []interface{}) {
	r.desLock.RLock()
	defer r.desLock.RUnlock()
	r.queue <- command{action: 2, keys: keys}
}

func (r *RLUCache) Keys() []interface{} {
	callback := make(chan []interface{})
	defer close(callback)
	r.desLock.RLock()
	r.queue <- command{action: 3, callback: callback}
	r.desLock.RUnlock()
	return <-callback
}
