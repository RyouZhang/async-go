package async

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type operation struct {
	method   func(*KVData) (interface{}, error)
	callback chan interface{}
}

type keyInfo struct {
	key    interface{}
	expire int64
}

type KVData struct {
	pairs   map[interface{}]interface{}
	keys    *list.List
	index   map[interface{}]*list.Element
	ttl     int64 //second
	maxSize int
}

func newKVData() *KVData {
	return &KVData{
		pairs:   make(map[interface{}]interface{}),
		keys:    list.New(),
		index:   make(map[interface{}]*list.Element),
		ttl:     0,
		maxSize: 0,
	}
}

func (kv *KVData) drain() {
	if kv.maxSize == 0 {
		return
	}
	offset := kv.keys.Len() - kv.maxSize
	for offset > 0 {
		ele := kv.keys.Front()
		info := ele.Value.(*keyInfo)
		kv.keys.Remove(ele)
		delete(kv.index, info.key)
		delete(kv.pairs, info.key)
		offset = offset - 1
	}
}

func (kv *KVData) updateLRU(keys []interface{}) {
	now := time.Now().Unix() + kv.ttl
	for _, key := range keys {
		ele, ok := kv.index[key]
		if ok {
			info := ele.Value.(*keyInfo)
			info.expire = now
			kv.keys.MoveToBack(ele)
		}
	}
}

func (kv *KVData) updateTTL() {
	if kv.ttl <= 0 {
		return
	}
	now := time.Now().Unix()
	for {
		if kv.keys.Len() == 0 {
			break
		}
		ele := kv.keys.Front()
		info := ele.Value.(*keyInfo)
		if info.expire > now {
			break
		}
		delete(kv.pairs, info.key)
		delete(kv.index, info.key)
		kv.keys.Remove(ele)
	}
}

func (kv *KVData) Set(key interface{}, value interface{}) {
	defer kv.drain()
	kv.pairs[key] = value

	ele, ok := kv.index[key]
	if ok {
		kv.keys.MoveToBack(ele)
		info := ele.Value.(*keyInfo)
		info.expire = time.Now().Unix() + kv.ttl
	} else {
		info := &keyInfo{key: key, expire: time.Now().Unix() + kv.ttl}
		kv.keys.PushBack(info)
		kv.index[key] = kv.keys.Back()
	}
}

func (kv *KVData) MSet(keys []interface{}, values []interface{}) {
	defer kv.drain()
	for i, key := range keys {
		kv.pairs[key] = values[i]

		ele, ok := kv.index[key]
		if ok {
			kv.keys.MoveToBack(ele)
			info := ele.Value.(*keyInfo)
			info.expire = time.Now().Unix() + kv.ttl
		} else {
			info := &keyInfo{key: key, expire: time.Now().Unix() + kv.ttl}
			kv.keys.PushBack(info)
			kv.index[key] = kv.keys.Back()
		}
	}
}

func (kv *KVData) Merge(pairs map[interface{}]interface{}) {
	defer kv.drain()
	for key, value := range pairs {
		kv.pairs[key] = value

		ele, ok := kv.index[key]
		if ok {
			kv.keys.MoveToBack(ele)
			info := ele.Value.(*keyInfo)
			info.expire = time.Now().Unix() + kv.ttl
		} else {
			info := &keyInfo{key: key, expire: time.Now().Unix() + kv.ttl}
			kv.keys.PushBack(info)
			kv.index[key] = kv.keys.Back()
		}
	}
}

func (kv *KVData) Get(key interface{}) (interface{}, error) {
	defer kv.updateLRU([]interface{}{key})
	value, ok := kv.pairs[key]
	if ok {
		return value, nil
	} else {
		return nil, errors.New("Invalid Key")
	}
}

func (kv *KVData) MGet(keys []interface{}) []interface{} {
	defer kv.updateLRU(keys)
	values := make([]interface{}, len(keys))
	for i, key := range keys {
		value, ok := kv.pairs[key]
		if ok {
			values[i] = value
		} else {
			values[i] = errors.New("Invalid Key")
		}
	}
	return values
}

func (kv *KVData) Del(key interface{}) {
	delete(kv.pairs, key)
	ele, ok := kv.index[key]
	if ok {
		kv.keys.Remove(ele)
		delete(kv.index, key)
	}
}

func (kv *KVData) MDel(keys []interface{}) {
	for _, key := range keys {
		kv.Del(key)
	}
}

func (kv *KVData) Keys() []interface{} {
	keys := make([]interface{}, len(kv.pairs))
	index := 0
	for key, _ := range kv.pairs {
		keys[index] = key
		index = index + 1
	}
	return keys
}

func (kv *KVData) Clean() {
	for {		
		ele := kv.keys.Front()
		if ele == nil {
			return
		}		
		info := ele.Value.(*keyInfo)
		kv.keys.Remove(ele)
		delete(kv.index, info.key)
		delete(kv.pairs, info.key)
	}
}


type KVCache struct {
	data      *KVData
	queue     chan operation
	shutdown  chan bool
	desLock   sync.RWMutex
	isDestory bool
}

func NewKVCache() *KVCache {
	kv := &KVCache{
		data:      newKVData(),
		queue:     make(chan operation, 16),
		shutdown:  make(chan bool),
		isDestory: false,
	}
	go kv.runloop()
	return kv
}

func (kv *KVCache) TTL(ttl int64) *KVCache {
	kv.data.ttl = ttl
	return kv
}

func (kv *KVCache) MaxSize(maxSize int) *KVCache {
	kv.data.maxSize = maxSize
	return kv
}

func (kv *KVCache) runloop() {
	timer := time.NewTimer(1 * time.Second)
	for {
		select {
		case <-kv.shutdown:
			{
				timer.Stop()
				goto End
			}
		case <-timer.C:
			{
				if kv.data.ttl > 0 {
					kv.data.updateTTL()
				}
				timer.Reset(1 * time.Second)
			}
		case op := <-kv.queue:
			{
				func() {
					defer func() {
						if e := recover(); e != nil {
							op.callback <- e.(error)
						}
					}()
					res, err := op.method(kv.data)
					if err != nil {
						op.callback <- err
					} else {
						op.callback <- res
					}
				}()
			}
		}
	}
End:
	close(kv.queue)
}

func (kv *KVCache) Commit(method func(*KVData) (interface{}, error)) (interface{}, error) {
	kv.desLock.RLock()
	if kv.isDestory {
		kv.desLock.RUnlock()
		return nil, errors.New("Invalid KVCache")
	}
	output := make(chan interface{})
	defer close(output)
	kv.queue <- operation{method: method, callback: output}
	kv.desLock.RUnlock()

	select {
	case res := <-output:
		{
			switch res.(type) {
			case error:
				{
					return nil, res.(error)
				}
			default:
				{
					return res, nil
				}
			}
		}
	}
}

func (kv *KVCache) Destory() {
	kv.desLock.Lock()
	defer kv.desLock.Unlock()
	if kv.isDestory {
		return
	}
	kv.isDestory = true
	kv.shutdown <- true
	close(kv.shutdown)
}
