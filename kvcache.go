package async

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type operation struct {
	method   func(*KVData) (any, error)
	callback chan any
}

type keyInfo struct {
	key    any
	expire int64
}

type KVData struct {
	pairs   map[any]any
	keys    *list.List
	index   map[any]*list.Element
	lru     bool
	ttl     int64 //second
	maxSize int
}

func newKVData() *KVData {
	return &KVData{
		pairs:   make(map[any]any),
		keys:    list.New(),
		index:   make(map[any]*list.Element),
		lru:     false,
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

func (kv *KVData) updateLRU(keys []any) {
	if false == kv.lru {
		return
	}
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

func (kv *KVData) Set(key any, value any) {
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

func (kv *KVData) MSet(keys []any, values []any) {
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

func (kv *KVData) Merge(pairs map[any]any) {
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

func (kv *KVData) Get(key any) (any, error) {
	defer kv.updateLRU([]any{key})
	value, ok := kv.pairs[key]
	if ok {
		return value, nil
	} else {
		return nil, errors.New("Invalid Key")
	}
}

func (kv *KVData) MGet(keys []any) []any {
	defer kv.updateLRU(keys)
	values := make([]any, len(keys))
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

func (kv *KVData) Del(key any) {
	delete(kv.pairs, key)
	ele, ok := kv.index[key]
	if ok {
		kv.keys.Remove(ele)
		delete(kv.index, key)
	}
}

func (kv *KVData) MDel(keys []any) {
	for _, key := range keys {
		kv.Del(key)
	}
}

func (kv *KVData) Keys() []any {
	keys := make([]any, len(kv.pairs))
	index := 0
	for key := range kv.pairs {
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
	shutdown  chan struct{}
	desLock   sync.RWMutex
	isDestory bool
}

func NewKVCache() *KVCache {
	kv := &KVCache{
		data:      newKVData(),
		queue:     make(chan operation, 16),
		shutdown:  make(chan struct{}),
		isDestory: false,
	}
	go kv.runloop()
	return kv
}

func (kv *KVCache) TTL(ttl int64) *KVCache {
	kv.data.ttl = ttl
	return kv
}

func (kv *KVCache) LRU(flag bool) *KVCache {
	kv.data.lru = flag
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

func (kv *KVCache) Commit(method func(*KVData) (any, error)) (any, error) {
	kv.desLock.RLock()
	if kv.isDestory {
		kv.desLock.RUnlock()
		return nil, errors.New("Invalid KVCache")
	}
	output := make(chan any, 1)
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
	close(kv.shutdown)
}
