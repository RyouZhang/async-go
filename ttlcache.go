package async

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type command struct {
	action   int8
	keys     []interface{}
	values   []interface{}
	callback chan []interface{}
}

type item struct {
	key    interface{}
	expire int64
}

type TTLCache struct {
	data      map[interface{}]interface{}
	ttl       *list.List
	index     map[interface{}]*list.Element
	expire    time.Duration
	queue     chan command
	shutdown  chan bool
	desLock   sync.RWMutex
	isDestory bool
}

func NewTTLCache(expire time.Duration) *TTLCache {
	c := &TTLCache{
		queue:     make(chan command, 32),
		data:      make(map[interface{}]interface{}),
		index:     make(map[interface{}]*list.Element),
		shutdown:  make(chan bool),
		ttl:       list.New(),
		expire:    expire,
		isDestory: false}
	go c.run()
	return c
}

func (c *TTLCache) Destory() {
	c.desLock.Lock()
	if c.isDestory {
		return
	}
	c.isDestory = true
	defer c.desLock.Unlock()
	c.shutdown <- true
	close(c.shutdown)
}

func (c *TTLCache) processCommand(cmd command) {
	now := time.Now()
	switch cmd.action {
	case 0: //mset
		{
			expire := now.Add(c.expire).UnixNano()
			for i, k := range cmd.keys {
				_, ok := c.data[k]
				if ok {
					ele, ele_ok := c.index[k]
					if ele_ok {
						c.ttl.Remove(ele)
						delete(c.index, k)
					}
				}
				c.data[k] = cmd.values[i]
				c.ttl.PushBack(item{key: k, expire: expire})
				c.index[k] = c.ttl.Back()
			}
		}
	case 1: //mget
		{
			result := make([]interface{}, len(cmd.keys))
			for i, k := range cmd.keys {
				v, ok := c.data[k]
				if ok {
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
				_, ok := c.data[k]
				if ok {
					ele, ele_ok := c.index[k]
					if ele_ok {
						c.ttl.Remove(ele)
						delete(c.index, k)
					}
					delete(c.data, k)
				}
			}
		}
	case 3: //keys
		{
			result := make([]interface{}, len(c.data))
			index := 0
			for k, _ := range c.data {
				result[index] = k
				index++
			}
			cmd.callback <- result
		}
	case 4: //ttl
		{
			for {
				if c.ttl.Len() == 0 {
					break
				}
				ele := c.ttl.Front()
				tn := ele.Value.(item)
				if tn.expire > now.UnixNano() {
					break
				}
				delete(c.data, tn.key)
				delete(c.index, tn.key)
				c.ttl.Remove(ele)

			}
		}
	}
}

func (c *TTLCache) run() {
	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-c.shutdown:
			{
				timer.Stop()
				goto End
			}
		case <-timer.C:
			{
				c.processCommand(command{action: 4})
				timer.Reset(10 * time.Second)
			}
		case cmd := <-c.queue:
			{
				c.processCommand(cmd)
			}
		}
	}
End:
	c.data = nil
	c.ttl = nil
	c.index = nil
	close(c.queue)
}

func (c *TTLCache) Set(key interface{}, value interface{}) {
	c.MSet([]interface{}{key}, []interface{}{value})
}

func (c *TTLCache) MSet(keys []interface{}, values []interface{}) {
	c.desLock.RLock()
	defer c.desLock.RUnlock()
	c.queue <- command{action: 0, keys: keys, values: values}
}

func (c *TTLCache) Get(key interface{}) (interface{}, error) {
	result := c.MGet([]interface{}{key})
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

func (c *TTLCache) MGet(keys []interface{}) []interface{} {
	callback := make(chan []interface{})
	defer close(callback)
	c.desLock.RLock()
	c.queue <- command{action: 1, keys: keys, callback: callback}
	c.desLock.RUnlock()
	return <-callback
}

func (c *TTLCache) Del(key interface{}) {
	c.MDel([]interface{}{key})
}

func (c *TTLCache) MDel(keys []interface{}) {
	c.desLock.RLock()
	defer c.desLock.RUnlock()
	c.queue <- command{action: 2, keys: keys}
}

func (c *TTLCache) Keys() []interface{} {
	callback := make(chan []interface{})
	defer close(callback)
	c.desLock.RLock()
	c.queue <- command{action: 3, callback: callback}
	c.desLock.RUnlock()
	return <-callback
}
