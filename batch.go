package async

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	input    chan *cmd
	output   chan *batchCmd
	groupDic map[string]*group
	gLocker  sync.RWMutex
)

type CacheProvider interface {
	Put(string, interface{}, interface{}) error
	Get(string, interface{}) (interface{}, error)
}

func init() {
	groupDic = make(map[string]*group)
	input = make(chan *cmd, 128)
	output = make(chan *batchCmd, 128)
	go runloop()
}

type group struct {
	name      string
	batchSize int
	method    func(...interface{}) (map[interface{}]interface{}, error)
	cmdDic    map[interface{}][]*cmd
	keyDic    map[interface{}]bool
	cache     CacheProvider
}

type cmd struct {
	group    string
	key      interface{}
	callback chan interface{}
}

type batchCmd struct {
	group  string
	keys   []interface{}
	err    error
	result map[interface{}]interface{}
}

func RegisterGroup(
	name string,
	batchSize int,
	method func(...interface{}) (map[interface{}]interface{}, error),
	cache CacheProvider) error {
	gLocker.Lock()
	defer gLocker.Unlock()
	_, ok := groupDic[name]
	if ok {
		return fmt.Errorf("duplicate group:%s", name)
	}
	groupDic[name] = &group{
		name:      name,
		batchSize: batchSize,
		method:    method,
		cmdDic:    make(map[interface{}][]*cmd),
		keyDic:    make(map[interface{}]bool),
		cache:     cache,
	}
	return nil
}

func doing(ctx context.Context, b *batchCmd, method func(...interface{}) (map[interface{}]interface{}, error)) {
	res, err := Safety(func() (interface{}, error) {
		return method(b.keys...)
	})
	if err != nil {
		b.err = err
	} else {
		b.result = res.(map[interface{}]interface{})
	}
	output <- b
}

func runloop() {
	ctx := context.Background()
	timer := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case c := <-input:
			{
				gLocker.RLocker()
				defer gLocker.RUnlock()
				g, ok := groupDic[c.group]
				if false == ok {
					c.callback <- fmt.Errorf("invalid group:%s", c.group)
					close(c.callback)
					continue
				}
				// cache provider
				if g.cache != nil {
					res, err := g.cache.Get(g.name, c.key)
					if res != nil && err == nil {
						c.callback <- res
						close(c.callback)
						continue
					}
				}
				target, ok := g.cmdDic[c.key]
				if ok {
					g.cmdDic[c.key] = append(target, c)
				} else {
					g.cmdDic[c.key] = []*cmd{c}
					g.keyDic[c.key] = true
				}

				if len(g.keyDic) >= g.batchSize {
					b := &batchCmd{group: g.name}
					b.keys = make([]interface{}, len(g.keyDic))
					index := 0
					for k, _ := range g.keyDic {
						b.keys[index] = k
						index = index + 1
					}
					g.keyDic = nil
					g.keyDic = make(map[interface{}]bool)
					go doing(ctx, b, g.method)
				}
			}
		case b := <-output:
			{
				gLocker.RLocker()
				defer gLocker.RUnlock()
				g, _ := groupDic[b.group]
				if b.err != nil {
					for _, key := range b.keys {
						target, ok := g.cmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.callback <- b.err
								close(c.callback)
							}
						}
						delete(g.cmdDic, key)
					}
				} else {
					for _, key := range b.keys {
						res, ok := b.result[key]
						if false == ok {
							res = fmt.Errorf("invalid key:%s", key)
						} else {
							// cache provider
							if g.cache != nil {
								g.cache.Put(g.name, key, res)
							}
						}
						target, ok := g.cmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.callback <- res
								close(c.callback)
							}
						}
						delete(g.cmdDic, key)
					}
				}
			}
		case <-timer.C:
			{
				gLocker.RLocker()
				defer gLocker.RUnlock()
				for _, g := range groupDic {
					if len(g.keyDic) > 0 {
						b := &batchCmd{group: g.name}
						b.keys = make([]interface{}, len(g.keyDic))
						index := 0
						for k, _ := range g.keyDic {
							b.keys[index] = k
							index = index + 1
						}
						g.keyDic = make(map[interface{}]bool)
						go doing(ctx, b, g.method)
					}
				}
				timer.Reset(10 * time.Millisecond)
			}
		}
	}
}

func Get(group string, key interface{}) (interface{}, error) {
	c := &cmd{
		group:    group,
		key:      key,
		callback: make(chan interface{}, 1),
	}
	input <- c
	res := <-c.callback
	switch res.(type) {
	case error:
		return nil, res.(error)
	default:
		return res, nil
	}
}
