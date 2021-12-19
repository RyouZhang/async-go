package async

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	one      sync.Once
	input    chan *cmd
	output   chan *batchCmd
	groupDic map[string]*group
)

type CacheProvider interface {
	Put(string, interface{}, interface{}) error
	Get(string, interface{}) (interface{}, error)
}

func init() {
	groupDic = make(map[string]*group)
	input = make(chan *cmd, 128)
	output = make(chan *batchCmd, 128)
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
	count    int
	callback chan interface{}
	forced   bool
}

func (c *cmd) output(res interface{}) {
	c.callback <- res
	c.count--
	if c.count == 0 {
		close(c.callback)
	}
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
	one.Do(func() {
		go runloop()
	})
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
	timer := time.NewTimer(10 * time.Millisecond)
	for {
		select {
		case c := <-input:
			{
				g, ok := groupDic[c.group]
				if false == ok {
					c.output(fmt.Errorf("invalid found:%s", c.group))
					if c.count > 0 {
						close(c.callback)
					}
					continue
				}
				var keys []interface{}
				if c.count > 1 {
					keys = c.key.([]interface{})
				} else {
					keys = []interface{}{c.key}
				}

				for index, _ := range keys {
					key := keys[index]
					// cache provider
					if g.cache != nil && c.forced == false {
						res, err := g.cache.Get(g.name, key)
						if res != nil && err == nil {
							c.output(res)
							continue
						}
					}
					target, ok := g.cmdDic[key]
					if ok {
						g.cmdDic[key] = append(target, c)
					} else {
						g.cmdDic[key] = []*cmd{c}
						g.keyDic[key] = true
					}

					if len(g.keyDic) >= g.batchSize {
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
			}
		case b := <-output:
			{
				g, _ := groupDic[b.group]
				if b.err != nil {
					for _, key := range b.keys {
						target, ok := g.cmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.output(b.err)
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
							if g.cache != nil && res != nil {
								g.cache.Put(g.name, key, res)
							}
						}
						target, ok := g.cmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.output(res)
							}
						}
						delete(g.cmdDic, key)
					}
				}
			}
		case <-timer.C:
			{
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
	return exec(group, false, key)
}

func ForceGet(group string, key interface{}) (interface{}, error) {
	return exec(group, true, key)
}

func Exec(group string, req interface{}) (interface{}, error) {
	return exec(group, true, req)
}

func exec(group string, forced bool, key interface{}) (interface{}, error) {
	c := &cmd{
		group:    group,
		key:      key,
		count:    1,
		callback: make(chan interface{}, 1),
		forced:   forced,
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

func MGet(group string, keys ...interface{}) []interface{} {
	return mexec(group, false, keys...)
}

func ForceMGet(group string, keys ...interface{}) []interface{} {
	return mexec(group, true, keys...)
}

func MExec(group string, reqs ...interface{}) (interface{}, error) {
	return mexec(group, true, reqs...)
}

func mexec(group string, forced bool, keys ...interface{}) []interface{} {
	c := &cmd{
		group:    group,
		key:      keys,
		count:    len(keys),
		callback: make(chan interface{}, len(keys)),
		forced:   forced,
	}
	input <- c
	results := make([]interface{}, 0)
	for result := range c.callback {
		results = append(results, result)
	}
	return results
}
