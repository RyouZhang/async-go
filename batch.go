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
	Put(string, any, any) error
	Get(string, any) (any, error)
}

func init() {
	groupDic = make(map[string]*group)
	input = make(chan *cmd, 128)
	output = make(chan *batchCmd, 128)
}

type group struct {
	name      string
	batchSize int
	method    func(...any) (map[any]any, error)
	cmdDic    map[any][]*cmd
	keyDic    map[any]bool
	cache     CacheProvider
}

type cmd struct {
	group    string
	key      any
	count    int
	callback chan any
	forced   bool
}

func (c *cmd) output(res any) {
	c.callback <- res
	c.count--
	if c.count == 0 {
		close(c.callback)
	}
}

type batchCmd struct {
	group  string
	keys   []any
	err    error
	result map[any]any
}

func RegisterGroup(
	name string,
	batchSize int,
	method func(...any) (map[any]any, error),
	cache CacheProvider) error {

	_, ok := groupDic[name]
	if ok {
		return fmt.Errorf("duplicate group:%s", name)
	}

	groupDic[name] = &group{
		name:      name,
		batchSize: batchSize,
		method:    method,
		cmdDic:    make(map[any][]*cmd),
		keyDic:    make(map[any]bool),
		cache:     cache,
	}
	one.Do(func() {
		go runloop()
	})
	return nil
}

func doing(ctx context.Context, b *batchCmd, method func(...any) (map[any]any, error)) {
	res, err := Safety(func() (any, error) {
		return method(b.keys...)
	})
	if err != nil {
		b.err = err
	} else {
		b.result = res.(map[any]any)
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
				var keys []any
				if c.count > 1 {
					keys = c.key.([]any)
				} else {
					keys = []any{c.key}
				}

				for index := range keys {
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
						b.keys = make([]any, len(g.keyDic))
						index := 0
						for k := range g.keyDic {
							b.keys[index] = k
							index = index + 1
						}
						g.keyDic = make(map[any]bool)
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
							for index := range target {
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
							for index := range target {
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
						b.keys = make([]any, len(g.keyDic))
						index := 0
						for k := range g.keyDic {
							b.keys[index] = k
							index = index + 1
						}
						g.keyDic = make(map[any]bool)
						go doing(ctx, b, g.method)
					}
				}
				timer.Reset(10 * time.Millisecond)
			}
		}
	}
}

func Get(group string, key any) (any, error) {
	return get(group, false, key)
}

func ForceGet(group string, key any) (any, error) {
	return get(group, true, key)
}

func get(group string, forced bool, key any) (any, error) {
	c := &cmd{
		group:    group,
		key:      key,
		count:    1,
		callback: make(chan any, 1),
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

func MGet(group string, keys ...any) []any {
	return mget(group, false, keys...)
}

func ForceMGet(group string, keys ...any) []any {
	return mget(group, true, keys...)
}

func mget(group string, forced bool, keys ...any) []any {
	c := &cmd{
		group:    group,
		key:      keys,
		count:    len(keys),
		callback: make(chan any, len(keys)),
		forced:   forced,
	}
	input <- c
	results := make([]any, 0)
	for result := range c.callback {
		results = append(results, result)
	}
	return results
}
