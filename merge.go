package async

import (
	"errors"
	"sync"
	"sync/atomic"
)

type request struct {
	key      string
	method   func() (any, error)
	callback chan *reply
}

type reply struct {
	key    string
	result any
	err    error
}

const mergeShardMask = 15 // 16 个分片，必须是 2^n - 1

type mergeShard struct {
	callbackDic map[string][]chan *reply
	inputQueue  chan *request
	outputQueue chan *reply
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

type Merge struct {
	shards      [mergeShardMask + 1]*mergeShard
	isDestroyed atomic.Bool
	destroyOnce sync.Once
}

func NewMerge() *Merge {
	m := &Merge{}
	for i := range m.shards {
		s := &mergeShard{
			callbackDic: make(map[string][]chan *reply),
			inputQueue:  make(chan *request, 128),
			outputQueue: make(chan *reply, 128),
			shutdown:    make(chan struct{}),
		}
		go s.runloop()
		m.shards[i] = s
	}
	return m
}

// shardIndex 用 FNV-1a 把 key 映射到分片，保证同 key 落同 shard（去重语义不变）。
func shardIndex(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h & mergeShardMask
}

func (s *mergeShard) runloop() {
	for {
		select {
		case <-s.shutdown:
			return
		case rep := <-s.outputQueue:
			{
				target, ok := s.callbackDic[rep.key]
				if ok {
					delete(s.callbackDic, rep.key)
					// 分发挪出 runloop：单个 hot key 的海量等待者派发
					// 不再阻塞同 shard 其它 key 的受理。
					go func(cbs []chan *reply, r *reply) {
						for _, callback := range cbs {
							callback <- r
						}
					}(target, rep)
				}
			}
		case req := <-s.inputQueue:
			{
				target, ok := s.callbackDic[req.key]
				if ok {
					s.callbackDic[req.key] = append(target, req.callback)
				} else {
					target = make([]chan *reply, 1)
					target[0] = req.callback
					s.callbackDic[req.key] = target

					go func(key string, method func() (any, error)) {
						res, err := Safety(method)
						s.outputQueue <- &reply{key: key, result: res, err: err}
					}(req.key, req.method)
				}
			}
		}
	}
}

func (m *Merge) Destroy() {
	m.destroyOnce.Do(func() {
		m.isDestroyed.Store(true)
		for _, s := range m.shards {
			s.wg.Wait()
			close(s.shutdown)
			close(s.inputQueue)
			close(s.outputQueue)
		}
	})
}

func (m *Merge) Exec(key string, method func() (any, error)) (any, error) {
	if m.isDestroyed.Load() {
		return nil, errors.New("Merge Destroyed")
	}

	s := m.shards[shardIndex(key)]
	s.wg.Add(1)
	defer s.wg.Done()

	callback := make(chan *reply, 1)
	s.inputQueue <- &request{key: key, method: method, callback: callback}

	res := <-callback
	close(callback)
	return res.result, res.err
}
