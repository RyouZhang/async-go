package task

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/RyouZhang/async-go"
)

type taskGroup struct {
	name      string
	batchSize int
	maxWorker int32

	workerCount int32

	requestQueue chan *request
	resultQueue  chan *result

	taskToReq    map[string]*request
	mergeTaskDic map[string][]Task
	groupTaskDic map[string][]Task

	tasks []Task

	method func(...Task) (map[string]interface{}, error)
	cp     TaskCacheProvider
}

func newTaskGroup(name string, batchSize int, maxWorker int, method func(...Task) (map[string]interface{}, error), cp TaskCacheProvider) *taskGroup {
	tg := &taskGroup{
		name:         name,
		batchSize:    batchSize,
		maxWorker:    int32(maxWorker),
		workerCount:  int32(0),
		requestQueue: make(chan *request, 128),
		resultQueue:  make(chan *result, 128),
		taskToReq:    make(map[string]*request),
		mergeTaskDic: make(map[string][]Task),
		groupTaskDic: make(map[string][]Task),
		tasks:        make([]Task, 0),
		method:       method,
		cp:           cp,
	}

	go tg.runloop()

	return tg
}

func (tg *taskGroup) getFromCache(t Task) interface{} {
	if tg.cp != nil {
		val, _ := tg.cp.Get(t.UniqueId())
		if val != nil {
			return val
		}
		_, ok := t.(MergeTask)
		if ok {
			val, _ = tg.cp.Get(t.(MergeTask).MergeBy())
			return val
		}
	}
	return nil
}

func (tg *taskGroup) putToCache(res *result) {
	if res.err != nil {
		return
	}

	if tg.cp != nil {
		if len(res.mkey) > 0 {
			tg.cp.Put(res.mkey, res.val)
		} else {
			tg.cp.Put(res.key, res.val)
		}
	}
}

func (tg *taskGroup) runloop() {
	ctx := context.Background()
	timer := time.NewTimer(10 * time.Millisecond)
	for {
		select {
		case req := <-tg.requestQueue:
			{
				for i, _ := range req.tasks {
					t := req.tasks[i]

					// cache
					val := tg.getFromCache(t)
					if val != nil {
						req.callback <- &result{key: t.UniqueId(), val: val}
						req.count--
						if req.count == 0 {
							close(req.callback)
						}
						continue
					}
					_, ok := tg.taskToReq[t.UniqueId()]
					if !ok {
						tg.taskToReq[t.UniqueId()] = req
					} else {
						req.callback <- &result{key: t.UniqueId(), err: fmt.Errorf("duplicate unique id %s", t.UniqueId())}
						req.count--
						if req.count == 0 {
							close(req.callback)
						}
						continue
					}

					// merge
					_, ok = t.(MergeTask)
					if ok {
						mkey := t.(MergeTask).MergeBy()
						if len(mkey) > 0 {
							target, ok := tg.mergeTaskDic[mkey]
							if ok {
								tg.mergeTaskDic[mkey] = append(target, t)
								continue
							} else {
								tg.mergeTaskDic[mkey] = []Task{t}
							}
						}
					}
					// group
					_, ok = t.(GroupTask)
					if ok {
						gkey := t.(GroupTask).GroupBy()
						if len(gkey) > 0 {
							target, ok := tg.groupTaskDic[gkey]
							if ok {
								tg.groupTaskDic[gkey] = append(target, t)
							} else {
								tg.groupTaskDic[gkey] = []Task{t}
							}
							continue
						}
					}
					// default
					tg.tasks = append(tg.tasks, t)
				}
				if atomic.LoadInt32(&tg.workerCount) < tg.maxWorker {
					tg.schedule(ctx)
				}
			}
		case res := <-tg.resultQueue:
			{
				// cache
				tg.putToCache(res)
				//check merge
				if len(res.mkey) > 0 {
					target, ok := tg.mergeTaskDic[res.mkey]
					if ok {
						for i, _ := range target {
							t := target[i]
							req, ok := tg.taskToReq[t.UniqueId()]
							if ok {
								req.callback <- &result{
									key: t.UniqueId(),
									val: res.val,
									err: res.err,
								}
								req.count--
								if req.count == 0 {
									close(req.callback)
									delete(tg.taskToReq, t.UniqueId())
								}
							}
						}
						delete(tg.mergeTaskDic, res.mkey)
					}
				}
				req, ok := tg.taskToReq[res.key]
				if ok {
					req.callback <- res
					req.count--
					if req.count == 0 {
						close(req.callback)
					}
					delete(tg.taskToReq, res.key)
				}
				if atomic.LoadInt32(&tg.workerCount) < tg.maxWorker {
					tg.schedule(ctx)
				}
			}
		case <-timer.C:
			{
				if atomic.LoadInt32(&tg.workerCount) < tg.maxWorker {
					tg.timerSchedule(ctx)
				}
				timer.Reset(10 * time.Millisecond)
			}
		}
	}
}

func (tg *taskGroup) schedule(ctx context.Context) {
	delKeys := make([]string, 0)
	index := -1

	for gkey, _ := range tg.groupTaskDic {
		if atomic.LoadInt32(&tg.workerCount) == tg.maxWorker {
			goto CLEAN
		}

		tasks := tg.groupTaskDic[gkey]
		if len(tasks) >= tg.batchSize {
			tg.processing(ctx, tasks)
			delKeys = append(delKeys, gkey)
		}
	}

	for i, _ := range tg.tasks {
		if atomic.LoadInt32(&tg.workerCount) == tg.maxWorker {
			goto CLEAN
		}

		t := tg.tasks[i]
		tg.processing(ctx, []Task{t})
		index = i
	}
CLEAN:
	if index == len(tg.tasks)-1 {
		tg.tasks = []Task{}
	} else {
		tg.tasks = tg.tasks[index+1:]
	}

	for i, _ := range delKeys {
		gkey := delKeys[i]
		delete(tg.groupTaskDic, gkey)
	}
}

func (tg *taskGroup) timerSchedule(ctx context.Context) {
	delKeys := make([]string, 0)
	index := -1

	for gkey, _ := range tg.groupTaskDic {
		if atomic.LoadInt32(&tg.workerCount) == tg.maxWorker {
			goto CLEAN
		}

		tasks := tg.groupTaskDic[gkey]
		if len(tasks) > 0 {
			tg.processing(ctx, tasks)
			delKeys = append(delKeys, gkey)
		}
	}

	for i, _ := range tg.tasks {
		if atomic.LoadInt32(&tg.workerCount) == tg.maxWorker {
			goto CLEAN
		}

		t := tg.tasks[i]
		tg.processing(ctx, []Task{t})
		index = i
	}
CLEAN:
	if index == len(tg.tasks)-1 {
		tg.tasks = []Task{}
	} else {
		tg.tasks = tg.tasks[index+1:]
	}

	for i, _ := range delKeys {
		gkey := delKeys[i]
		delete(tg.groupTaskDic, gkey)
	}
}

func (tg *taskGroup) processing(ctx context.Context, tasks []Task) {
	tasklist := tasks
	for {
		if len(tasklist) <= tg.batchSize {
			tg.running(ctx, tasklist)
			return
		}
		target := tasklist[:tg.batchSize]
		tasklist = tasklist[tg.batchSize:]
		tg.running(ctx, target)
	}
}

func (tg *taskGroup) running(ctx context.Context, tasks []Task) {
	atomic.AddInt32(&tg.workerCount, 1)
	go func() {
		defer func() {
			atomic.AddInt32(&tg.workerCount, -1)
		}()

		_, err := async.Safety(func() (interface{}, error) {

			res, err := tg.method(tasks...)
			if err != nil {
				return nil, err
			}

			for i, _ := range tasks {
				t := tasks[i]

				mkey := ""
				_, ok := t.(MergeTask)
				if ok {
					mkey = t.(MergeTask).MergeBy()
				}

				val, ok := res[t.UniqueId()]
				if ok {
					tg.resultQueue <- &result{
						key:  t.UniqueId(),
						mkey: mkey,
						val:  val,
					}
				} else {
					tg.resultQueue <- &result{
						key:  t.UniqueId(),
						mkey: mkey,
						val:  nil,
					}
				}
			}
			return nil, nil
		})
		if err == nil {
			return
		}

		for i, _ := range tasks {
			t := tasks[i]

			mkey := ""
			_, ok := t.(MergeTask)
			if ok {
				mkey = t.(MergeTask).MergeBy()
			}

			tg.resultQueue <- &result{
				key:  t.UniqueId(),
				mkey: mkey,
				err:  err,
			}
		}
	}()
}
