package task

import (
	"context"
	"time"

	"github.com/RyouZhang/async-go"
)

type taskGroup struct {
	name      string
	batchSize int
	maxWorker int

	workerCount int

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
		maxWorker:    maxWorker,
		workerCount:  0,
		requestQueue: make(chan *request, 256),
		resultQueue:  make(chan *result, 256),
		taskToReq:    make(map[string]*request),
		mergeTaskDic: make(map[string][]Task),
		groupTaskDic: make(map[string][]Task),
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

					tg.taskToReq[t.UniqueId()] = req

					_, ok := t.(MergeTask)
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

					_, ok = t.(GroupTask)
					if ok {
						gkey := t.(GroupTask).GroupBy()
						if len(gkey) > 0 {
							target, ok := tg.groupTaskDic[gkey]
							if ok {
								tg.groupTaskDic[gkey] = append(target, t)
								continue
							} else {
								tg.groupTaskDic[gkey] = []Task{t}
							}
						} else {
							tg.tasks = append(tg.tasks, t)
						}
					} else {
						tg.tasks = append(tg.tasks, t)
					}

					if tg.workerCount < tg.maxWorker {
						tg.schedule(ctx)
					}
				}
			}
		case res := <-tg.resultQueue:
			{
				// cache
				tg.putToCache(res)

				//check merge
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
							}
						}
					}
					delete(tg.mergeTaskDic, res.mkey)
				} else {
					req, ok := tg.taskToReq[res.key]
					if ok {
						req.callback <- res
						req.count--
						if req.count == 0 {
							close(req.callback)
						}
					}
				}
				tg.schedule(ctx)
			}
		case <-timer.C:
			{
				if tg.workerCount < tg.maxWorker {
					tg.timerSchedule(ctx)
				}
				timer.Reset(10 * time.Millisecond)
			}
		}
	}
}

func (tg *taskGroup) schedule(ctx context.Context) {

	if len(tg.tasks) > tg.batchSize {
		tg.running(ctx, tg.tasks)
		tg.tasks = []Task{}
	}

	delKeys := make([]string, 0)

	for gkey, _ := range tg.groupTaskDic {
		if tg.workerCount == tg.maxWorker {
			goto CLEAN
		}

		tasks := tg.groupTaskDic[gkey]
		if len(tasks) > tg.batchSize {
			tg.running(ctx, tasks)
			delKeys = append(delKeys, gkey)
		}
	}
CLEAN:
	for i, _ := range delKeys {
		gkey := delKeys[i]
		delete(tg.groupTaskDic, gkey)
	}
}

func (tg *taskGroup) timerSchedule(ctx context.Context) {

	if len(tg.tasks) > 0 {
		t := tg.tasks[0]
		tg.running(ctx, []Task{t})
		if len(tg.tasks) == 1{
			tg.tasks = []Task{}
		} else {
			tg.tasks = tg.tasks[1:]
		}		
	}

	delKeys := make([]string, 0)

	for gkey, _ := range tg.groupTaskDic {
		if tg.workerCount == tg.maxWorker {
			goto CLEAN
		}

		tasks := tg.groupTaskDic[gkey]
		if len(tasks) > 0 {
			tg.running(ctx, tasks)
			delKeys = append(delKeys, gkey)
		}
	}
CLEAN:
	for i, _ := range delKeys {
		gkey := delKeys[i]
		delete(tg.groupTaskDic, gkey)
	}
}

func (tg *taskGroup) running(ctx context.Context, tasks []Task) {
	tg.workerCount++
	go func() {
		defer func() {
			tg.workerCount--
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
