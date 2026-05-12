package task

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RyouZhang/async-go"
)

type taskGroup struct {
	name      string
	batchSize int
	timeRange int

	requestQueue chan *request
	resultQueue  chan *result
	wg           sync.WaitGroup
	shutdown     chan struct{}

	workerCount atomic.Int64
	maxWorker   int64

	taskToReq    map[string]*request
	mergeTaskDic map[string][]Task
	groupTaskDic map[string][]Task

	tasks []Task

	method func(...Task) (map[string]any, error)
}

func newTaskGroup(name string, batchSize int, maxWorker int, timeRange int, method func(...Task) (map[string]any, error)) *taskGroup {
	tg := &taskGroup{
		name:         name,
		batchSize:    batchSize,
		timeRange:    timeRange,
		requestQueue: make(chan *request, 128),
		resultQueue:  make(chan *result, 128),
		shutdown:     make(chan struct{}),
		maxWorker:    int64(maxWorker),
		taskToReq:    make(map[string]*request),
		mergeTaskDic: make(map[string][]Task),
		groupTaskDic: make(map[string][]Task),
		tasks:        make([]Task, 0),
		method:       method,
	}

	go tg.runloop()

	return tg
}

func (tg *taskGroup) runloop() {
	tg.wg.Add(1)
	defer tg.wg.Done()

	isStop := false
	ctx := context.Background()
	timer := time.NewTimer(time.Duration(tg.timeRange) * time.Millisecond)
	for {
		if isStop && len(tg.taskToReq) == 0 {
			return
		}
		select {
		case <-tg.shutdown:
			{
				isStop = true
			}
		case req := <-tg.requestQueue:
			{
				for i, _ := range req.tasks {
					t := req.tasks[i]

					_, ok := tg.taskToReq[t.UniqueId()]
					if !ok {
						tg.taskToReq[t.UniqueId()] = req
					} else {
						if req.isAsync == false {
							req.callback <- &result{key: t.UniqueId(), err: fmt.Errorf("duplicate unique id %s", t.UniqueId())}
						}
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
				tg.schedule(ctx)
			}
		case res := <-tg.resultQueue:
			{
				//check merge
				if len(res.mkey) > 0 {
					target, ok := tg.mergeTaskDic[res.mkey]
					if ok {
						for i, _ := range target {
							t := target[i]
							req, ok := tg.taskToReq[t.UniqueId()]
							if ok {
								if req.isAsync == false {
									req.callback <- &result{
										key: t.UniqueId(),
										val: res.val,
										err: res.err,
									}
								}
								req.count--
								if req.count == 0 {
									close(req.callback)
								}
								delete(tg.taskToReq, t.UniqueId())
							}
						}
						delete(tg.mergeTaskDic, res.mkey)
					}
				} else {
					req, ok := tg.taskToReq[res.key]
					if ok {
						if req.isAsync == false {
							req.callback <- res
						}
						req.count--
						if req.count == 0 {
							close(req.callback)
						}

						delete(tg.taskToReq, res.key)
					}
				}
				tg.schedule(ctx)
			}
		case <-timer.C:
			{
				tg.timerSchedule(ctx)
				timer.Reset(time.Duration(tg.timeRange) * time.Millisecond)
			}
		}
	}
}

func (tg *taskGroup) schedule(ctx context.Context) {
	tg.scheduleGroupTask(ctx, tg.batchSize)
	tg.scheduleTask(ctx)
}

func (tg *taskGroup) timerSchedule(ctx context.Context) {
	tg.scheduleGroupTask(ctx, 0)
	tg.scheduleTask(ctx)
}

func (tg *taskGroup) scheduleGroupTask(ctx context.Context, max int) {
	if tg.workerCount.Load() >= tg.maxWorker {
		return
	}

	delKeys := make([]string, 0)
	for gkey, _ := range tg.groupTaskDic {
		tasks := tg.groupTaskDic[gkey]

		if len(tasks) < max {
			continue
		}

		for {
			if tg.workerCount.Load() >= tg.maxWorker {
				goto CLEAN
			}
			if len(tasks) <= tg.batchSize {
				tg.workerCount.Add(1)
				go tg.running(ctx, tasks)
				delKeys = append(delKeys, gkey)
				break
			}

			target := tasks[:tg.batchSize]
			tasks = tasks[tg.batchSize:]
			tg.groupTaskDic[gkey] = tasks

			tg.workerCount.Add(1)
			go tg.running(ctx, target)
		}
	}
CLEAN:
	for i, _ := range delKeys {
		gkey := delKeys[i]
		delete(tg.groupTaskDic, gkey)
	}
}

func (tg *taskGroup) scheduleTask(ctx context.Context) {
	for {
		if tg.workerCount.Load() >= tg.maxWorker || len(tg.tasks) == 0 {
			break
		}

		t := tg.tasks[0]
		tg.workerCount.Add(1)
		go tg.running(ctx, []Task{t})
		if len(tg.tasks) == 1 {
			tg.tasks = tg.tasks[:0]
		} else {
			tg.tasks = tg.tasks[1:]
		}
	}
}

func (tg *taskGroup) running(ctx context.Context, tasks []Task) {
	defer func() {
		tg.workerCount.Add(-1)
	}()
	_, err := async.Safety(func() (any, error) {
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
}

func (tg *taskGroup) Stop() {
	close(tg.shutdown)
	tg.wg.Wait()
}
