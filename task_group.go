package async

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	taskOne      sync.Once
	taskInput    chan *taskCmd
	taskOutput   chan *batchTaskCmd
	taskGroupDic map[string]*taskGroup
)

func init() {
	taskGroupDic = make(map[string]*taskGroup)
	taskInput = make(chan *taskCmd, 128)
	taskOutput = make(chan *batchTaskCmd, 128)
}

type Task interface {
	Key() string
}

type taskResult struct {
	key string
	val interface{}
}

type taskGroup struct {
	name       string
	batchSize  int
	method     func(...Task) (map[string]interface{}, error)
	taskCmdDic map[string][]*taskCmd
	taskDic    map[string]Task
}

type taskCmd struct {
	group    string
	tasks    []Task
	count    int
	callback chan *taskResult
}

func (c *taskCmd) output(res *taskResult) {
	c.callback <- res
	c.count--
	if c.count == 0 {
		close(c.callback)
	}
}

type batchTaskCmd struct {
	group  string
	tasks  []Task
	err    error
	result map[string]interface{}
}

func RegisterTaskGroup(
	name string,
	batchSize int,
	method func(...Task) (map[string]interface{}, error)) error {

	_, ok := taskGroupDic[name]
	if ok {
		return fmt.Errorf("duplicate task group:%s", name)
	}

	taskGroupDic[name] = &taskGroup{
		name:       name,
		batchSize:  batchSize,
		method:     method,
		taskCmdDic: make(map[string][]*taskCmd),
		taskDic:    make(map[string]Task),
	}
	taskOne.Do(func() {
		go taskRunloop()
	})
	return nil
}

func execTask(ctx context.Context, b *batchTaskCmd, method func(...Task) (map[string]interface{}, error)) {
	res, err := Safety(func() (interface{}, error) {
		return method(b.tasks...)
	})
	if err != nil {
		b.err = err
	} else {
		b.result = res.(map[string]interface{})
	}
	taskOutput <- b
}

func taskRunloop() {
	ctx := context.Background()
	timer := time.NewTimer(10 * time.Millisecond)
	for {
		select {
		case c := <-taskInput:
			{
				g, ok := taskGroupDic[c.group]
				if false == ok {
					close(c.callback)
					continue
				}

				for index, _ := range c.tasks {
					task := c.tasks[index]
					key := task.Key()

					target, ok := g.taskCmdDic[key]
					if ok {
						g.taskCmdDic[key] = append(target, c)
					} else {
						g.taskCmdDic[key] = []*taskCmd{c}
						g.taskDic[key] = task
					}

					if len(g.taskDic) >= g.batchSize {
						b := &batchTaskCmd{group: g.name}
						b.tasks = make([]Task, len(g.taskDic))
						index := 0
						for k, _ := range g.taskDic {
							b.tasks[index] = g.taskDic[k]
							index = index + 1
						}
						g.taskDic = make(map[string]Task)
						go execTask(ctx, b, g.method)
					}
				}
			}
		case b := <-taskOutput:
			{
				g, _ := taskGroupDic[b.group]
				if b.err != nil {
					for i, _ := range b.tasks {
						task := b.tasks[i]
						key := task.Key()

						target, ok := g.taskCmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.output(&taskResult{key: key, val: b.err})
							}
						}
						delete(g.taskCmdDic, key)
					}
				} else {
					for i, _ := range b.tasks {
						task := b.tasks[i]
						key := task.Key()

						res, ok := b.result[key]
						if false == ok {
							res = fmt.Errorf("invalid key:%s", key)
						}
						target, ok := g.taskCmdDic[key]
						if ok {
							for index, _ := range target {
								c := target[index]
								c.output(&taskResult{key: key, val: res})
							}
						}
						delete(g.taskCmdDic, key)
					}
				}
			}
		case <-timer.C:
			{
				for _, g := range taskGroupDic {
					if len(g.taskDic) > 0 {
						b := &batchTaskCmd{group: g.name}
						b.tasks = make([]Task, len(g.taskDic))
						index := 0
						for i, _ := range g.taskDic {
							b.tasks[index] = g.taskDic[i]
							index = index + 1
						}
						g.taskDic = make(map[string]Task)
						go execTask(ctx, b, g.method)
					}
				}
				timer.Reset(10 * time.Millisecond)
			}
		}
	}
}

func Exec(group string, task Task) (interface{}, error) {
	if task == nil {
		return nil, fmt.Errorf("invalid task")
	}
	return exec(group, task)
}

func exec(group string, task Task) (interface{}, error) {
	c := &taskCmd{
		group:    group,
		tasks:    []Task{task},
		count:    1,
		callback: make(chan *taskResult, 1),
	}
	taskInput <- c
	res := <-c.callback
	switch res.val.(type) {
	case error:
		return nil, res.val.(error)
	default:
		return res.val, nil
	}
}

func BarchExec(group string, tasks ...Task) map[string]interface{} {
	if tasks == nil || len(tasks) == 0 {
		return nil
	}
	return batchExec(group, tasks...)
}

func batchExec(group string, tasks ...Task) map[string]interface{} {
	c := &taskCmd{
		group:    group,
		tasks:    tasks,
		count:    len(tasks),
		callback: make(chan *taskResult, len(tasks)),
	}
	taskInput <- c
	results := make(map[string]interface{})
	for result := range c.callback {
		results[result.key] = result.val
	}
	return results
}
