package async

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const Unlimit = 0

type Method func(args ...interface{}) (interface{}, error)
type LambdaMethod func() (interface{}, error)

var panicHandler func(interface{})

func SetPanicHandler(hanlder func(interface{})) {
	panicHandler = hanlder
}

func Safety(method func() (interface{}, error)) (res interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			_, ok := e.(error)
			if ok {
				err = e.(error)
			} else {
				err = fmt.Errorf("%v", e)
			}
			if panicHandler != nil {
				panicHandler(err)
			}
		}
	}()
	res, err = method()
	return
}

func Retry(method func() (interface{}, error), maxCount int, interval time.Duration) (interface{}, error) {
	count := 0
	for {
		res, err := Lambda(method, Unlimit)
		if err == nil {
			return res, err
		}
		count = count + 1
		if count >= maxCount {
			return nil, err
		}
		<-time.After(time.Duration(count) * interval)
	}
}

type result struct {
	val interface{}
	err error
}

func Lambda(method func() (interface{}, error), timeout time.Duration) (interface{}, error) {
	output := make(chan *result, 1)
	go func() {
		defer close(output)
		defer func() {
			if e := recover(); e != nil {
				err := fmt.Errorf("%s", e)
				if panicHandler != nil {
					panicHandler(err)
				}
				output <- &result{err: err}
			}
		}()
		res, err := method()
		output <- &result{val: res, err: err}
	}()
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case res := <-output:
			{
				return res.val, res.err
			}
		case <-timer.C:
			{
				return nil, errors.New("Async_Timeout")
			}
		}
	} else {
		res := <-output
		return res.val, res.err
	}
}

func Call(m Method, timeout time.Duration, args ...interface{}) (interface{}, error) {
	return Lambda(func() (interface{}, error) {
		return m(args...)
	}, timeout)
}

func All(methods []LambdaMethod, timeout time.Duration) []interface{} {
	var wg sync.WaitGroup
	result := make([]interface{}, len(methods))
	for i, m := range methods {
		wg.Add(1)
		go func(index int, method LambdaMethod) {
			defer wg.Done()
			res, err := Lambda(method, timeout)
			if err != nil {
				result[index] = err
			} else {
				result[index] = res
			}
		}(i, m)
	}
	wg.Wait()
	return result
}

func Serise(methods []LambdaMethod, timeout time.Duration) []interface{} {
	result := make([]interface{}, 0)
	for _, m := range methods {
		res, err := Lambda(m, timeout)
		if err != nil {
			result = append(result, err)
			return result
		} else {
			result = append(result, res)
		}
	}
	return result
}

func Flow(enter Method, args []interface{}, methods []Method, timeout time.Duration) (interface{}, error) {
	var (
		res interface{}
		err error
	)
	res, err = Call(enter, timeout, args...)
	if err != nil {
		return nil, err
	}
	for _, m := range methods {
		res, err = Call(m, timeout, res)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func Any(methods []LambdaMethod, timeout time.Duration) ([]interface{}, error) {
	resChan := make(chan []interface{}, 1)
	errChan := make(chan error, len(methods))
	go func() {
		defer func() {
			close(resChan)
			close(errChan)
		}()
		var wg sync.WaitGroup
		result := make([]interface{}, len(methods))
		for i, m := range methods {
			wg.Add(1)
			go func(index int, method LambdaMethod) {
				defer wg.Done()
				res, err := Lambda(method, timeout)
				if err != nil {
					errChan <- err
				} else {
					result[index] = res
				}
			}(i, m)
		}
		wg.Wait()
		resChan <- result
	}()
	select {
	case err := <-errChan:
		return nil, err
	case res := <-resChan:
		return res, nil
	}
}

func AnyOne(methods []LambdaMethod, timeout time.Duration) (interface{}, []error) {
	resChan := make(chan interface{}, len(methods))
	errChan := make(chan []error, 1)
	go func() {
		defer func() {
			close(resChan)
			close(errChan)
		}()
		var wg sync.WaitGroup
		errs := make([]error, len(methods))
		for i, m := range methods {
			wg.Add(1)
			go func(index int, method LambdaMethod) {
				defer wg.Done()
				res, err := Lambda(method, timeout)
				if err != nil {
					errs[index] = err
				} else {
					resChan <- res
				}
			}(i, m)
		}
		wg.Wait()
		errChan <- errs
	}()
	select {
	case errs := <-errChan:
		return nil, errs
	case res := <-resChan:
		return res, nil
	}
}

func Parallel(methods []LambdaMethod, maxCount int) []interface{} {
	if maxCount == Unlimit {
		maxCount = 64
	}
	var wg sync.WaitGroup
	workers := make(chan bool, maxCount)
	results := make([]interface{}, len(methods))
	for index, method := range methods {
		workers <- true
		wg.Add(1)
		go func(i int, m LambdaMethod) {
			defer func() {
				<-workers
				wg.Done()
			}()
			res, err := Lambda(m, 0)
			if err != nil {
				results[i] = err
			} else {
				results[i] = res
			}
		}(index, method)
	}
	wg.Wait()
	close(workers)
	return results
}

func Foreach(objs []interface{}, method func(int) (interface{}, error), maxConcurrent int) []interface{} {
	if maxConcurrent == Unlimit {
		maxConcurrent = 64
	}
	var wg sync.WaitGroup
	workers := make(chan bool, maxConcurrent)
	results := make([]interface{}, len(objs))
	for index := range objs {
		workers <- true
		wg.Add(1)
		go func(i int, method func(int) (interface{}, error)) {
			defer func() {
				<-workers
				wg.Done()
			}()
			res, err := Safety(func() (interface{}, error) {
				return method(i)
			})
			if err != nil {
				results[i] = err
			} else {
				results[i] = res
			}
		}(index, method)
	}
	wg.Wait()
	close(workers)
	return results
}

func For(count int, method func(int) (interface{}, error), maxConcurrent int) []interface{} {
	if maxConcurrent == Unlimit {
		maxConcurrent = 64
	}
	var wg sync.WaitGroup
	workers := make(chan bool, maxConcurrent)
	results := make([]interface{}, count)
	for index := 0; index < count; index++ {
		workers <- true
		wg.Add(1)
		go func(i int, method func(int) (interface{}, error)) {
			defer func() {
				<-workers
				wg.Done()
			}()
			res, err := Safety(func() (interface{}, error) {
				return method(i)
			})
			if err != nil {
				results[i] = err
			} else {
				results[i] = res
			}
		}(index, method)
	}
	wg.Wait()
	close(workers)
	return results
}
