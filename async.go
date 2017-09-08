package async

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Method func(ctx context.Context, args ...interface{}) (interface{}, error)
type LambdaMethod func() (interface{}, error)

func Lambda(method LambdaMethod, timeout time.Duration) (interface{}, error) {
	output := make(chan interface{})
	go func() {
		defer close(output)
		res, err := method()
		if err != nil {
			output <- err
		} else {
			output <- res
		}
	}()
	if timeout <= 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case res := <-output:
			{
				switch err := res.(type) {
				case error:
					{
						return nil, err
					}
				default:
					{
						return res, nil
					}
				}
			}
		case <-timer.C:
			{
				return nil, errors.New("Async_Timeout")
			}
		}
	} else {
		res := <-output
		switch err := res.(type) {
		case error:
			{
				return nil, err
			}
		default:
			{
				return res, nil
			}
		}
	}
}

func Call(m Method, ctx context.Context, timeout time.Duration, args ...interface{}) (interface{}, error) {
	return Lambda(LambdaMethod {
		return m(ctx, args...)
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

func Serise(enter Method, ctx context.Context, args []interface{}, methods []Method, timeout time.Duration) (interface{}, error) {
	var (
		res interface{}
		err error
	)
	res, err = Call(enter, ctx, timeout, args...)
	if err != nil {
		return nil, err
	}
	for _, m := range methods {
		res, err = Call(m, ctx, timeout, res)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func Any(methods []LambdaMethod, timeout time.Duration) ([]interface{}, error) {
	resChan := make(chan []interface{})
	errChan := make(chan error)
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
