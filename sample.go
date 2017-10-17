package async

import (
	"time"
	"fmt"
	"github.com/RyouZhang/async-go"
)

func test_func(x, y int) int {
	<-time.After(1  * time.Second)
	return x + y
}

func test_func2(x, y int) int {
	<-time.After(2  * time.Second)
	return x * y	
}

func main() {

	var (
		res interface{}
		err error
	)

	f1 := func() (interface{}, error) {
		z := test_func(2, 3)
		return z, nil
	}
	//working well
	res, err = async.Lambda(f1, 5 * time.Second)
	fmt.Println(res, err)
	//now timeout trigger
	res, err = async.Lambda(f1, 50 * time.Millisecond)
	fmt.Println(res, err)
	
	f2 := func() (interface{}, error) {
		z := test_func2(2, 3)
		return z, nil
	}

	//for parallel
	res = async.All([]async.LambdaMethod{f1, f2}, 2 * time.Second)
	fmt.Println(res)

	res = async.All([]async.LambdaMethod{f1, f2}, 5 * time.Second)
	fmt.Println(res)

	//for any, one error all error
	res, err = async.Any([]async.LambdaMethod{f1, 1
						  2}, 2 * time.Second)
	fmt.Println(res, err)
}
