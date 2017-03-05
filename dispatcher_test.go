package dispatcher

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func Test_dispatcher(t *testing.T) {
	dpr := NewDispatcher(1, 2, func() Executer {
		return &TestExecuter{}
	})

	dpr.StartWorkers()

	var wg sync.WaitGroup

	f := func(no int) {
		for i := 1; i <= 5; i++ {
			dpr.Publish(fmt.Sprintf("%d-%d", no, i))
		}
		wg.Done()
	}

	for j := 1; j <= 5; j++ {
		wg.Add(1)
		go f(j)
	}

	wg.Wait()
	time.Sleep(1 * time.Second)
}

type TestExecuter struct {
}

func (e *TestExecuter) Do(i interface{}) {
	fmt.Println(i)
}
