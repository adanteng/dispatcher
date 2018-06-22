package dispatcher

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
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

func Test_PublishM(t *testing.T) {
	dpr := NewDispatcher(1, 2, func() Executer {
		return &TestExecuter{}
	})

	dpr.StartWorkers()

	var wg sync.WaitGroup

	ctx := context.Background()
	f := func(no int) {
		for i := 1; i <= 5; i++ {
			cctx := context.WithValue(ctx, i, i)
			dpr.PublishM(cctx, fmt.Sprintf("with ctx %d-%d", no, i))
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
	m, ok := i.(*msg)
	if ok {
		fmt.Println(m.data)
	} else {
		fmt.Println(i)
	}
}
