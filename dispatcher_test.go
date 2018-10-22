package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// For
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
}

func Test_NewDispatcher(t *testing.T) {
	dpr, _ := New(
		WithQueueSize(1),
		WithWorkerCount(2),
		WithExecuterBuilder(&testBuilder{}),
	)

	dpr.Publish("test publish")
	dpr.PublishM(context.Background(), "test publishm")

	var wg sync.WaitGroup

	ctx := context.Background()
	f := func(no int) {
		for i := 1; i <= 5; i++ {
			cctx := context.WithValue(ctx, i, i)
			dpr.PublishM(cctx, fmt.Sprintf("with ctx2 %d-%d", no, i))
		}
		wg.Done()
	}

	for j := 1; j <= 5; j++ {
		wg.Add(1)
		go f(j)
	}

	wg.Wait()
}

func Test_NonBlockingPublish(t *testing.T) {
	dpr, _ := New(
		WithQueueSize(5),
		WithWorkerCount(2),
		WithExecuterBuilder(&testBuilder{}),
	)

	var wg sync.WaitGroup
	ctx := context.Background()

	for j := 1; j <= 5; j++ {
		wg.Add(1)
		go func(no int) {
			for i := 1; i <= 5; i++ {
				cctx := context.WithValue(ctx, i, i)
				if err := dpr.NonBlockingPublish(cctx, fmt.Sprintf("with ctx3 %d-%d", no, i)); err != nil {
					fmt.Println(err)
				}
			}
			wg.Done()
		}(j)
	}
	wg.Wait()
}

type testBuilder struct{}

func (*testBuilder) Build() (Executer, error) {
	return &TestExecuter{}, nil
}

type TestExecuter struct {
}

func (e *TestExecuter) Do(i interface{}) {
	m, ok := i.(*Msg)
	if ok {
		fmt.Println(m.Data)
	} else {
		fmt.Println(i)
	}
}
