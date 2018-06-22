package dispatcher

import (
	"golang.org/x/net/context"
)

type Dispatcher struct {
	inputQueue     chan interface{}
	workerPool     chan chan interface{}
	createExecuter CreateExecuter
}

func NewDispatcher(queueSize, workerCount int, createExecuter CreateExecuter) *Dispatcher {
	return &Dispatcher{
		inputQueue:     make(chan interface{}, queueSize),
		workerPool:     make(chan chan interface{}, workerCount),
		createExecuter: createExecuter,
	}
}

func (d *Dispatcher) StartWorkers() {
	for i := 0; i < cap(d.workerPool); i++ {
		worker := newWorker(d.workerPool, d.createExecuter())
		worker.start()
	}

	go func() {
		for m := range d.inputQueue {
			idleWorker := <-d.workerPool
			idleWorker <- m
		}
	}()
}

// DEPRECATED: Use PublishM instead.
func (d *Dispatcher) Publish(i interface{}) {
	d.inputQueue <- i
}

// M contain ctx, you can put logid or something you want pass to executer
type msg struct {
	ctx  context.Context
	data interface{}
}

func (d *Dispatcher) PublishM(c context.Context, dat interface{}) {
	m := &msg{ctx: c, data: dat}
	d.inputQueue <- m
}

type worker struct {
	queuePool chan chan interface{}
	queue     chan interface{}
	executer  Executer
}

func newWorker(pool chan chan interface{}, executer Executer) *worker {
	return &worker{
		queuePool: pool,
		queue:     make(chan interface{}),
		executer:  executer,
	}
}

func (w *worker) start() {
	go func() {
		for {
			w.queuePool <- w.queue
			select {
			case m := <-w.queue:
				w.executer.Do(m)
			}
		}
	}()
}

type CreateExecuter func() Executer

// app implement this interface, inject your executer to worker
type Executer interface {
	Do(i interface{})
}
