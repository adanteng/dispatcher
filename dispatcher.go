package dispatcher

import (
	"context"
	"fmt"
	"sync"
)

type dispOptions struct {
	// buffer length, but when consumer is so slow this will change nothing
	queueSize int64
	// make the consume faster, according the cpu load
	workerCount int64

	executerBuilder Builder
}

type Builder interface {
	Build() (Executer, error)
}

type dispOptionsFunc func(*dispOptions)

func WithQueueSize(s int64) dispOptionsFunc {
	return func(o *dispOptions) {
		o.queueSize = s
	}
}

func WithWorkerCount(s int64) dispOptionsFunc {
	return func(o *dispOptions) {
		o.workerCount = s
	}
}

func WithExecuterBuilder(b Builder) dispOptionsFunc {
	return func(o *dispOptions) {
		o.executerBuilder = b
	}
}

var defaultDispOptions = dispOptions{
	queueSize:   50,
	workerCount: 3,
}

type Dispatcher struct {
	inputQueue chan interface{}
	workerPool chan chan interface{}

	// DEPRECATED: Not recommended to use this way.
	createExecuter CreateExecuter

	// Incase app SetExecuter to nil, add mu.
	once     sync.Once
	executer Executer
}

func NewDispatcher(queueSize, workerCount int, createExecuter CreateExecuter) *Dispatcher {
	d := &Dispatcher{
		inputQueue: make(chan interface{}, queueSize),
		workerPool: make(chan chan interface{}, workerCount),
	}

	if createExecuter == nil {
		fmt.Println("createExecuter should not be nil")
		return d
	}
	d.executer = createExecuter()

	return d
}

// MUST: Use SetExecuter after call this func
func New(opt ...dispOptionsFunc) (*Dispatcher, error) {
	opts := defaultDispOptions
	for _, o := range opt {
		o(&opts)
	}

	d := &Dispatcher{
		inputQueue: make(chan interface{}, opts.queueSize),
		workerPool: make(chan chan interface{}, opts.workerCount),
	}

	// May use SetExecuter too.
	if opts.executerBuilder != nil {
		executer, err := opts.executerBuilder.Build()
		if err != nil {
			return nil, err
		}
		d.executer = executer
	}

	// start worker according to workerPool directly,
	// and start to dispatch workerPool if msg come in.
	d.startWorker()

	return d, nil
}

// DEPRECATED: Use NewDispatcher(opt ...dispOptionsFunc), not necessary anymore.
func (d *Dispatcher) StartWorkers() {
	d.startWorker()
}

func (d *Dispatcher) startWorker() {
	for i := 0; i < cap(d.workerPool); i++ {
		worker := newWorker(d.workerPool, d)
		worker.start()
	}

	go func() {
		for m := range d.inputQueue {
			idleWorker := <-d.workerPool
			idleWorker <- m
		}
	}()
}

func (d *Dispatcher) SetExecuter(e Executer) {
	d.once.Do(func() { d.executer = e })
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
	// keep the ptr to Dispatcher
	dptr      *Dispatcher
	queuePool chan chan interface{}
	queue     chan interface{}
}

func newWorker(pool chan chan interface{}, d *Dispatcher) *worker {
	return &worker{
		queuePool: pool,
		queue:     make(chan interface{}),
		dptr:      d,
	}
}

func (w *worker) start() {
	go func() {
		for {
			w.queuePool <- w.queue
			select {
			case m := <-w.queue:
				if w.dptr.executer != nil {
					w.dptr.executer.Do(m)
				} else {
					fmt.Println("nil executer")
				}
			}
		}
	}()
}

type CreateExecuter func() Executer

// app implement this interface, inject your executer to worker
type Executer interface {
	Do(i interface{})
}
