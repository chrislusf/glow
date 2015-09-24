package scheduler

import (
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/flame"
)

type Scheduler struct {
	Leader           string
	EventChan        chan interface{}
	resources        []string
	taskGroups       []*TaskGroup
	requestResources *RequestResources
}

type RequestResources struct {
	a, b         int
	isRequesting bool
}

func NewScheduler(leader string) *Scheduler {
	return &Scheduler{
		Leader:    leader,
		EventChan: make(chan interface{}),
		requestResources: &RequestResources{
			a: 0,
			b: 1,
		},
	}
}

type SubmittedTaskGroup struct {
	FlowContext *flame.FlowContext
	TaskGroup   *TaskGroup
	WaitGroup   *sync.WaitGroup
}

type AddedServer struct {
}

/*
resources are leased to driver, expires every X miniute unless renewed.
1. request resource
2. release resource
*/
func (s *Scheduler) Loop() {
	for {
		event := <-s.EventChan
		switch event := event.(type) {
		default:
		case SubmittedTaskGroup:
			// fmt.Printf("processing %+v\n", event)
			taskGroup := event.TaskGroup
			server := "a1" // "localhost:8931" // s.pickOneServer()
			go func() {
				defer event.WaitGroup.Done()
				dir, _ := os.Getwd()
				request := NewStartRequest(os.Args[0], dir,
					"-task.context.id",
					strconv.Itoa(event.FlowContext.Id),
					"-task.taskGroup.id",
					strconv.Itoa(taskGroup.Id),
				)
				// fmt.Printf("starting on %s: %v\n", server, request)
				if err := RemoteExecute(s.Leader, server, request); err != nil {
					println("exeuction error:", err.Error())
				}
			}()
		case int:
		case *bool:
		case *int:
		}
	}
}

// Continuous Double Auction
func (s *Scheduler) pickOneServer() {
}

func (s *Scheduler) requestServers() []string {
	size, ok := s.requestResources.requestSize()
	if !ok || size <= 0 {
		return nil
	}
	// send size, datacenter, rack info to leader
	// receive servers []string
	servers := []string{"localhost:8931"}
	s.resources = append(s.resources, servers...)
	return servers
}

func (r *RequestResources) requestSize() (int, bool) {
	if r.isRequesting {
		return 0, false
	}
	r.isRequesting = true
	ret := r.b
	r.a, r.b = r.b, r.a+r.b
	return ret, true
}
