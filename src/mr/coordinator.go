package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP    = 0
	REDUCE = 1
)
const (
	MAPING   = 2
	REDUCING = 3
)
const (
	DONE    = 4
	DOING   = 5
	UNSTART = 6
)

type Coordinator struct {
	// Your definitions here.
	Files            []string
	Maps             []Map
	Reduces          []Reduce
	State            int
	MapNumber        int
	MapDoneNumber    int
	ReduceNumber     int
	ReduceDoneNumber int
	NReduce          int
	InterFilenames   []InterFile
	Mutex            sync.Mutex
}

type InterFile struct {
	InterFilename string
	ReduceNumber  int
}
type Map struct {
	Time     time.Time
	Type     int
	Number   int
	MapState int
	filename string
}
type Reduce struct {
	Time        time.Time
	Type        int
	Number      int
	ReduceState int
	filenames   []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Do(args *Args, reply *Reply) error {
	c.Mutex.Lock()
	if c.State == MAPING {
		if c.MapNumber < len(c.Files) {
			var temp = Map{time.Now(), MAP, c.MapNumber, DOING, c.Files[c.MapNumber]}
			c.Maps = append(c.Maps, temp)
			reply.MapFilename = temp.filename
			reply.Type = MAP
			reply.MapNumber = c.MapNumber
			reply.NReduce = c.NReduce
			reply.State = c.State
			c.MapNumber++
		} else {
			for index, per_map := range c.Maps {
				if per_map.MapState == UNSTART {
					c.Maps[index].MapState = DOING
					reply.MapFilename = per_map.filename
					reply.Type = MAP
					reply.MapNumber = per_map.Number
					reply.NReduce = c.NReduce
					reply.State = c.State
				}
			}
		}
	} else if c.State == REDUCING {
		if c.ReduceNumber < c.NReduce {
			var filenames []string
			for i := 0; i < len(c.InterFilenames); i++ {
				if c.ReduceNumber == c.InterFilenames[i].ReduceNumber {
					filenames = append(filenames, c.InterFilenames[i].InterFilename)
				}
			}
			var temp = Reduce{time.Now(), REDUCING, c.ReduceNumber, DOING, filenames}
			c.Reduces = append(c.Reduces, temp)
			reply.ReduceFilenames = filenames
			reply.Type = REDUCE
			reply.ReduceNumber = c.ReduceNumber
			reply.NReduce = c.NReduce
			reply.State = c.State
			c.ReduceNumber++
		} else {
			for index, per_reduce := range c.Reduces {
				if per_reduce.ReduceState == UNSTART {
					c.Reduces[index].ReduceState = DOING
					reply.ReduceFilenames = per_reduce.filenames
					reply.Type = REDUCE
					reply.ReduceNumber = per_reduce.Number
					reply.NReduce = c.NReduce
					reply.State = c.State
				}
			}
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) ReduceDo(args *Args, reply *Reply) error {
	fmt.Print("ReduceDo start\n")
	c.Mutex.Lock()
	if c.State == REDUCING {
		if c.ReduceNumber < c.NReduce {
			var filenames []string
			for i := 0; i < len(c.InterFilenames); i++ {
				if c.ReduceNumber == c.InterFilenames[i].ReduceNumber {
					filenames = append(filenames, c.InterFilenames[i].InterFilename)
				}
			}
			var temp = Reduce{time.Now(), REDUCING, c.ReduceNumber, DOING, filenames}
			c.Reduces = append(c.Reduces, temp)
			c.ReduceNumber++
		} else {
			for index, per_reduce := range c.Reduces {
				if per_reduce.ReduceState == UNSTART {
					c.Reduces[index].ReduceState = DOING
					reply.ReduceFilenames = per_reduce.filenames
					reply.Type = REDUCE
					reply.ReduceNumber = per_reduce.Number
					reply.NReduce = c.NReduce
				}
			}
		}
	}
	c.Mutex.Unlock()
	return nil
}
func (c *Coordinator) MapDo(args *Args, reply *Reply) error {
	fmt.Print("MapDo start\n")
	c.Mutex.Lock()
	fmt.Printf("%v", c.State)
	if c.State == MAPING {
		if c.MapNumber < len(c.Files) {
			var temp = Map{time.Now(), MAP, c.MapNumber, DOING, c.Files[c.MapNumber]}
			c.Maps = append(c.Maps, temp)
			reply.MapFilename = temp.filename
			reply.Type = MAP
			reply.MapNumber = c.MapNumber
			reply.NReduce = c.NReduce
			c.MapNumber++
		} else {
			for index, per_map := range c.Maps {
				if per_map.MapState == UNSTART {
					c.Maps[index].MapState = DOING
					reply.MapFilename = per_map.filename
					reply.Type = MAP
					reply.MapNumber = per_map.Number
					reply.NReduce = c.NReduce
				}
			}
		}
	}
	c.Mutex.Unlock()
	fmt.Print("MapDo reply\n")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	ret := false
	// Your code here.
	if c.State == DONE {
		ret = true
	}
	c.Mutex.Unlock()
	return ret
}

func (c *Coordinator) MapDone(args *Args, reply *Reply) error {
	//fmt.Print("MapDone start\n")
	c.Mutex.Lock()
	for index, filename := range args.Filenames {
		newfilename := fmt.Sprintf("mr-%d-%d.txt", args.MapNumber, index)
		os.Rename(filename, newfilename)
		//tmp := Reduce{time.Now(), REDUCE, index, UNSTART, newfilename}
		//c.Reduces = append(c.Reduces, tmp)
		tmp := InterFile{newfilename, index}
		c.InterFilenames = append(c.InterFilenames, tmp)
		if c.MapDoneNumber == len(c.Files)-1 {
			c.State = REDUCING
		}
	}
	c.MapDoneNumber++
	c.Mutex.Unlock()
	//fmt.Print("MapDone over\n")
	return nil
}
func (c *Coordinator) ReduceDone(args *Args, reply *Reply) error {
	//fmt.Print("ReduceDone start\n")
	c.Mutex.Lock()
	c.ReduceDoneNumber++
	if c.ReduceDoneNumber == c.NReduce {
		c.State = DONE
	}
	c.Mutex.Unlock()
	//fmt.Print("ReduceDone over\n")
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.NReduce = nReduce
	c.Files = files
	c.State = MAPING
	c.MapNumber = 0
	c.ReduceNumber = 0
	c.MapDoneNumber = 0
	c.ReduceDoneNumber = 0
	//fmt.Print("coordinator\n")
	c.server()
	return &c
}
