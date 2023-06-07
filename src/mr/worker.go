package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//// uncomment to send the Example RPC to the coordinator.
	//// CallExample()
	//args := ExampleArgs{}
	//args.X = 99
	//reply := ExampleReply{}
	//ok := call("Coordinator.Example", &args, &reply)

	//reply = Reply{}
	//reply.ReduceNumber = -1
	//for reply.ReduceNumber != -1 {
	//	_ = call("Coordinator.Do", &args, &reply)
	//	time.Sleep(time.Second)
	//}

	args := Args{}
	reply := Reply{}
	reply.State = 0
	for reply.State != DONE {
		args = Args{}
		reply = Reply{}
		reply.MapFilename = ""
		reply.ReduceFilenames = []string{}
		reply.Valid = 0
		reply.State = 0
		ok := call("Coordinator.Do", &args, &reply)
		if ok && reply.Valid == 1 {
			if reply.Type == MAP {
				//fmt.Printf("%d %s\n", reply.MapNumber, reply.MapFilename)
				type ByKey []KeyValue
				intermediate := []KeyValue{}
				file, err := os.Open(reply.MapFilename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.MapFilename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.MapFilename)
				}
				file.Close()
				kva := mapf(reply.MapFilename, string(content))
				intermediate = append(intermediate, kva...)
				var filePtrs []*os.File
				for i := 0; i < reply.NReduce; i++ {
					oname := fmt.Sprintf("mr-%d-%d.txt", reply.MapNumber, i)
					ofile, _ := ioutil.TempFile("", oname)
					filePtrs = append(filePtrs, ofile)
				}
				for _, kv := range intermediate {
					ReduceNumber := ihash(kv.Key) % reply.NReduce
					enc := json.NewEncoder(filePtrs[ReduceNumber])
					_ = enc.Encode(&kv)
				}
				args = Args{}
				for i := 0; i < len(filePtrs); i++ {
					args.Filenames = append(args.Filenames, filePtrs[i].Name())
				}
				args.MapNumber = reply.MapNumber
				_ = call("Coordinator.MapDone", &args, &reply)
			} else if reply.Type == REDUCE {
				intermediate := []KeyValue{}
				for i := 0; i < len(reply.ReduceFilenames); i++ {
					file, _ := os.Open(reply.ReduceFilenames[i])
					dec := json.NewDecoder(file)
					var kva []KeyValue
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
					intermediate = append(intermediate, kva...)
				}
				sort.Sort(ByKey(intermediate))
				oname := fmt.Sprintf("mr-out-%d", reply.ReduceNumber)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				ofile.Close()
				args.ReduceNumber = reply.ReduceNumber
				_ = call("Coordinator.ReduceDone", &args, &reply)
			}
		}
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
