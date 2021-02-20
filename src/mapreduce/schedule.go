package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	constructTaskArgs := func(phase jobPhase, task int) DoTaskArgs {
		debug("task: %d\n", task)
		var taskArgs DoTaskArgs
		taskArgs.Phase = phase
		taskArgs.JobName = jobName
		taskArgs.NumOtherPhase = n_other
		taskArgs.TaskNumber = task
		if phase == mapPhase {
			taskArgs.File = mapFiles[task]
		}
		return taskArgs
	}

	// 用一个waitGroup
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go callTask(&waitGroup, constructTaskArgs(phase, i), registerChan)
	}
	waitGroup.Wait()
	// todo: registerChan怎么用: 这个chan会全告诉一遍，后面如果有新来的也会告诉一遍
	// 并发且负载均衡地call这些task，然后统一收集结果，如果返回false就把它放到另一个worker上

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func callTask(wg *sync.WaitGroup, args DoTaskArgs, registerChan chan string) {
	workerName := <-registerChan
	ok := call(workerName, "Worker.DoTask", args, new(struct{}))
	if ok {
		wg.Done()
		registerChan <- workerName
		// 失败了就把这个worker还回到信道中去，然后再重新请求一遍，有可能换一个worker就能搞定了
	} else {
		//todo: 如果失败了不还回去呢？为啥这样就行呢？
		//registerChan <- workerName
		callTask(wg, args, registerChan)
	}
}
