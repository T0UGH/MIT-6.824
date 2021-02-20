package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

// doReduce管理一个reduce任务：
// 1. 读取中间内容(read the intermediate key/value pairs for this task)
// 2. sort
// 3. 为每个键调用用户定义的reduce函数（reduceF
// 4. 将输出写入磁盘

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//1. 读取所有mapTask的中间文件
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	var keyValues []KeyValue
	for mapTaskNumber := 0; mapTaskNumber < nMap; mapTaskNumber++ {
		intermediateFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		intermediateFile, err := os.Open(intermediateFileName)
		// 有可能出现没有中间文件的情况
		if err != nil {
			break
		}
		content, err := ioutil.ReadAll(intermediateFile)
		intermediateFile.Close()
		var currKeyValues []KeyValue
		err = json.Unmarshal(content, &currKeyValues)
		if err != nil {
			fmt.Print(string(content))
			panic(err)
		}
		keyValues = append(keyValues, currKeyValues...)
	}
	debug("Len of keyValues common_reduce: %d", len(keyValues))
	//2. 对Key进行排序
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	sort.Slice(keyValues, func(i, j int) bool {
		if keyValues[i].Key < keyValues[j].Key {
			return true
		}
		return false
	})
	debug("Len of keyValues common_reduce: %d", len(keyValues))

	//3.调用reduceF
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	var outputContent = make([]KeyValue, 0)
	if len(keyValues) == 0 {
		return
	}
	var valueList = make([]string, 0)
	lastKey := keyValues[0].Key
	for _, keyValue := range keyValues {
		if keyValue.Key == lastKey {
			valueList = append(valueList, keyValue.Value)
		} else {
			// 调用reduceF
			reduceValue := reduceF(lastKey, valueList)
			outputContent = append(outputContent, KeyValue{Key: lastKey, Value: reduceValue})
			// 清空valueList
			valueList = make([]string, 0)
			// 更新lastKey
			lastKey = keyValue.Key
			// 放入
			valueList = append(valueList, keyValue.Value)
		}
	}
	// 最后还得调用一次
	reduceValue := reduceF(lastKey, valueList)
	outputContent = append(outputContent, KeyValue{Key: lastKey, Value: reduceValue})

	jsonStr, err := json.Marshal(outputContent)
	if err != nil {
		fmt.Println("生成json字符串错误")
		panic(err)
	}

	outputFile := openFile(mergeName(jobName, reduceTaskNumber))
	_, err = outputFile.Write(jsonStr)
	if err != nil {
		panic(err)
	}
	outputFile.Close()

	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}

func openFile(filename string) *os.File {
	var file *os.File
	var err error
	if fileIfExist(filename) {
		err = os.Remove(filename)
		if err != nil {
			panic(err)
		}
	}
	file, err = os.Create(filename)
	if err != nil {
		panic(err)
	}

	return file
}
