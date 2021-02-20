package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
// 读取某个输入文件，对于文件的内容调用用户定义的map函数，并且将输出分成nReduce个中间文件
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	var fileMap map[string]*os.File = make(map[string]*os.File)
	// 首先读取文件
	in, err := os.Open(inFile)
	if err != nil {
		panic(err)
	}
	defer in.Close()
	content, err := ioutil.ReadAll(in)

	// 然后调用mapF函数
	keyValues := mapF(inFile, string(content))

	// 然后将kv对放到不同的文件中
	for _, keyValue := range keyValues {

		// 获取文件file指针
		reduceTaskNumber := ihash(keyValue.Key) % nReduce
		reduceFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		reduceFile, ok := fileMap[reduceFileName]
		if !ok {
			reduceFile = openFileAndWriteJsonPrefix(reduceFileName)
			fileMap[reduceFileName] = reduceFile
		}

		// 将keyValue解析为json
		jsonStr, err := json.Marshal(keyValue)
		if err != nil {
			fmt.Println("生成json字符串错误")
			panic(err)
		}

		// 写入到文件中
		_, err1 := io.WriteString(reduceFile, string(jsonStr)+",")
		if err1 != nil {
			panic(err1)
		}

	}
	// 关闭文件map中所有打开的文件
	for _, file := range fileMap {

		// 写入JSON数组后缀
		_, err = file.Seek(-1, 1)
		if err != nil {
			panic(err)
		}
		_, err = file.WriteString("]")
		if err != nil {
			panic(err)
		}

		// 关闭文件
		defer file.Close()
	}

	//
	// You will need to write this function.
	//
	// The intermediate output of a map task is stored as multiple
	// files, one per destination reduce task. The file name includes
	// both the map task number and the reduce task number. Use the
	// filename generated by reduceName(jobName, mapTaskNumber, r) as
	// the intermediate file for reduce task r. Call ihash() (see below)
	// on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
}

//存在返回 true，不存在返回 false
func fileIfExist(filename string) bool {
	_, err := os.Stat(filename)
	if nil != err {
		return false
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
