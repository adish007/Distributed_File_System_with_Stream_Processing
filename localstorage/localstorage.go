// All local operations, no messages to other nodes.
package localstorage

import (
	"encoding/json"
	"fmt"
	"g85/mp4_again_again/utils"
	"net"
	"net/rpc"
	"os"
	"syscall"
)

type LocalStorage struct {
	Filenames     []string
	FileToAppends map[string][]AppendInfo
}

// RPC to perform action locally
func (l *LocalStorage) RPCRecieve(input utils.FileSystemMessage, reply *utils.FileSystemReply) error {
	if input.Action != "listFiles" {
		fmt.Printf("Recieved %s for %s in RPCRecieve\n", input.Action, input.Filename)
	}
	var err error
	switch input.Action {
	case "create":
		err = l.AddFile(input.Filename, input.Content)
		l.Filenames = append(l.Filenames, input.Filename)
	case "append":
		err = l.AppendToFile(input.Filename, input.AppendTimestamp, input.Content)
	case "rawAppend":
		err = l.RawAppendToFile(input.Filename, input.Content)
	case "get":
		reply.Content, err = l.GetFile(input.Filename)
	case "getRemoveAppendInfos":
		jsonAppendInfos, _ := json.Marshal(l.GetAndRemoveAppendInfosForFile(input.Filename))
		reply.Content = jsonAppendInfos
	case "listFiles":
		var filenames []string
		filenames, err = l.ListFiles()
		reply.Content, _ = json.Marshal(filenames)
	case "kill":
		fmt.Printf("KILLING PROCESS\n")
		// os.Exit(1)
		// panic("KILLING PROCESS")
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}
	if err != nil {
		reply.Error = err.Error()
	}
	// skipped case "list" bc unnecessary
	return nil
}

func (l *LocalStorage) AddFile(hdfs_filename string, content []byte) error {
	fmt.Printf("LOG: Local Storage: Create for hdfs (%s): Started \n", hdfs_filename)

	local_filename := GetLocalFilename(hdfs_filename)

	if fileExists(local_filename) {
		return fmt.Errorf("file already exists (%s)", local_filename)
	}

	f, err := os.Create(local_filename)
	if err != nil {
		return fmt.Errorf("failed to create file (%s): %v", local_filename, err)
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return fmt.Errorf("failed to write to file (%s): %v", local_filename, err)
	}

	fmt.Printf("LOG: Local Storage: Create for hdfs (%s): Finished \n", hdfs_filename)
	return nil
}

type AppendInfo struct {
	Filename  string `json:"filename"`
	Timestamp int64  `json:"timestamp"`
	Content   []byte `json:"content"`
}

func (l *LocalStorage) AppendToFile(hdfs_filename string, append_timestamp int64, content []byte) error {
	fmt.Printf("LOG: Local Storage: Append for hdfs (%s): Started \n", hdfs_filename)

	local_filename := GetLocalFilename(hdfs_filename)

	if !fileExists(local_filename) {
		return fmt.Errorf("file doesn't exist (%s)", local_filename)
	}

	appendInfo := AppendInfo{Filename: hdfs_filename, Timestamp: append_timestamp, Content: content}
	l.FileToAppends[hdfs_filename] = append(l.FileToAppends[hdfs_filename], appendInfo)

	fmt.Printf("LOG: Local Storage: Append for hdfs (%s): Finished \n", hdfs_filename)
	return nil
}

func (l *LocalStorage) RawAppendToFile(hdfs_filename string, content []byte) error {
	fmt.Printf("LOG: Local Storage: Raw Append for hdfs (%s): Started \n", hdfs_filename)

	local_filename := GetLocalFilename(hdfs_filename)

	if !fileExists(local_filename) {
		return fmt.Errorf("file doesn't exist (%s)", local_filename)
	}

	f, err := os.OpenFile(local_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // open file in write mode
	if err != nil {
		return fmt.Errorf("failed to create file (%s): %v", local_filename, err)
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return fmt.Errorf("failed to append to file (%s): %v", local_filename, err)
	}
	fmt.Printf("LOG: Local Storage: Raw Append for hdfs (%s): Finished \n", hdfs_filename)
	return nil

}

// This will not read the appends - it assumes that a merge occurred before this get
func (l *LocalStorage) GetFile(hdfs_filename string) ([]byte, error) {
	fmt.Printf("LOG: Local Storage: Get for hdfs (%s): Started \n", hdfs_filename)

	local_filename := GetLocalFilename(hdfs_filename)

	data, err := os.ReadFile(local_filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file (%s): %v", local_filename, err)
	}
	fmt.Printf("LOG: Local Storage: Get for hdfs (%s): Finished \n", hdfs_filename)

	return data, nil
}

func (l *LocalStorage) GetAndRemoveAppendInfosForFile(hdfs_filename string) []AppendInfo {
	res := l.FileToAppends[hdfs_filename]
	l.FileToAppends[hdfs_filename] = []AppendInfo{}
	return res
}

func (l *LocalStorage) ListFiles() ([]string, error) {
	files, err := os.ReadDir("localfiles")

	if err != nil {
		return nil, fmt.Errorf("failed to list local files: %v", err)
	}

	var res []string
	for _, file := range files {
		res = append(res, file.Name())
	}

	return res, nil
}

func GetLocalFilename(hdfs_filename string) string {
	return "localfiles/" + hdfs_filename
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func ContinuouslyListen(listener net.Listener) {
	for {
		memberConn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go rpc.ServeConn(memberConn)
	}
}

// Ran from @TODO somewhere to start LocalStorage
func Start(TCPport int) (*LocalStorage, error) {
	fmt.Println("Starting LocalStorage")

	// Create localfiles dir (if it doesn't already exist)
	err := os.MkdirAll("localfiles", 0755)
	if err != nil {
		return nil, err
	}

	// Start RPC server for LocalStorage
	l := &LocalStorage{Filenames: []string{}, FileToAppends: map[string][]AppendInfo{}}
	rpc.Register(l)
	listeningAddress := fmt.Sprintf(":%d", TCPport)
	listener, err := net.Listen("tcp", listeningAddress)
	if err != nil {
		return nil, err
	}
	fmt.Printf("LOG: TCP listener created on port %d \n", TCPport)

	// Continuously listen for a connection
	go ContinuouslyListen(listener)
	return l, nil
}
