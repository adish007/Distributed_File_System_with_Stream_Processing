package rainstorm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"g85/mp4_again_again/ring/member"
	"g85/mp4_again_again/synchronizer"
	"g85/mp4_again_again/utils"
	"math"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	MAX_RPC_ATTEMPTS = 1
	RPC_TIMEOUT      = 7 * time.Second
	BATCH_SIZE       = 5
	PORT             = 50001
)

type RainStormMember struct {
	LocalMember                 *member.Member
	VMNum                       int
	VMHostname                  string
	UploadedBatchLineNumbers    map[int]bool
	Mu_UploadedBatchLineNumbers *sync.Mutex
	AggregateCounts             map[string]int
	Worker_BatchesInProgress    map[int]int // batch start line number to VMnum that is doing it, -1 if done
}

func GetHostname(vmNum int) string {
	if vmNum == 10 {
		return "fa24-cs425-8510.cs.illinois.edu"
	}
	return fmt.Sprintf("fa24-cs425-850%d.cs.illinois.edu", vmNum)
}

func (r *RainStormMember) Run() error {
	r.VMHostname = GetHostname(r.VMNum)
	if r.VMNum == 10 {
		return r.RunIntroducer()
	}
	return r.RunWorker()
}

func (r *RainStormMember) RunIntroducer() error {
	// Start RPC server for RainStormLeader to collect outputs
	rpc.Register(r)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		return err
	}
	fmt.Printf("LOG: TCP Leader listener created on port %d \n", PORT)

	// Continuously listen for a connection
	go ContinuouslyListen(listener)
	return nil
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

func (r *RainStormMember) RunWorker() error {
	r.Worker_BatchesInProgress = make(map[int]int) // clear the batches in progress
	// Start RPC server for RainStormMember
	rpc.Register(r)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		return err
	}
	fmt.Printf("LOG: TCP Worker listener created on port %d \n", PORT)

	// Continuously listen for a connection
	go ContinuouslyListen(listener)
	return nil
}

type Partition struct {
	Offset   int
	Length   int
	SrcFile  string
	DstFile  string
	Op1File  string
	Op2File  string
	NumTasks int
}

// this will always run on 10
func (r *RainStormMember) RainstormCmd(op1_file string, op2_file string, src_file string, dest_file string, num_tasks int) error {
	r.Mu_UploadedBatchLineNumbers = &sync.Mutex{}
	r.UploadedBatchLineNumbers = make(map[int]bool) // clear the uploaded batches set
	r.AggregateCounts = make(map[string]int)        // clear the existing aggregate

	// All files should be created in hydfs already

	// Create partition structs
	srcContent, err := r.GetSourceContent(src_file)
	if err != nil {
		return fmt.Errorf("failed to get source content (%s): %v", src_file, err)
	}
	srcContentLines := bytes.Split(srcContent, []byte("\n"))

	srcLength := len(srcContentLines)
	partitions := []Partition{}
	partition_size := int(math.Ceil(float64(srcLength) / float64(num_tasks)))
	for offset := 0; offset < srcLength; offset += partition_size {
		length := partition_size
		if offset+partition_size > srcLength {
			length = srcLength - offset
		}
		partitions = append(partitions, Partition{Offset: offset, Length: length, SrcFile: src_file, DstFile: dest_file,
			Op1File: op1_file, Op2File: op2_file, NumTasks: num_tasks})
	}

	// Send to stage 1 members
	fmt.Printf("LOG: Created partitions, sending payload to nodes ... \n")

	// LOG (TODO remove)
	fmt.Printf("Number of lines in input: %d \n", len(srcContentLines))
	for _, p := range partitions {
		fmt.Printf("Number of lines in partition at offset %d: \t %d \n", p.Offset, p.Length)
	}

	err = r.SendPartitionsToStage1(partitions)
	if err != nil {
		return fmt.Errorf("failed to send partitions to stage 1 nodes (%s): %v", src_file, err)
	}
	return nil
}

func (r *RainStormMember) GetSourceContent(HyDFS_src_filename string) ([]byte, error) {
	// get content of src
	filenameHash := utils.ComputeHash(HyDFS_src_filename)
	nextNode, err := synchronizer.GetNextNode(filenameHash, r.LocalMember.MembershipList)
	if err != nil {
		return nil, err
	}
	targetAddress := nextNode.Host
	fsmsg := utils.FileSystemMessage{
		Action:   "get",
		Filename: HyDFS_src_filename,
		Content:  nil,
	}
	reply, err := synchronizer.SendFileSystemMessage(targetAddress, fsmsg)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, fmt.Errorf(reply.Error)
	}
	return reply.Content, nil
}

func (r *RainStormMember) SendPartitionsToStage1(partitions []Partition) error {
	num_tasks := len(partitions)

	fmt.Printf("num_tasks=%d\n", num_tasks)
	for worker := 0; worker < num_tasks; worker++ {
		partition := partitions[worker]
		workerVmNum := (worker + 1) // start at VM 1, no such thing as VM 0
		fmt.Printf("workerVmNum=%d\n", workerVmNum)
		fmt.Printf("Connecting to %s at RainStormMember.ListenStage1 for partition.Offset=%d\n", GetHostname(workerVmNum), partition.Offset)
		ConnectToRPCWithTimeout(workerVmNum, partition, "RainStormMember.RPCListenStage1")
	}

	return nil
}

// func ConnectToRPCWithAttempts(vmNum int, input any, target string) {
// 	for attempt := 0; attempt < MAX_RPC_ATTEMPTS; attempt++ {
// 		workerHostname := GetHostname(vmNum)
// 		fmt.Printf("workerHostname=%s\n", workerHostname)
// 		address := fmt.Sprintf("%s:%d", workerHostname, PORT)
// 		fmt.Printf("rpc_address=%s\n", address)

// 		// conn, err := rpc.Dial("tcp", address)

// 		tmpConn, err := net.DialTimeout("tcp", address, RPC_TIMEOUT)
// 		client := rpc.NewClient(tmpConn)
// 		defer client.Close()
// 		if err != nil {
// 			fmt.Printf("(attempt=%d), failed to dial (%d: %s): %v\n", attempt, vmNum, workerHostname, err)
// 			continue
// 		}
// 		if client == nil {
// 			fmt.Printf("(attempt=%d), RPC connection is nil\n", attempt)
// 			continue
// 		}

// 		// go conn.Call(target, input, &reply) //replacing this

// 		done := make(chan error, 1)
// 		// call blocks, so we run it in a goroutine
// 		go func() {
// 			var reply bool
// 			err := client.Call(target, input, &reply)
// 			done <- err
// 		}()

// 		var success bool
// 		select {
// 		case err := <-done:
// 			if err != nil {
// 				fmt.Printf("(attempt=%d) RPC call error: %v\n", attempt, err)
// 				continue
// 			}
// 			// If we get here, the call succeeded
// 			success = true
// 		case <-time.After(RPC_TIMEOUT):
// 			fmt.Printf("(attempt=%d) RPC call timed out\n", attempt)
// 			success = false
// 		}
// 		if success {
// 			break // break out if the attempt was successful
// 		}
// 	}
// }

func ConnectToRPCWithTimeout(vmNum int, input any, target string) bool {
	workerHostname := GetHostname(vmNum)
	fmt.Printf("workerHostname=%s\n", workerHostname)
	address := fmt.Sprintf("%s:%d", workerHostname, PORT)
	fmt.Printf("rpc_address=%s\n", address)

	// conn, err := rpc.Dial("tcp", address)

	tmpConn, err := net.DialTimeout("tcp", address, RPC_TIMEOUT)
	if err != nil {
		fmt.Printf("failed to dial (%d: %s): %v\n", vmNum, workerHostname, err)
		return false
	}
	client := rpc.NewClient(tmpConn)
	defer client.Close()

	// go conn.Call(target, input, &reply) //replacing this

	done := make(chan error, 1)
	// call blocks, so we run it in a goroutine
	go func() {
		var reply bool
		err := client.Call(target, input, &reply)
		done <- err
	}()

	var success bool
	select {
	case err := <-done:
		if err != nil {
			fmt.Printf("RPC call error: %v\n", err)
			return false
		}
		// If we get here, the call succeeded
		success = true
	case <-time.After(RPC_TIMEOUT):
		fmt.Printf("RPC call timed out\n")
		success = false
	}
	return success
}

type STAGE_ENUM int

const (
	STAGE_1 STAGE_ENUM = iota + 1
	STAGE_2
	STAGE_3
)

type Batch struct {
	P               *Partition
	StartLineNumber int
	Data            [][]byte
	Stage2Data      [][]byte
	Stage3Data      []byte
	Stage           STAGE_ENUM
}

var VM_MAP = map[int]string{
	1:  "172.22.157.26",
	2:  "172.22.159.26",
	3:  "172.22.95.26",
	4:  "172.22.157.27",
	5:  "172.22.159.27",
	6:  "172.22.95.27",
	7:  "172.22.157.28",
	8:  "172.22.159.28",
	9:  "172.22.95.28",
	10: "172.22.157.29",
}

// RPC server for worker was created already
func (r *RainStormMember) RPCListenStage1(partition Partition, reply *bool) error {
	fmt.Printf("RPCListenStage1 at (%s) with partition=%+v\n", r.VMHostname, partition)
	srcContent, err := r.GetSourceContent(partition.SrcFile)
	if err != nil {
		fmt.Printf("failed to get source content (%s): %v\n", partition.SrcFile, err)
		return nil
	}
	// fmt.Printf("Hello4\n")
	srcContentLines := bytes.Split(srcContent, []byte("\n"))
	// Create batches and send them out
	// lineNumber holds the lineNumber from the original file
	for lineNumber := partition.Offset; lineNumber < partition.Offset+partition.Length; lineNumber += BATCH_SIZE {
		cur_batch_size := min(BATCH_SIZE, partition.Offset+partition.Length-lineNumber)
		batch := Batch{P: &partition, StartLineNumber: lineNumber, Data: srcContentLines[lineNumber : lineNumber+cur_batch_size], Stage: STAGE_1}
		// calc next vm
		try := 0
		nextVmNum := r.VMNum + partition.NumTasks
		for !r.IsInMembershipList(VM_MAP[nextVmNum]) && try < partition.NumTasks {
			fmt.Printf("nextVMNum=%d\n", nextVmNum)
			nextVmNum += 1
			if nextVmNum > partition.NumTasks*2 { // 2 bc stage 2
				nextVmNum = partition.NumTasks + 1
			}
			try += 1
		}
		fmt.Printf("nextVMNum=%d\n", nextVmNum)
		if try == partition.NumTasks {
			fmt.Printf("Failed to connect to any VM in next stage")
			return nil
		}

		fmt.Printf("Connecting to %s at RainStormMember.ListenStage2 for batch.StartLineNumber=%d\n", GetHostname(nextVmNum), batch.StartLineNumber)
		success := ConnectToRPCWithTimeout(nextVmNum, batch, "RainStormMember.RPCListenStage2")
		if !success {
			for change := 0; change < batch.P.NumTasks; change++ {
				nextVmNum += 1
				if nextVmNum > batch.P.NumTasks*2 { // 2 bc stage 2
					nextVmNum = batch.P.NumTasks*2 + 1
				}
				fmt.Printf("Connecting to %s at RainStormMember.ListenStage2 for batch.StartLineNumber=%d\n", GetHostname(nextVmNum), batch.StartLineNumber)
				success := ConnectToRPCWithTimeout(nextVmNum, batch, "RainStormMember.RPCListenStage2")
				if success {
					break
				}
			}
		}
	}
	return nil
}

func (r *RainStormMember) RPCListenStage2(batch Batch, reply *bool) error {
	fmt.Printf("RPCListenStage2 at (%s) with batch.StartLineNumber=%d\n", r.VMHostname, batch.StartLineNumber)
	// _, batch_seen := r.Worker_BatchesInProgress[batch.StartLineNumber]
	// if batch_seen {
	// 	fmt.Printf("Duplicate batch recieved \n")
	// 	return nil
	// }
	exec_filename := GetExecFilename(batch.P.Op1File)
	_, err := os.Stat(exec_filename)
	// check whether op1 file already exists on this VM
	if errors.Is(err, os.ErrNotExist) {
		// exec/{op1file} does not exist
		err := os.MkdirAll("exec", os.FileMode(0777))
		if err != nil {
			fmt.Printf("failed to create exec directory to hold file (%s): %v\n", exec_filename, err)
			return nil
		}
		f, err := os.Create(exec_filename)
		if err != nil {
			fmt.Printf("failed to create file (%s): %v\n", exec_filename, err)
			return nil
		}
		op1_bytes, err := r.GetSourceContent(batch.P.Op1File)
		if err != nil {
			fmt.Printf("failed to get op1 bytes from file (%s):%v\n", batch.P.Op1File, err)
			return nil
		}
		_, err = f.Write(op1_bytes)
		if err != nil {
			fmt.Printf("failed to write op1 bytes to file (%s): %v\n", exec_filename, err)
			return nil
		}
		err = f.Chmod(os.FileMode(0777)) // set it able to execute
		if err != nil {
			fmt.Println("Error changing file permissions:", err)
		}
		f.Close()
	}

	// Execute the command and capture the output
	// @TODO check that this works
	joined_batch_data := bytes.Join(batch.Data, []byte("\n"))
	input_arg := string(joined_batch_data[:]) // sending batch data as an argument so wrap in quotes
	fmt.Printf("Input_arg:\n%s\n", input_arg)
	cmd := exec.Command(fmt.Sprintf("./%s", exec_filename), input_arg)
	outputBytes, err := cmd.Output()
	if err != nil {
		fmt.Println("command failed:", err)
		return nil
	}
	fmt.Printf("Output:\n%s\n", string(outputBytes[:]))
	outputBytesLines := bytes.Split(outputBytes, []byte("\n"))
	batch.Stage = STAGE_2
	batch.Stage2Data = outputBytesLines

	// send to stage 3
	try := 0
	nextVmNum := r.VMNum + batch.P.NumTasks
	for !r.IsInMembershipList(VM_MAP[nextVmNum]) && try < batch.P.NumTasks {
		// temp = all batches in batches_in_progress[nextVmNum], del batches_in_progress[nextVmNum], batches_in_progress[ACTUAL nextVmNum].extend(temp)
		fmt.Printf("nextVMNum=%d\n", nextVmNum)
		nextVmNum += 1
		if nextVmNum > batch.P.NumTasks*3 { // 3 bc stage 3
			nextVmNum = batch.P.NumTasks*2 + 1
		}
		try += 1
	}
	fmt.Printf("nextVMNum=%d\n", nextVmNum)
	if try == batch.P.NumTasks {
		fmt.Printf("Failed to connect to any VM in next stage")
		return nil
	}

	fmt.Printf("Connecting to %s at RainStormMember.ListenStage3 for batch.StartLineNumber=%d\n", GetHostname(nextVmNum), batch.StartLineNumber)
	// r.Worker_BatchesInProgress[batch.StartLineNumber] = nextVmNum
	success := ConnectToRPCWithTimeout(nextVmNum, batch, "RainStormMember.RPCListenStage3")
	if !success {
		// delete(r.Worker_BatchesInProgress, batch.StartLineNumber)
		// r.RPCListenStage2(batch, reply)
		for change := 0; change < batch.P.NumTasks; change++ {
			nextVmNum += 1
			if nextVmNum > batch.P.NumTasks*3 { // 3 bc stage 3
				nextVmNum = batch.P.NumTasks*2 + 1
			}
			fmt.Printf("Connecting to %s at RainStormMember.ListenStage3 for batch.StartLineNumber=%d\n", GetHostname(nextVmNum), batch.StartLineNumber)
			success := ConnectToRPCWithTimeout(nextVmNum, batch, "RainStormMember.RPCListenStage3")
			if success {
				break
			}
		}
	}
	// r.Worker_BatchesInProgress[batch.StartLineNumber] = -1
	return nil
}

func (r *RainStormMember) RPCListenStage3(batch Batch, reply *bool) error {
	fmt.Printf("RPCListenStage3 at (%s) with batch.StartLineNumber=%d\n", r.VMHostname, batch.StartLineNumber)
	// _, batch_seen := r.Worker_BatchesInProgress[batch.StartLineNumber]
	// if batch_seen {
	// 	fmt.Printf("Duplicate batch recieved \n")
	// 	return nil
	// }
	exec_filename := GetExecFilename(batch.P.Op2File)
	_, err := os.Stat(exec_filename)
	// check whether op1 file already exists on this VM
	if errors.Is(err, os.ErrNotExist) {
		// exec/{op1file} does not exist
		err := os.MkdirAll("exec", os.FileMode(0777))
		if err != nil {
			fmt.Printf("failed to create exec directory to hold file (%s): %v\n", exec_filename, err)
			return nil
		}
		f, err := os.Create(exec_filename)
		if err != nil {
			fmt.Printf("failed to create file (%s): %v\n", exec_filename, err)
			return nil
		}
		op2_bytes, err := r.GetSourceContent(batch.P.Op2File)
		if err != nil {
			fmt.Printf("failed to get op2 bytes from file (%s):%v\n", batch.P.Op2File, err)
			return nil
		}
		_, err = f.Write(op2_bytes)
		if err != nil {
			fmt.Printf("failed to write op2 bytes to file (%s): %v\n", exec_filename, err)
			return nil
		}
		err = f.Chmod(os.FileMode(0777)) // set it able to execute
		if err != nil {
			fmt.Println("Error changing file permissions:", err)
		}
		f.Close()
	}

	// Execute the command and capture the output
	// @TODO check that this works
	joined_batch_data := bytes.Join(batch.Stage2Data, []byte("\n"))
	input_arg := string(joined_batch_data[:]) // sending batch data as an argument so wrap in quotes
	fmt.Printf("Input_arg:\n%s\n", input_arg)
	cmd := exec.Command(fmt.Sprintf("./%s", exec_filename), input_arg)
	outputBytes, err := cmd.Output()
	if err != nil {
		fmt.Println("command failed:", err)
		return nil
	}
	fmt.Printf("Output:\n%s\n", string(outputBytes[:]))
	// outputBytesLines := bytes.Split(outputBytes, []byte("\n"))
	batch.Stage = STAGE_3
	batch.Stage3Data = outputBytes

	// send to leader
	leaderVmNum := 10

	fmt.Printf("Connecting to %s at RainStormMember.ListenOutputAtLeader for batch.StartLineNumber=%d\n", GetHostname(leaderVmNum), batch.StartLineNumber)
	// r.Worker_BatchesInProgress[batch.StartLineNumber] = leaderVmNum
	success := ConnectToRPCWithTimeout(leaderVmNum, batch, "RainStormMember.ListenOutputAtLeader")
	if !success {
		// delete(r.Worker_BatchesInProgress, batch.StartLineNumber)
		r.RPCListenStage3(batch, reply)
	}
	// r.Worker_BatchesInProgress[batch.StartLineNumber] = -1
	return nil
}

// What we just did:
// 		modified op2_t1 and op2_t2 to return JSONs - compile them, check they work, run the system so far.
// TODO
// make RainStormMember.ListenOutputAtLeader
// 	it should: 	check whether batch already happened -> use UploadedBatchLineNumbers in RainstormMember to keep track
// 				find type of output -> parse into JSON, find "type" field. If aggregate, keep running count. If transform, keep separate.
// 				write to the dest HDFS file -> write using copied func from user action info to append to file
// 				print to console -> normal print
// recover batches that died on failed VM
// 	use batches_in_progress (check whiteboard) at each stage

type Record struct {
	Type string `json:"Type"`
	Data string `json:"Data"`
}

func (r *RainStormMember) ListenOutputAtLeader(batch Batch, reply *bool) error {
	r.Mu_UploadedBatchLineNumbers.Lock()
	defer r.Mu_UploadedBatchLineNumbers.Unlock()
	_, contains_batch := r.UploadedBatchLineNumbers[batch.StartLineNumber]
	if contains_batch {
		*reply = false
		return nil
	}
	output := Record{}
	if err := json.Unmarshal(batch.Stage3Data, &output); err != nil {
		return fmt.Errorf("failed to parse batch data: %v", err)
	}
	var result string
	if output.Type == "Aggregate" {
		var aggregateOutput map[string]int
		err := json.Unmarshal([]byte(output.Data), &aggregateOutput)
		if err != nil {
			return fmt.Errorf("failed to parse batch data: %v", err)
		}
		for category, count := range aggregateOutput {
			r.AggregateCounts[category] += count
		}
		result = fmt.Sprintf("%v", r.AggregateCounts)
	} else if output.Type == "Transform" {
		result = strings.TrimSpace(output.Data)
	}
	// print result to console
	fmt.Println(result)
	// save result to hdfs file
	r.Append([]byte(result+"\n"), batch.P.DstFile)

	// LOG (TODO REMOVE)
	fmt.Printf("Number of received batches: %d \n", len(r.UploadedBatchLineNumbers))

	// mark batch as seen
	r.UploadedBatchLineNumbers[batch.StartLineNumber] = true

	return nil
}

func GetExecFilename(hdfs_filename string) string {
	return "exec/" + hdfs_filename
}
func (r *RainStormMember) IsInMembershipList(vmAddress string) bool {
	for _, memId := range r.LocalMember.MembershipList {
		if memId.Host == vmAddress {
			return true
		}
	}
	return false
}

// copied func from user action info
func (r *RainStormMember) Append(content []byte, HyDFSfilename string) error {
	// fmt.Printf("LOG: Append content to HyDFS(%s): Started \n", HyDFSfilename)

	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := synchronizer.GetNextThreeCorrespondingNodes(filenameHash, r.LocalMember.MembershipList)
	if err != nil {
		return err
	}
	// send RPC msg
	ts := time.Now().UnixNano()
	for _, node := range nextThreeNodes {
		fsmsg := utils.FileSystemMessage{
			Action:          "append",
			Filename:        HyDFSfilename,
			AppendTimestamp: ts,
			Content:         content,
		}
		// send {action, filename, hash, isLeader, content} to node
		reply, err := synchronizer.SendFileSystemMessage(node.Host, fsmsg)
		if err != nil {
			return err
		}
		if reply.Error != "" {
			return fmt.Errorf(reply.Error)
		}
	}
	// fmt.Printf("LOG: Append content to HyDFS(%s): Finished \n", HyDFSfilename)

	return nil
}

// Done So Far
// System seems to work with failure detection and batch upload
// Automated tests seems to work with SIGINT
//TODO
// Test all 4 tests
// Do Report
// Get Data
