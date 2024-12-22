package synchronizer

import (
	"cmp"
	"encoding/json"
	"fmt"
	"g85/mp4_again_again/localstorage"
	"g85/mp4_again_again/ring/member"
	"g85/mp4_again_again/utils"
	"net/rpc"
	"slices"
)

var MANUALLY_SET_PORT_LOCAL_STORAGE = 1114

func SendFileSystemMessage(host string, input utils.FileSystemMessage) (*utils.FileSystemReply, error) {
	address := fmt.Sprintf("%s:%d", host, MANUALLY_SET_PORT_LOCAL_STORAGE)
	conn, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %v", err)
	}
	if conn == nil {
		return nil, fmt.Errorf("RPC connection is nil")
	}

	var reply utils.FileSystemReply
	// fmt.Printf("Sending %s for %s into LocalStorage.RPCRecieve\n", input.Action, input.Filename)

	// fmt.Println("Action:", input.Action)
	// fmt.Println("Filename:", input.Filename)
	// fmt.Printf("Content: %s\n", input.Content)

	err = conn.Call("LocalStorage.RPCRecieve", input, &reply)
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %v", err)
	}
	return &reply, nil
}

func GetNextNode(filenameHash uint32, sortedHashList []member.MemberID) (member.MemberID, error) {
	n := len(sortedHashList)
	if n < 1 {
		return member.MemberID{}, fmt.Errorf("less than three nodes")
	}
	first_matching_node_i := 0
	for i, node := range sortedHashList {
		if node.HostHash >= filenameHash {
			first_matching_node_i = i
			break
		}
	}
	return sortedHashList[first_matching_node_i], nil
}

func GetNextThreeCorrespondingNodes(filenameHash uint32, sortedHashList []member.MemberID) ([]member.MemberID, error) {
	n := len(sortedHashList)
	if n < 3 {
		return nil, fmt.Errorf("less than three nodes")
	}
	first_matching_node_i := 0
	for i, node := range sortedHashList {
		if node.HostHash >= filenameHash {
			first_matching_node_i = i
			break
		}
	}
	return []member.MemberID{
		sortedHashList[utils.Mod(first_matching_node_i, n)],
		sortedHashList[utils.Mod(1+first_matching_node_i, n)],
		sortedHashList[utils.Mod(2+first_matching_node_i, n)],
	}, nil
}

// This merges all the files, is run after message is recieved with a different file than the one that is held locally
func Merge(HyDFSfilename string, membershipList []member.MemberID) error {
	//Get three replicas first
	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := GetNextThreeCorrespondingNodes(filenameHash, membershipList)
	if err != nil {
		return err
	}
	// Timestamp to append info, relies on timestamps being unique for each append
	allAppendInfosMap := make(map[int64]localstorage.AppendInfo)
	for _, node := range nextThreeNodes {
		fsmsg := utils.FileSystemMessage{
			Action:   "getRemoveAppendInfos",
			Filename: HyDFSfilename,
			Content:  nil,
		}
		// send {action, filename, hash, isLeader, content} to node
		reply, err := SendFileSystemMessage(node.Host, fsmsg)
		if err != nil {
			return err
		}
		if reply.Error != "" {
			return fmt.Errorf(reply.Error)
		}
		var appendInfos []localstorage.AppendInfo
		json.Unmarshal([]byte(reply.Content), &appendInfos)
		// this node's appendInfos are here
		// allAppendInfos = append(allAppendInfos, appendInfos...)
		for _, appendInfo := range appendInfos {
			allAppendInfosMap[appendInfo.Timestamp] = appendInfo
		}
	}
	var allAppendInfos []localstorage.AppendInfo
	// fmt.Println("Unique Append Infos")
	for _, ai := range allAppendInfosMap {
		// fmt.Printf("%d: %v \n", ts, ai.Content)
		allAppendInfos = append(allAppendInfos, ai)
	}

	allAppendInfos = SortAppendInfosByTimestamp(allAppendInfos)

	var overallAppend []byte
	for _, appendInfo := range allAppendInfos {
		overallAppend = append(overallAppend, appendInfo.Content...)
	}

	for _, node := range nextThreeNodes {
		fsmsg := utils.FileSystemMessage{
			Action:   "rawAppend",
			Filename: HyDFSfilename,
			Content:  overallAppend,
		}
		// send {action, filename, hash, isLeader, content} to node
		reply, err := SendFileSystemMessage(node.Host, fsmsg)
		if err != nil {
			return err
		}
		if reply.Error != "" {
			return fmt.Errorf(reply.Error)
		}
	}

	return nil
}

func SortAppendInfosByTimestamp(allAppendInfos []localstorage.AppendInfo) []localstorage.AppendInfo {
	timestampCmp := func(a, b localstorage.AppendInfo) int {
		return cmp.Compare(a.Timestamp, b.Timestamp)
	}
	slices.SortFunc(allAppendInfos, timestampCmp)
	return allAppendInfos
}
