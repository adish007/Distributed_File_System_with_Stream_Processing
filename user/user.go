package user

import (
	"bufio"
	"encoding/json"
	"fmt"
	"g85/mp4_again_again/cache"
	"g85/mp4_again_again/localstorage"
	"g85/mp4_again_again/rainstorm"
	"g85/mp4_again_again/ring/member"
	"g85/mp4_again_again/synchronizer"
	"g85/mp4_again_again/utils"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var USE_CACHE = false

type UserActionInfo struct {
	LocalMember     *member.Member
	RainStormMember *rainstorm.RainStormMember
	LocalCache      cache.Cache
}

func (u *UserActionInfo) Create(sourceLocalfilename string, HyDFSfilename string) error {
	fmt.Printf("LOG: Create from local (%s) to HyDFS (%s): Started \n", sourceLocalfilename, HyDFSfilename)
	// get content of file
	// content, err := os.ReadFile(sourceLocalfilename)
	// if err != nil {
	// 	return fmt.Errorf("couldn't find source: %s", sourceLocalfilename)
	// }
	file, err := os.Open(sourceLocalfilename)
	if err != nil {
		return fmt.Errorf("couldn't find source: %s", sourceLocalfilename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("couldn't read content: %v", err)
	}

	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := synchronizer.GetNextThreeCorrespondingNodes(filenameHash, u.LocalMember.MembershipList)
	if err != nil {
		return err
	}
	// send RPC msg {isLeader bool, HyDFSfilename string, contents byte[ ]} to next three nodes
	// 		note: isLeader is true for first, false for next two
	// 			  replies should be bools of whether it succeeded or not
	for _, node := range nextThreeNodes {
		fsmsg := utils.FileSystemMessage{
			Action:   "create",
			Filename: HyDFSfilename,
			Content:  content,
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
	fmt.Printf("LOG: Create from local (%s) to HyDFS (%s): Finished \n", sourceLocalfilename, HyDFSfilename)
	return nil
}

func (u *UserActionInfo) Get(HyDFSfilename string, targetLocalfilename string) error {
	fmt.Printf("LOG: Get from HyDFS (%s) to local (%s): Started \n", HyDFSfilename, targetLocalfilename)
	if USE_CACHE {
		cacheContent := u.LocalCache.RetrieveFromCache(HyDFSfilename)
		if cacheContent != nil {
			f, err := os.Create(targetLocalfilename)
			if err != nil {
				return fmt.Errorf("failed to create file (%s): %v", targetLocalfilename, err)
			}
			defer f.Close()
			_, err = f.Write(cacheContent)
			if err != nil {
				return fmt.Errorf("failed to write to file (%s): %v", targetLocalfilename, err)
			}
			// fmt.Printf("LOG: [CACHE] Get from HyDFS (%s) to local (%s): Finished \n", HyDFSfilename, targetLocalfilename)
			return nil
		}
	}
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextNode, err := synchronizer.GetNextNode(filenameHash, u.LocalMember.MembershipList)
	if err != nil {
		return err
	}
	res := u.GetFromReplica(nextNode.Host, HyDFSfilename, targetLocalfilename)
	fmt.Printf("LOG: Get from HyDFS (%s) to local (%s): Finished \n", HyDFSfilename, targetLocalfilename)
	return res
}

func (u *UserActionInfo) GetFromReplica(VMaddress string, HyDFSfilename string, targetLocalfilename string) error {
	fmt.Printf("LOG: Get from Replica (%s) from HyDFS (%s) to local (%s): Started \n", VMaddress, HyDFSfilename, targetLocalfilename)

	synchronizer.Merge(HyDFSfilename, u.LocalMember.MembershipList)
	// calculate hash of HyDFSfilename
	// filenameHash := utils.ComputeHash(HyDFSfilename)
	fsmsg := utils.FileSystemMessage{
		Action:   "get",
		Filename: HyDFSfilename,
		Content:  nil,
	}
	// send {action, filename, hash, isLeader, content} to node
	reply, err := synchronizer.SendFileSystemMessage(VMaddress, fsmsg)
	if err != nil {
		return err
	}
	if reply.Error != "" {
		return fmt.Errorf(reply.Error)
	}

	// Write using reply.Content
	f, err := os.Create(targetLocalfilename)
	if err != nil {
		return fmt.Errorf("failed to create file (%s): %v", targetLocalfilename, err)
	}
	defer f.Close()
	_, err = f.Write(reply.Content)
	if err != nil {
		return fmt.Errorf("failed to write to file (%s): %v", targetLocalfilename, err)
	}

	if USE_CACHE {
		u.LocalCache.AddToCache(HyDFSfilename, reply.Content)
	}

	fmt.Printf("LOG: Get from Replica (%s) from HyDFS (%s) to local (%s): Finished \n", VMaddress, HyDFSfilename, targetLocalfilename)
	return nil
}

// NOTE: copied from "Create" with only msg.action changed
func (u *UserActionInfo) Append(sourceLocalfilename string, HyDFSfilename string) error {
	fmt.Printf("LOG: Append from source (%s) to HyDFS(%s): Started \n", sourceLocalfilename, HyDFSfilename)

	// get content of file
	content, err := os.ReadFile(sourceLocalfilename)
	if err != nil {
		return fmt.Errorf("couldn't find source: %s", sourceLocalfilename)
	}
	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := synchronizer.GetNextThreeCorrespondingNodes(filenameHash, u.LocalMember.MembershipList)
	if err != nil {
		return err
	}
	// send RPC msg {isLeader bool, HyDFSfilename string, contents byte[ ]} to next three nodes
	// 		note: isLeader is true for first, false for next two
	// 			  replies should be bools of whether it succeeded or not
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
	fmt.Printf("LOG: Append from source (%s) to HyDFS(%s): Finished \n", sourceLocalfilename, HyDFSfilename)

	return nil
}

func (u *UserActionInfo) Merge(HyDFSfilename string) error {
	start_ts := time.Now().UnixNano()
	fmt.Printf("LOG: Merge for (%s) : Started \n", HyDFSfilename)
	res := synchronizer.Merge(HyDFSfilename, u.LocalMember.MembershipList)
	fmt.Printf("LOG: Merge for (%s) : Finished \n", HyDFSfilename)
	end_ts := time.Now().UnixNano()
	fmt.Printf("LOG: Merge for (%s) : Duration: %.2f seconds\n", HyDFSfilename, float64(end_ts-start_ts)/1e9)
	return res
}

func (u *UserActionInfo) Ls(HyDFSfilename string) error {
	fmt.Printf("LOG: Ls for (%s): Started \n", HyDFSfilename)

	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := synchronizer.GetNextThreeCorrespondingNodes(filenameHash, u.LocalMember.MembershipList)
	fmt.Printf("%s is present at: ", HyDFSfilename)
	for _, node := range nextThreeNodes {
		fmt.Printf("%s, ", node.Host)
	}
	fmt.Printf("\n")

	fmt.Printf("LOG: Ls for (%s): Finished \n", HyDFSfilename)
	return err
}

func (u *UserActionInfo) Store() error {
	fmt.Printf("LOG: Store: Started \n")

	var filenames []string
	fsmsg := utils.FileSystemMessage{
		Action:   "listFiles",
		Filename: "",
		Content:  nil,
	}
	reply, err := synchronizer.SendFileSystemMessage(u.LocalMember.ID.Host, fsmsg)
	if err != nil {
		return err
	}
	if reply.Error != "" {
		return fmt.Errorf(reply.Error)
	}
	json.Unmarshal([]byte(reply.Content), &filenames)

	fmt.Println("Files in 'localfiles' directory:", filenames)

	fmt.Printf("LOG: Store: Finished \n")
	return nil
}

func (u *UserActionInfo) ListMemIds() error {
	fmt.Printf("LOG: ListMemIds: Started \n")

	for _, member_id := range u.LocalMember.MembershipList {
		fmt.Printf("HostHash %d: {Host %s, Port: %d, Timestamp: %d}\n", member_id.HostHash, member_id.Host, member_id.Port, member_id.Timestamp)
	}

	fmt.Printf("LOG: ListMemIds: Finished \n")
	return nil
}

// Keep listening in terminal for input
func (u *UserActionInfo) ListenForInput() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Fprintf(os.Stderr, "Enter option: \n")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		u.HandleTerminalInput(input)
	}
}

func (u *UserActionInfo) Multiappend(HyDFSfilename string, ListofVMs []string, sourceLocalfilenames []string) error {
	fmt.Printf("LOG: Multiappend: Started \n")

	n := len(ListofVMs)
	if len(sourceLocalfilenames) != n {
		return fmt.Errorf("bad number of source filenames")
	}
	for i := 0; i < n; i++ {
		err := u.multiappendHelper(HyDFSfilename, ListofVMs[i], sourceLocalfilenames[i])
		if err != nil {
			return err
		}
	}
	fmt.Printf("LOG: Multiappend to HyDFS(%s): Finished \n", HyDFSfilename)
	return nil
}

func (u *UserActionInfo) multiappendHelper(HyDFSfilename string, VM string, sourceLocalfilename string) error {
	// get
	relativePath := fmt.Sprintf("../%s", sourceLocalfilename)
	fsmsg_get := utils.FileSystemMessage{
		Action:   "get",
		Filename: relativePath,
		Content:  nil,
	}
	// send {action, filename, hash, isLeader, content} to node
	reply, err := synchronizer.SendFileSystemMessage(VM, fsmsg_get)
	if err != nil {
		return err
	}
	if reply.Error != "" {
		return fmt.Errorf(reply.Error)
	}
	content := reply.Content
	// append
	// calculate hash of HyDFSfilename
	filenameHash := utils.ComputeHash(HyDFSfilename)
	// get MemberID[ ] of next three nodes
	nextThreeNodes, err := synchronizer.GetNextThreeCorrespondingNodes(filenameHash, u.LocalMember.MembershipList)
	if err != nil {
		return err
	}
	ts := time.Now().UnixNano()
	for _, node := range nextThreeNodes {
		fsmsg := utils.FileSystemMessage{
			Action:          "append",
			Filename:        HyDFSfilename,
			AppendTimestamp: ts,
			Content:         content,
		}
		reply, err := synchronizer.SendFileSystemMessage(node.Host, fsmsg)
		if err != nil {
			return err
		}
		if reply.Error != "" {
			return fmt.Errorf(reply.Error)
		}
	}
	fmt.Printf("LOG: Multiappend from source (%s) to HyDFS(%s): Finished \n", sourceLocalfilename, HyDFSfilename)

	return nil
}

// Handle the different possible terminal inputs
func (u *UserActionInfo) HandleTerminalInput(input string) {
	fmt.Printf("Recieved input: %s \n", input)
	inputArgs := strings.Fields(input)
	if len(inputArgs) == 0 {
		fmt.Printf("Bad input: No input arguments\n")
		return
	}
	var err error
	switch inputArgs[0] {
	case "status_sus":
		if len(inputArgs) != 1 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		fmt.Printf("Suspicion status: %t\n", member.SUS_ACTIVATED)
	case "create":
		if len(inputArgs) != 3 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Create(inputArgs[1], inputArgs[2])
	case "get":
		if len(inputArgs) != 3 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Get(inputArgs[1], inputArgs[2])
	case "append":
		if len(inputArgs) != 3 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Append(inputArgs[1], inputArgs[2])
	case "merge":
		if len(inputArgs) != 2 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Merge(inputArgs[1])
	case "ls":
		if len(inputArgs) != 2 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Ls(inputArgs[1])
	case "store":
		if len(inputArgs) != 1 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.Store()
	case "getfromreplica":
		if len(inputArgs) != 4 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.GetFromReplica(inputArgs[1], inputArgs[2], inputArgs[3])
	case "list_mem_ids":
		if len(inputArgs) != 1 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		err = u.ListMemIds()
	case "multiappend":
		if (len(inputArgs) < 2) || (len(inputArgs)%2 != 0) {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		var VMList []string
		var filenameList []string
		hydfsFilename := inputArgs[1]
		n := len(inputArgs)
		numVms := (n - 2) / 2
		for i := 0; i < numVms; i++ {
			VMList = append(VMList, inputArgs[2+i])
		}
		for i := 0; i < numVms; i++ {
			filenameList = append(filenameList, inputArgs[2+numVms+i])
		}
		err = u.Multiappend(hydfsFilename, VMList, filenameList)

	case "test1_part1":
		if len(inputArgs) != 6 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test1_part1(inputArgs[1], inputArgs[2], inputArgs[3], inputArgs[4], inputArgs[5])
	case "test1_part2":
		if len(inputArgs) != 2 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test1_part2(inputArgs[1])
	case "test2":
		if len(inputArgs) != 2 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test2(inputArgs[1])
	case "test3":
		if len(inputArgs) != 2 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test3(inputArgs[1])

	case "test4":
		if len(inputArgs) != 4 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test4(inputArgs[1], inputArgs[2], inputArgs[3])
	case "test5_part1":
		if len(inputArgs) != 10 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test5_part1(inputArgs[1], inputArgs[2], inputArgs[3],
			inputArgs[4], inputArgs[5], inputArgs[6],
			inputArgs[7], inputArgs[8], inputArgs[9])
	case "test5_part2":
		if len(inputArgs) != 4 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		u.test5_part2(inputArgs[1], inputArgs[2], inputArgs[3])
	case "exp2_append":
		if len(inputArgs) != 4 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		hydfsFilename := inputArgs[1]
		append_filename := inputArgs[2]
		var num_concurrent_vms int
		num_concurrent_vms, err = strconv.Atoi(inputArgs[3])
		if err != nil {
			break
		}
		VMlist := []string{}
		filenameList := []string{}

		for i := 0; i < 1000; i++ {
			VMlist = append(VMlist, VM_MAP[fmt.Sprintf("VM%d", (i%num_concurrent_vms)+1)])
			filenameList = append(filenameList, append_filename)
		}

		err = u.Multiappend(hydfsFilename, VMlist, filenameList)
	case "RainStorm":
		if len(inputArgs) != 6 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		op1_exe := inputArgs[1]
		op2_exe := inputArgs[2]
		src_csv := inputArgs[3]
		dest_file := inputArgs[4]
		var num_tasks int
		num_tasks, err = strconv.Atoi(inputArgs[5])
		if err != nil {
			fmt.Println("Error converting string to int:", err)
			break
		}
		err = u.RainstormCmd(op1_exe, op2_exe, src_csv, dest_file, num_tasks)
	case "test_mp4":
		if len(inputArgs) != 8 {
			fmt.Printf("Incorrect number of arguments\n")
			return
		}
		op1_exe := inputArgs[1]
		op2_exe := inputArgs[2]
		src_csv := inputArgs[3]
		dest_file := inputArgs[4]
		var num_tasks int
		num_tasks, err = strconv.Atoi(inputArgs[5])
		if err != nil {
			fmt.Println("Error converting string to int:", err)
			break
		}
		var VM1 int
		VM1, err = strconv.Atoi(inputArgs[6])
		if err != nil {
			fmt.Println("Error converting string to int:", err)
			break
		}
		var VM2 int
		VM2, err = strconv.Atoi(inputArgs[7])
		if err != nil {
			fmt.Println("Error converting string to int:", err)
			break
		}
		u.test_MP4(op1_exe, op2_exe, src_csv, dest_file, num_tasks, VM1, VM2)
	case "setup_mp4":
		err = u.Create("op1_t1", "h_op1_t1")
		if err != nil {
			fmt.Printf("Error in h_op1_t2 creation: %v\n", err)
		}
		err = u.Create("op2_t1", "h_op2_t1")
		if err != nil {
			fmt.Printf("Error in h_op2_t2 creation: %v", err)
		}
		err = u.Create("op1_t2", "h_op1_t2")
		if err != nil {
			fmt.Printf("Error in h_op1_t2 creation: %v\n", err)
		}
		err = u.Create("op2_t2", "h_op2_t2")
		if err != nil {
			fmt.Printf("Error in h_op2_t2 creation: %v", err)
		}
		err = u.Create("Traffic_Signs_1000.csv", "h_Traffic_Signs_1000.csv")
		if err != nil {
			fmt.Printf("Error in h_Traffic_Signs_1000.csv creation: %v", err)
		}
		err = u.Create("Traffic_Signs_5000.csv", "h_Traffic_Signs_5000.csv")
		if err != nil {
			fmt.Printf("Error in h_Traffic_Signs_5000.csv creation: %v\n", err)
		}
		err = u.Create("Traffic_Signs_10000.csv", "h_Traffic_Signs_10000.csv")
		if err != nil {
			fmt.Printf("Error in h_Traffic_Signs_10000.csv creation: %v\n", err)
		}
		err = u.Create("test_dst.txt", "h_test_dst_t1.txt")
		if err != nil {
			fmt.Printf("Error in h_test_dst_t1.txt creation: %v\n", err)
		}
		err = u.Create("test_dst.txt", "h_test_dst_t2.txt")
		if err != nil {
			fmt.Printf("Error in h_test_dst_t2.txt creation: %v\n", err)
		}
		err = u.Create("test_dst.txt", "h_test_dst_t3.txt")
		if err != nil {
			fmt.Printf("Error in h_test_dst_t3.txt creation: %v\n", err)
		}
		fmt.Printf("Setup finished\n")
	}

	if err != nil {
		fmt.Printf("%s: %v\n", inputArgs[0], err)
	}

}

func (u *UserActionInfo) ContinuouslyPullFromPreds() {
	for {
		// Every 10 seconds
		time.Sleep(10 * time.Second)
		// get membership list
		membershipList := u.LocalMember.MembershipList
		// get ind of this node
		this_node_ind := 0
		for i, node := range membershipList {
			if node == u.LocalMember.ID {
				this_node_ind = i
				break
			}
		}
		// get previous node
		prev_ind := utils.Mod(this_node_ind-1, len(membershipList))
		if prev_ind == this_node_ind {
			continue
		}
		prevNode := membershipList[prev_ind]
		// pull
		err := u.PullFromNode(prevNode)
		if err != nil {
			fmt.Println("Failed to pull from previous node: ", err)
		}
		// get previous previous node
		prevprev_ind := utils.Mod(this_node_ind-2, len(membershipList))
		if prevprev_ind == this_node_ind {
			continue
		}
		prevprevNode := membershipList[prevprev_ind]
		// pull
		err = u.PullFromNode(prevprevNode)
		if err != nil {
			fmt.Println("Failed to pull from previous previous node: ", err)
		}
		fmt.Println("Pulled from predecessors")
	}
}

func (u *UserActionInfo) PullFromNode(m member.MemberID) error {
	// get local filenames
	var localFilenames []string
	fsmsg := utils.FileSystemMessage{
		Action:   "listFiles",
		Filename: "",
		Content:  nil,
	}
	reply, err := synchronizer.SendFileSystemMessage(u.LocalMember.ID.Host, fsmsg)
	if err != nil {
		return fmt.Errorf("failed to get local files: %v", err)
	}
	if reply.Error != "" {
		return fmt.Errorf("failed to get local files: %s", reply.Error)
	}
	json.Unmarshal([]byte(reply.Content), &localFilenames)

	// get hashset of local filenames
	localFilenamesMap := make(map[string]bool)
	for _, filename := range localFilenames {
		localFilenamesMap[filename] = true
	}
	// request all filenames from m where m is the leader (calculated based on m's files)
	leaderFilenames, err := GetLeaderFilenames(m.Host, u.LocalMember.MembershipList)
	if err != nil {
		return fmt.Errorf("couldn't get leader filenames: %v", err)
	}
	for _, filename := range leaderFilenames {
		if _, ok := localFilenamesMap[filename]; !ok {
			// doesn't exist locally, add it
			err := u.GetFromReplica(m.Host, filename, localstorage.GetLocalFilename(filename))
			if err != nil {
				return err
			}
		}
	}
	fmt.Printf("Pulled from %s: %s\n", m.Host, strings.Join(leaderFilenames, ","))
	return nil
}

func GetLeaderFilenames(host string, membershipList []member.MemberID) ([]string, error) {
	var allFilenames []string
	fsmsg := utils.FileSystemMessage{
		Action:   "listFiles",
		Filename: "",
		Content:  nil,
	}
	reply, err := synchronizer.SendFileSystemMessage(host, fsmsg)
	if err != nil {
		return nil, fmt.Errorf("failed to get local files: %v", err)
	}
	if reply.Error != "" {
		return nil, fmt.Errorf("failed to get local files: %s", reply.Error)
	}
	json.Unmarshal([]byte(reply.Content), &allFilenames)

	var leaderFilenames []string
	for _, filename := range allFilenames {
		filenameHash := utils.ComputeHash(filename)
		nextNode, err := synchronizer.GetNextNode(filenameHash, membershipList)
		if err != nil {
			return nil, err
		}
		if host == nextNode.Host {
			leaderFilenames = append(leaderFilenames, filename)
		}
	}

	return leaderFilenames, nil
}
