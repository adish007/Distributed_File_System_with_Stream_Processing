package user

func (u *UserActionInfo) RainstormCmd(op1_exe string, op2_exe string, HyDFS_src_filename string, HyDFS_dest_filename string, num_tasks int) error {
	return u.RainStormMember.RainstormCmd(op1_exe, op2_exe, HyDFS_src_filename, HyDFS_dest_filename, num_tasks)
}

// func Partition(srcContent []byte, num_tasks int) map[string][]byte {
// 	partition_size := int(math.Ceil(float64(len(srcContent)) / float64(num_tasks)))
// 	input_kv := make(map[string][]byte)

// 	for i := 0; i < num_tasks; i++ {
// 		start := i * partition_size
// 		end := (i + 1) * partition_size

// 		if end > len(srcContent) {
// 			end = len(srcContent)
// 		}
// 		partition := srcContent[start:end]
// 		input_kv[fmt.Sprintf("%d", i)] = partition
// 	}
// 	return input_kv
// }

// func (u *UserActionInfo) CommunicateWithWorkers(input_kv map[string][]byte, op_bytes_list [][]byte) error {
// 	// create file for each k:v on its corresponding (ring-hash-based) node with name {keyHash}
// 	for key, value := range input_kv {
// 		keyHash := utils.ComputeHash(key)
// 		nextNode, err := synchronizer.GetNextNode(keyHash, u.LocalMember.MembershipList)
// 		if err != nil {
// 			return err
// 		}
// 		targetAddress := nextNode.Host

// 		fsmsg := utils.FileSystemMessage{
// 			Action:      "createWExec",
// 			Filename:    fmt.Sprintf("%d", keyHash),
// 			Content:     value,
// 			Executables: op_bytes_list,
// 		}
// 		reply, err := synchronizer.SendFileSystemMessage(targetAddress, fsmsg)
// 		if err != nil {
// 			return err
// 		}
// 		if reply.Error != "" {
// 			return fmt.Errorf(reply.Error)
// 		}
// 	}

// 	return nil
// }

// func CollectOutputs() {

// }
