package user

import (
	"fmt"
	"g85/mp4_again_again/synchronizer"
	"g85/mp4_again_again/utils"
	"time"
)

func hdfs_fn(fn string) string {
	return fmt.Sprintf("hdfs_%s", fn[5:])
}

func retrieved_fn(fn string) string {
	return fmt.Sprintf("retrieved_%s", fn[5:])
}

// test1_part1 data/business_1.txt data/business_2.txt data/business_3.txt data/business_4.txt data/business_5.txt
func (u *UserActionInfo) test1_part1(f1, f2, f3, f4, f5 string) {
	fs := []string{f1, f2, f3, f4, f5}
	for _, f := range fs {
		err := u.Create(f, hdfs_fn(f))
		if err != nil {
			fmt.Printf("Failed to create file (%s): %v\n", f, err)
			return
		}
		time.Sleep(1 * time.Second)
	}
	// wait for create-acks
}

// * Run this from a different VM
// test1_part2 hdfs_data/business_1.txt
// * more retrieved_hdfs_data/business_1.txt hdfs_data/business_1.txt
// * diff retrieved_hdfs_data/business_1.txt hdfs_data/business_1.txt
func (u *UserActionInfo) test1_part2(HyDFS_filename string) {
	// get 1 of the files from any chosen VM
	// creates retrieved_hdfs_data/business_i.txt
	err := u.Get(HyDFS_filename, retrieved_fn(HyDFS_filename))
	if err != nil {
		fmt.Printf("Failed to get file (%s): %v\n", HyDFS_filename, err)
		return
	}
}

// test2 hdfs_data/business_1.txt
// * Run store on a randomly picked VM after
func (u *UserActionInfo) test2(HyDFS_filename string) {
	err := u.Ls(HyDFS_filename)
	if err != nil {
		fmt.Printf("Failed to ls file (%s): %v\n", HyDFS_filename, err)
		return
	}
	time.Sleep(500 * time.Millisecond)
	err = u.ListMemIds()
	if err != nil {
		fmt.Printf("Failed to list mem ids: %v\n", err)
		return
	}
}

// * Fail 2 VMs
// test3 hdfs_data/business_1.txt
func (u *UserActionInfo) test3(HyDFS_filename string) {
	err := u.Ls(HyDFS_filename)
	if err != nil {
		fmt.Printf("Failed to ls file (%s): %v\n", HyDFS_filename, err)
		return
	}
	time.Sleep(500 * time.Millisecond)
	err = u.Store()
	if err != nil {
		fmt.Printf("Failed to do store: %v\n", err)
		return
	}
}

// test4 hdfs_data/business_1.txt data/business_50.txt data/business_51.txt
// * Manually grep the file retrieved_hdfs_data/business_1.txt
func (u *UserActionInfo) test4(HyDFS_filename, a1, a2 string) {
	// create 5 files in hdfs
	as := []string{a1, a2}
	for _, a := range as {
		err := u.Append(a, HyDFS_filename)
		if err != nil {
			fmt.Printf("Failed to append file (%s) to (%s): %v\n", a, HyDFS_filename, err)
			return
		}
		time.Sleep(1 * time.Second)
	}
	err := u.Get(HyDFS_filename, retrieved_fn(HyDFS_filename))
	if err != nil {
		fmt.Printf("Failed to get file (%s): %v\n", HyDFS_filename, err)
		return
	}
	// wait for create-acks
}

var VM_MAP = map[string]string{
	"VM1":  "172.22.157.26",
	"VM2":  "172.22.159.26",
	"VM3":  "172.22.95.26",
	"VM4":  "172.22.157.27",
	"VM5":  "172.22.159.27",
	"VM6":  "172.22.95.27",
	"VM7":  "172.22.157.28",
	"VM8":  "172.22.159.28",
	"VM9":  "172.22.95.28",
	"VM10": "172.22.157.29",
}

// test5_part1 hdfs_data/business_1.txt VM1 VM2 VM3 VM4 data/business_60.txt data/business_61.txt data/business_62.txt data/business_63.txt
func (u *UserActionInfo) test5_part1(HyDFS_filename, vm1, vm2, vm3, vm4, a1, a2, a3, a4 string) {
	host1 := VM_MAP[vm1]
	host2 := VM_MAP[vm2]
	host3 := VM_MAP[vm3]
	host4 := VM_MAP[vm4]

	err := u.Multiappend(HyDFS_filename, []string{host1, host2, host3, host4}, []string{a1, a2, a3, a4})
	if err != nil {
		fmt.Printf("Failed to multiappend to (%s): %v\n", HyDFS_filename, err)
		return
	}
	err = u.Merge(HyDFS_filename)
	if err != nil {
		fmt.Printf("Failed to merge (%s): %v\n", HyDFS_filename, err)
		return
	}
	err = u.Ls(HyDFS_filename)
	if err != nil {
		fmt.Printf("Failed to merge (%s): %v\n", HyDFS_filename, err)
		return
	}
}

// * Pick two VMs where the above file is stored
// test5_part2 HyDFSfilename VM1 VM2
// * more 172.22.157.26_retrieved_hdfs_data/business_1.txt 172.22.159.26_retrieved_hdfs_data/business_1.txt
// * diff 172.22.157.26_retrieved_hdfs_data/business_1.txt 172.22.159.26_retrieved_hdfs_data/business_1.txt
func (u *UserActionInfo) test5_part2(HyDFS_filename, host1, host2 string) {
	target1 := fmt.Sprintf("%s_%s", host1, retrieved_fn(HyDFS_filename))
	err := u.GetFromReplica(host1, HyDFS_filename, target1)
	if err != nil {
		fmt.Printf("Failed to get (%s) from replica (%s): %v\n", HyDFS_filename, host1, err)
		return
	}
	target2 := fmt.Sprintf("%s_%s", host2, retrieved_fn(HyDFS_filename))
	u.GetFromReplica(host2, HyDFS_filename, target2)

}

func (u *UserActionInfo) test_MP4(op1_exe string, op2_exe string, HyDFS_src_filename string, HyDFS_dest_filename string, num_tasks int, VM1 int, VM2 int) {
	fmt.Printf("op1_exe=%s op2_exe=%s HyDFS_src_filename=%s HyDFS_dest_filename=%s num_tasks=%d VM1=%d VM2=%d\n",
		op1_exe, op2_exe, HyDFS_src_filename, HyDFS_dest_filename, num_tasks, VM1, VM2)

	errCh := make(chan error, 1)
	go func() {
		errCh <- u.RainstormCmd(op1_exe, op2_exe, HyDFS_src_filename, HyDFS_dest_filename, num_tasks)
	}()

	time.Sleep(1500 * time.Millisecond)

	vm1_address := VM_MAP[fmt.Sprintf("VM%d", VM1)]
	fmt.Printf("VM1 address is %s\n", vm1_address)

	// Create and send the "kill" message to VM1
	fsmsg := utils.FileSystemMessage{
		Action:   "kill",
		Filename: "",
		Content:  nil,
	}
	synchronizer.SendFileSystemMessage(vm1_address, fsmsg)

	vm2_address := VM_MAP[fmt.Sprintf("VM%d", VM2)]
	fmt.Printf("VM2 address is %s\n", vm2_address)

	// Create and send the "kill" message to VM2
	fsmsg2 := utils.FileSystemMessage{
		Action:   "kill",
		Filename: "",
		Content:  nil,
	}
	synchronizer.SendFileSystemMessage(vm2_address, fsmsg2)

	// Check if RainstormCmd completed successfully
	err := <-errCh
	if err != nil {
		fmt.Printf("Rainstorm Cmd Failed: %v\n", err)
		return
	}
}
