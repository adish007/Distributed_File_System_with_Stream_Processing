package main

import (
	"fmt"
	"g85/mp4_again_again/cache"
	"g85/mp4_again_again/localstorage"
	"g85/mp4_again_again/rainstorm"
	"g85/mp4_again_again/ring/introducer"
	"g85/mp4_again_again/ring/member"
	"g85/mp4_again_again/user"
	"os"
	"strconv"
)

func HandleMemberOrIntroducer(args []string) (*member.Member, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("invalid arguments")
	}
	typeArg := args[1]
	if typeArg == "member" {
		m, err := member.StartMember(1112)
		if err != nil {
			return nil, fmt.Errorf("start member failed: %v", err)
		}
		return m, nil
	}
	// run ONLY introducer on introducer node (not member).
	if typeArg == "introducer" {
		i, err := introducer.StartIntroducer(1112, 1113)
		if err != nil {
			return nil, fmt.Errorf("start introducer failed: %v", err)
		}
		return i.LocalMember, nil
	}
	return nil, fmt.Errorf("invalid member type (member/introducer)")
}

func main() {
	args := os.Args

	fmt.Print("Enter (Rainstorm) VM Number: ")
	var input string
	fmt.Scanln(&input)
	vmNum, err := strconv.Atoi(input)
	if err != nil {
		fmt.Printf("Failed to get (Rainstorm) VM Number: %v\n", err)
	}

	// typeArg is either "member" or "introducer", otherwise invalid
	localMember, err := HandleMemberOrIntroducer(args)
	if err != nil {
		fmt.Printf("Failed to create local member: %v\n", err)
		return
	}
	r := &rainstorm.RainStormMember{LocalMember: localMember, VMNum: vmNum}
	r.Run()

	fmt.Printf("Starting local storage daemon \n")
	_, err = localstorage.Start(1114)
	if err != nil {
		fmt.Printf("Failed to start local storage daemon: %v\n", err)
	}
	k := 0
	info := user.UserActionInfo{LocalMember: localMember, RainStormMember: r, LocalCache: cache.NewCache(k)}
	fmt.Printf("Starting to listen for input \n")
	go info.ContinuouslyPullFromPreds()
	info.ListenForInput()
}
