package introducer

import (
	"fmt"
	"g85/mp4_again_again/ring/member"
	"net"
	"net/rpc"
	"os"
	"time"
)

type Introducer struct {
	LocalMember *member.Member
}

// RPC to add a new member to the group
func (i *Introducer) AddMember(newMember *member.Member, reply *[]member.MemberID) error {
	// Send msgNewMember to every MemberID that isn't itself (or the new member which isn't added yet)
	fmt.Printf("Recived %s:%d in Introducer.AddMember\n", newMember.ID.Host, newMember.ID.Port)
	for _, existingMemberID := range i.LocalMember.MembershipList {
		if existingMemberID == i.LocalMember.ID {
			continue
		}
		i.LocalMember.ActionAckTarget("NEW", newMember.ID, 4*time.Second, existingMemberID)
	}

	// Remove locally
	fmt.Fprintf(os.Stderr, "Adding %s\n", newMember.ID.Host)
	i.LocalMember.MembershipList = append(i.LocalMember.MembershipList, newMember.ID)
	i.LocalMember.SortMembershipListByHash()
	*reply = i.LocalMember.MembershipList
	fmt.Printf("AddMember finished \n")
	return nil
}

func ContinuouslyListen(listener net.Listener) {
	for {
		memberConn, err := listener.Accept()
		fmt.Println("LOG: member connected")
		if err != nil {
			fmt.Println(err)
			continue
		}
		go rpc.ServeConn(memberConn)
	}
}

// Ran from main.go to start introducer
func StartIntroducer(memberPort int, introducerPort int) (*Introducer, error) {
	fmt.Println("Starting introducer")
	localMember, err := member.SetupMember(memberPort)
	if err != nil {
		fmt.Printf("setup member: %v\n", err)
		return nil, err
	}

	go localMember.ListenForInput()
	go localMember.ContinuouslyCheckForIncoming()
	go localMember.ContinuouslyPingTargets()

	// Start RPC server for introducer
	i := &Introducer{LocalMember: localMember}
	rpc.Register(i)
	listeningAddress := fmt.Sprintf(":%d", introducerPort)
	listener, err := net.Listen("tcp", listeningAddress)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf("LOG: TCP listener created on port %d \n", introducerPort)

	// PATCH
	// err = localMember.IntroduceSelf()
	// if err != nil {
	// 	fmt.Println(err)
	// 	return nil, err
	// }

	// Continuously listen for a connection
	go ContinuouslyListen(listener)
	return i, nil
}
