package member

import (
	"bufio"
	"cmp"
	"encoding/json"
	"fmt"
	"g85/mp4_again_again/utils"
	"net"
	"net/rpc"
	"os"
	"slices"
	"time"

	"math/rand"
)

type MemberID struct {
	Host      string
	Port      int
	Timestamp int64
	HostHash  uint32
}

type Member struct {
	ID             MemberID
	MembershipList []MemberID
	SuspicionMap   map[MemberID]bool
	// SuspicionTimeMap records the last timestamp when [memberID] was suspected, so it doesn't try to delete right away
	SuspicionTimeMap     map[MemberID]int64
	IncarnationNumberMap map[MemberID]int
	// new
	// RainStormMember *rainstorm.RainStormMember
}

// Helper function to select a random target during PINGing
func (m *Member) SelectRandTarget() (MemberID, bool) {
	var memListExceptCur []MemberID
	for _, existingMember := range m.MembershipList {
		if existingMember != m.ID {
			memListExceptCur = append(memListExceptCur, existingMember)
		}
	}

	if len(memListExceptCur) == 0 {
		return MemberID{"", 0, 0.0, 0}, false
	}

	randomInt := rand.Intn(len(memListExceptCur))
	return memListExceptCur[randomInt], true
}

// Declare some configurable globals
var (
	SUS_ACTIVATED     = true
	MESSAGE_TIME      = 6 * time.Second
	PING_TIME         = 3 * time.Second
	MESSAGE_SENT_RATE = 100
)

// Random function to help with drop rate, utilized in *-ACK responses
func (m *Member) ShouldSendMessage() bool {
	if MESSAGE_SENT_RATE == 100 {
		return true
	}
	randomNumber := rand.Intn(100)
	// If randomNumber is less than the percentage, return true, otherwise return false
	return randomNumber < MESSAGE_SENT_RATE
}

// Multiple utility function that allows for PING, NEW, DEL, etc. The ACK-s are handled in responses instead.
func (m *Member) ActionAckTarget(action string, item MemberID, deadline time.Duration, target MemberID) {
	ack := make([]byte, 1024)
	address := fmt.Sprintf("%s:%d", target.Host, target.Port)
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("UDP Connection Failed %v", err)
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(deadline))
	msg := Message{action, item, m.IncarnationNumberMap[item]}
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Send message
	_, err = conn.Write(jsonMsg)
	if err != nil {
		fmt.Printf("Failed to send data over UDP: %v\n", err)
		return
	}
	//fmt.Printf("LOG: Sending UDP message to %s: %s\n", target.Host, jsonMsg)

	// Read message
	n, err := bufio.NewReader(conn).Read(ack)
	ack = ack[:n]
	if err == nil {
		// fmt.Printf("LOG: Recieved from %s: %s\n", address, ack)
	} else {
		fmt.Printf("LOG: No response from %s: %v\n", address, err)
		// This will call other members to SUS/DEL target REGARDLESS if action was PING or NEW or DEL or SUS or ALIVE
		// i.e. *-ACK wasn't recieved
		if !SUS_ACTIVATED || m.SuspicionMap[target] {
			if m.SuspicionMap[target] {
				now := time.Now().UnixNano()
				var fivesec int64 = 5e9
				if now-m.SuspicionTimeMap[target] <= fivesec {
					fmt.Fprintf(os.Stderr, "Recent suspicion: do nothing \n")
					return
				}
			}
			// FAIL path
			// delete from local membership list
			deleteSuccessful := m.DeleteFromMembershipList(target)
			if !deleteSuccessful {
				fmt.Printf("Failed to delete from membership list: %s\n", target.Host)
			}
			fmt.Printf("Deleted from local membership list: %s\n", target.Host)

			// delete from other membership lists
			for _, existingMemberID := range m.MembershipList {
				if existingMemberID != m.ID {
					m.ActionAckTarget("DEL", target, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
				}
			}
			return
		}
		// Should call suspect first if > deadline and SUS activated
		fmt.Fprintf(os.Stderr, "Initial Suspecting %s\n", target.Host)
		m.SuspicionMap[target] = true
		m.SuspicionTimeMap[target] = time.Now().UnixNano()
		// Message SUS to everyone else, including target
		for _, existingMemberID := range m.MembershipList {
			if existingMemberID != m.ID {
				m.ActionAckTarget("SUS", target, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
			}
		}

	}
}

// Delete from local membership list. Flexible so it doesn't fail if it was already deleted
func (m *Member) DeleteFromMembershipList(toDelete MemberID) bool {
	if m.ID == toDelete {
		RUNNING = false
		return true
	}

	succeeded := false
	newMembershipList := []MemberID{}
	for _, existingMemberID := range m.MembershipList {
		if existingMemberID != toDelete {
			newMembershipList = append(newMembershipList, existingMemberID)
			succeeded = true
		}
	}
	m.MembershipList = newMembershipList

	if succeeded {
		fmt.Fprintf(os.Stderr, "Deleting %s: %d\n", toDelete.Host, time.Now().UnixNano())
	}
	return succeeded
}

type Message struct {
	Action            string
	ID                MemberID
	IncarnationNumber int
}

// Global channel-related variables for "leave"/"join"
var (
	RUNNING             = true
	RUN_PING_CHANNEL    = make(chan bool, 1000)
	QUIT_PING_CHANNEL   = make(chan bool, 1000)
	RUN_LISTEN_CHANNEL  = make(chan bool, 1000)
	QUIT_LISTEN_CHANNEL = make(chan bool, 1000)
)

// Listen for incoming messages
func (m *Member) ContinuouslyCheckForIncoming() {
	// Sets up a UDP server
	full_p_size := 2048
	p := make([]byte, full_p_size)
	addr := net.UDPAddr{
		Port: m.ID.Port,
		IP:   net.ParseIP(m.ID.Host),
	}
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("UDP Listen failed: %v\n", err)
		return
	}
	fmt.Printf("LOG: UDP listener created on  %s:%d \n", addr.IP.String(), addr.Port)

	// For-select used so that function may be paused and resumed if necessary
	for {
		select {
		case <-QUIT_LISTEN_CHANNEL:
			fmt.Fprintf(os.Stderr, "Pausing ContinuouslyCheckForIncoming")
			<-RUN_LISTEN_CHANNEL
			fmt.Fprintf(os.Stderr, "Resumed ContinuouslyCheckForIncoming")
		default:
			// Read data and handle appropriately
			p = p[:full_p_size]
			n, remoteaddr, err := ser.ReadFromUDP(p)
			p = p[:n]
			if err != nil {
				fmt.Printf("UDP Read failed: %v\n", err)
				continue
			}
			var msg Message
			err = json.Unmarshal(p, &msg)
			if err != nil {
				fmt.Printf("unmarshal error: %v\n", err)
				return
			}
			m.HandleMessage(msg, ser, remoteaddr)
		}
	}
}

// Handler function if SUS message recieved, indicating either itself or another node is suspicious
func (m *Member) HandleSUS(msg Message, ser *net.UDPConn, remoteaddr *net.UDPAddr) {
	if msg.IncarnationNumber <= m.IncarnationNumberMap[msg.ID] {
		// if stale message, only send back an ACK
		m.sendResponse(ser, remoteaddr, "SUS-ACK")
		return
	}
	if m.ID == msg.ID {
		// Tries to save itself! Both responds to SUS and sends out an ALIVE message to everyone
		m.IncarnationNumberMap[m.ID] += 1
		m.sendResponse(ser, remoteaddr, "SUS-ACK")
		for _, existingMemberID := range m.MembershipList {
			if existingMemberID != m.ID {
				m.ActionAckTarget("ALIVE", m.ID, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
			}
		}
	} else {
		// Update suspicion map accordingly
		fmt.Fprintf(os.Stderr, "Suspecting %s\n", msg.ID.Host)
		m.SuspicionMap[msg.ID] = true
		m.SuspicionTimeMap[msg.ID] = time.Now().UnixNano()
		m.sendResponse(ser, remoteaddr, "SUS-ACK")
	}
}

// Handle an ALIVE message, sent from a node that is being investigated for suspicion
func (m *Member) HandleALIVE(msg Message, ser *net.UDPConn, remoteaddr *net.UDPAddr) {
	if msg.IncarnationNumber <= m.IncarnationNumberMap[msg.ID] {
		// if stale message, only send back an ACK
		m.sendResponse(ser, remoteaddr, "ALIVE-ACK")
		return
	}
	fmt.Fprintf(os.Stderr, "Unsuspecting %s\n", msg.ID.Host)
	m.SuspicionMap[msg.ID] = false
	m.SuspicionTimeMap[msg.ID] = 0
	m.IncarnationNumberMap[msg.ID] = msg.IncarnationNumber
	m.sendResponse(ser, remoteaddr, "ALIVE-ACK")
}

// Handle all different messages
func (m *Member) HandleMessage(msg Message, ser *net.UDPConn, remoteaddr *net.UDPAddr) {
	switch msg.Action {
	case "NEW":
		fmt.Println("Recieved NEW message")
		fmt.Fprintf(os.Stderr, "Adding %s\n", msg.ID.Host)
		m.MembershipList = append(m.MembershipList, msg.ID)
		m.SortMembershipListByHash()
		m.sendResponse(ser, remoteaddr, "NEW-ACK")
	case "SUS":
		fmt.Println("Recieved SUS message")
		m.HandleSUS(msg, ser, remoteaddr)
	case "DEL":
		fmt.Println("Recieved DEL message")
		m.DeleteFromMembershipList(msg.ID)
		m.sendResponse(ser, remoteaddr, "DEL-ACK")
	case "PING":
		// fmt.Println("Recieved PING")
		m.sendResponse(ser, remoteaddr, "PING-ACK")
	case "ALIVE":
		fmt.Println("Recieved ALIVE")
		m.HandleALIVE(msg, ser, remoteaddr)

	case "SUS-ON":
		fmt.Println("Recieved SUS-ON message")
		SUS_ACTIVATED = true
		m.sendResponse(ser, remoteaddr, "SUS-ON-ACK")
	case "SUS-OFF":
		fmt.Println("Recieved SUS-OFF message")
		SUS_ACTIVATED = false
		m.sendResponse(ser, remoteaddr, "SUS-OFF-ACK")

	// No work needed if an ACK is recieved. The problem is if it isn't recieved, which is handled in ActionAckTarget
	case "NEW-ACK":
		fmt.Println("Recieved NEW-ACK: Do nothing")
	case "PING-ACK":
		// fmt.Println("Recieved PING-ACK: Do nothing")
	case "SUS-ACK":
		fmt.Println("Recieved SUS-ACK: Do nothing")
	case "DEL-ACK":
		fmt.Println("Recieved DEL-ACK: Do nothing")
	case "SUS-ON-ACK":
		fmt.Println("Recieved SUS-ON-ACK: Do nothing")
	case "SUS-OFF-ACK":
		fmt.Println("Recieved SUS-OFF-ACK: Do nothing")
	}
	// Update incarnation number if not a stale message
	if msg.IncarnationNumber > m.IncarnationNumberMap[msg.ID] {
		m.IncarnationNumberMap[msg.ID] = msg.IncarnationNumber
	}
}

// Usually used for *-ACK. Message drop is implemented here, simulating network misses
func (m *Member) sendResponse(conn *net.UDPConn, addr *net.UDPAddr, reply string) {
	ok := m.ShouldSendMessage()
	if !ok {
		fmt.Printf("Message dropped\n")
		return
	}

	_, err := conn.WriteToUDP([]byte(reply), addr)
	if err != nil {
		fmt.Printf("Couldn't send response %v\n", err)
	}
}

// Keep pinging a random target
func (m *Member) ContinuouslyPingTargets() {
	fmt.Println("Started ContinuouslyPingTargets")
	// For-select used so it can be paused/resumed
	for {
		select {
		case <-QUIT_PING_CHANNEL:
			fmt.Fprintf(os.Stderr, "Pausing ContinuouslyPingTargets")
			<-RUN_PING_CHANNEL
			fmt.Fprintf(os.Stderr, "Resumed ContinuouslyPingTargets")
		default:
			time.Sleep((PING_TIME)) // wait for 1 second in between pings
			// fmt.Printf("Current Membership List: %v\n", m.MembershipList)
			randomTarget, ok := m.SelectRandTarget()
			if !ok {
				// fmt.Printf("Failed to select random target \n")
				continue
			}
			//fmt.Printf("About to PING %s:%d\n", randomTarget.Host, randomTarget.Port)
			m.ActionAckTarget("PING", MemberID{"", 0, 0.0, 0}, MESSAGE_TIME, randomTarget)
			// fmt.Printf("PINGed %s:%d\n", randomTarget.Host, randomTarget.Port)
		}
	}
}

// RPC connect to Introducer so that it is introduced to the group
func (m *Member) IntroduceSelf() error {
	introducerConfig, err := GetIntroducerConfig("ring/introducerConfig.json")
	if err != nil {
		return fmt.Errorf("LOG: Reading ring/introducerConfig.json failed: %v", err)
	}

	address := fmt.Sprintf("%s:%d", introducerConfig.Host, introducerConfig.Port)
	conn, err := rpc.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("LOG: Failed to dial introducer: %v", err)
	}

	fmt.Printf("Sending %s:%d into Introducer.AddMember\n", m.ID.Host, m.ID.Port)
	var reply []MemberID
	err = conn.Call("Introducer.AddMember", m, &reply)
	if err != nil {
		return fmt.Errorf("LOG: RPC call to introducer failed: %v", err)
	}
	if conn == nil {
		return fmt.Errorf("LOG: RPC connection with introducer is nil")
	}
	if len(reply) == 0 {
		return fmt.Errorf("LOG: (UNEXPECTED ERR) Member list returned unsuccessful")
	}
	m.MembershipList = reply

	fmt.Println("Successfully introduced self to group")
	return nil
}

type IntroducerConfig struct {
	Host string
	Port int
}

// Helper to figure out who the (hardcoded) introducer is
func GetIntroducerConfig(path string) (*IntroducerConfig, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var machineConfig IntroducerConfig
	err = json.Unmarshal(bytes, &machineConfig)
	if err != nil {
		return nil, err
	}
	return &machineConfig, nil
}

// Make an empty member
func SetupMember(port int) (*Member, error) {
	// Connect to some address (google) so that we can get the local address
	conn, err := net.Dial("udp", "8.8.8.8:")
	if err != nil {
		fmt.Println("Error:", err)
		return nil, err
	}
	defer conn.Close()

	// Get the local address of the connection
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	fmt.Fprintln(os.Stderr, "IP address of the machine:", localAddr.IP.String())
	host := localAddr.IP.String()

	fmt.Println("Starting member")

	memberID := MemberID{
		Host:      host,
		Port:      port,
		Timestamp: time.Now().UnixNano(),
		HostHash:  utils.ComputeHash(host),
	}
	fmt.Fprintln(os.Stderr, "Timestamp of the machine:", memberID.Timestamp)

	m := &Member{
		ID:                   memberID,
		MembershipList:       []MemberID{memberID},
		SuspicionMap:         map[MemberID]bool{},
		SuspicionTimeMap:     map[MemberID]int64{},
		IncarnationNumberMap: map[MemberID]int{},
	}
	return m, nil
}

// DEPRECATED Keep listening in terminal for input
func (m *Member) ListenForInput() {
	// reader := bufio.NewReader(os.Stdin)
	// for {
	// 	fmt.Fprintf(os.Stderr, "Enter list_mem / list_self / join / leave / enable_sus / disable_sus / status_sus: \n")
	// 	input, _ := reader.ReadString('\n')
	// 	input = strings.TrimSpace(input)
	// 	m.HandleTerminalInput(input)
	// }
}

// DEPRECATED Handle the different possible terminal inputs
func (m *Member) HandleTerminalInput(input string) {
	switch input {
	case "list_mem":
		fmt.Fprintf(os.Stderr, "Membership List: %v\n", m.MembershipList)
	case "list_self":
		fmt.Fprintf(os.Stderr, "My ID: %v\n", m.ID)
	case "join":
		if RUNNING {
			fmt.Fprintf(os.Stderr, "Already joined\n")
			return
		}
		fmt.Fprintf(os.Stderr, "Joining")
		// Reset member
		m.ID.Timestamp = time.Now().UnixNano()
		m.MembershipList = []MemberID{}
		m.IncarnationNumberMap = map[MemberID]int{}
		m.SuspicionMap = map[MemberID]bool{}
		m.SuspicionTimeMap = map[MemberID]int64{}
		// Re-introduce member
		err := m.IntroduceSelf()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return
		}
		// Allow ongoing goroutines to continue
		RUNNING = true
		RUN_LISTEN_CHANNEL <- true
		RUN_PING_CHANNEL <- true
	case "leave":
		fmt.Fprintf(os.Stderr, "Leaving\n")
		for _, existingMemberID := range m.MembershipList {
			if existingMemberID != m.ID {
				m.ActionAckTarget("DEL", m.ID, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
			}
		}
		// Pause ongoing goroutines
		RUNNING = false
		QUIT_LISTEN_CHANNEL <- true
		QUIT_PING_CHANNEL <- true
	case "enable_sus":
		fmt.Fprintf(os.Stderr, "Enabling Suspicion\n")
		SUS_ACTIVATED = true
		// Notify all that suspicion was turned on
		for _, existingMemberID := range m.MembershipList {
			if existingMemberID != m.ID {
				m.ActionAckTarget("SUS-ON", MemberID{"", 0, 0.0, 0}, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
			}
		}
	case "disable_sus":
		fmt.Fprintf(os.Stderr, "Disabling Suspicion\n")
		SUS_ACTIVATED = false
		// Notify all that suspicion was turned off
		for _, existingMemberID := range m.MembershipList {
			if existingMemberID != m.ID {
				m.ActionAckTarget("SUS-OFF", MemberID{"", 0, 0.0, 0}, MESSAGE_TIME, existingMemberID) // slightly less than 5 sec
			}
		}
	case "status_sus":
		fmt.Fprintf(os.Stderr, "Suspicion status: %t\n", SUS_ACTIVATED)
	}
}

// Called from main.go to start a member
// Only used for non-introducer members. Must be called after an introducer is already established.
func StartMember(port int) (*Member, error) {
	m, err := SetupMember(port)
	if err != nil {
		fmt.Printf("setup member: %v\n", err)
		return nil, err
	}
	err = m.IntroduceSelf()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	go m.ListenForInput()
	go m.ContinuouslyCheckForIncoming()
	go m.ContinuouslyPingTargets()
	return m, nil
}

func (m *Member) SortMembershipListByHash() {
	hashCmp := func(a, b MemberID) int {
		return cmp.Compare(a.HostHash, b.HostHash)
	}
	slices.SortFunc(m.MembershipList, hashCmp)
}
