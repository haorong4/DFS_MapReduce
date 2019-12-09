package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const TIMEOUT = 2000.0
const HEARTBEAT = 500.0

const introducer = "172.22.152.46"
const PORT = ":8080"
const INTROPORT = ":8000"
const HEARTPORT = ":8088"

var time_base = time.Now()
var localIP = ""
var se *net.UDPConn

//lists
var TimeTable = make(map[string]time.Time)
var FailList = make(map[string]int)
var joinList = make(map[string]int)
var MemberList = [4]string{"", "", "", ""}
var conns [4]*net.UDPConn
var Allmembers []hashRecord

// mutex and wait groups for synchronize
var mutex_time = &sync.Mutex{}
var mutex_FailList sync.Mutex
var mutex_conns sync.Mutex
var wg_heartbeat sync.WaitGroup
var wg_signal sync.WaitGroup

//channels
var quitNow = make(chan bool)
var channel = make(chan string, 300)
var failDone = make(chan bool)

type hashRecord struct {
	ip    string
	value uint32
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/*
	function using for getting local IP address
*/
func GetIpAddr() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	output := conn.LocalAddr().String()
	output = strings.Split(output, ":")[0]
	return output
}

/*
	function using generateTimestamp
*/
func generateTimestamp() string {
	return string(time.Now().Second())
}

/*
	updating the heartbeat time for IP in Time table.
	if IP does not exist in the membership list, do nothing.
*/
func UpdateList(IP string) {
	IP = strings.Split(IP, ":")[0]
	mutex_time.Lock()
	if _, ok := TimeTable[IP]; ok {
		TimeTable[IP] = time.Now()
	}
	mutex_time.Unlock()
}

/*
	Updating failure information
		fail_ip: the IP address of the fail machine
		time_off: the time stamp of the failure
	if this failure is new, pushing it to all its neighbors,
	if it have receive the same failure before, block the message
*/
func FailUpdate(fail_ip string, time_off int) {
	mutex_FailList.Lock()
	for key, val := range FailList {
		if key == fail_ip && (math.Abs(float64(time_off-val)) < 30.0) {
			mutex_FailList.Unlock()
			return
		}
	}
	if MemberList[3] == fail_ip {
		MemberList[3] = "0"
		conns[3] = nil
	}
	joinList[fail_ip] = 1000
	delete(joinList, fail_ip)
	FailList[fail_ip] = time_off
	fmt.Println(fail_ip, " fails at ", time_off)
	msg := "Fail\n" + fail_ip + "\n" + strconv.Itoa(time_off) + "\n"
	channel <- msg
	mutex_FailList.Unlock()
}

func JoinUpdate(join_ip string, time_off int) {
	for key, val := range joinList {
		if key == join_ip && (math.Abs(float64(time_off-val)) < 10.0) {
			joinList[join_ip] = time_off
			return
		}
	}

	hashval := hash(join_ip)
	lenAllm := len(Allmembers)
	var index int
	for index = 0; index < lenAllm; index++ {
		if Allmembers[index].value < hashval {
			continue
		} else {
			temp := make([]hashRecord, 1)
			temp[0] = hashRecord{join_ip, hashval}
			Allmembers = append(Allmembers[:index], append(temp, Allmembers[index:]...)...)
			break
		}
	}
	if index == len(Allmembers) {
		Allmembers = append(Allmembers, hashRecord{join_ip, hashval})
	}

	joinList[join_ip] = time_off
	fmt.Println(join_ip, "join at ", time_off)
	msg := "Join\n" + join_ip + "\n" + strconv.Itoa(time_off) + "\n"
	channel <- msg
}

/*
	this function is used for updating all the membership connection (to listeners)
*/
func ConnUpdate() {
	mutex_conns.Lock()
	for key, value := range MemberList {
		if value == localIP || value == "0" {
			conns[key] = nil
			continue
		}
		udpAddr, err := net.ResolveUDPAddr("udp4", value+PORT)
		if err != nil {
			fmt.Printf("ResolveUDPAddr %d failed\n", key)
			continue
		}
		conn, err2 := net.DialUDP("udp", nil, udpAddr)
		if err2 != nil {
			fmt.Printf("DialUDP %d failed\n", key)
			continue
		}
		conns[key] = conn
	}
	mutex_conns.Unlock()
}

/*
	this function is used for updating membership list
		command: the string list of the new membership's IP address
*/
func MembershipUpdate(commands []string) {
	fmt.Println("MembershipUpdate")
	fmt.Println(commands)
	for i, s := range commands {
		if s != "0" {
			MemberList[i] = s
		}
	}
	for key, _ := range TimeTable {
		delete(TimeTable, key)
	}
	for _, s := range MemberList[0:3] {
		if s == localIP || s == "0" {
			continue
		}
		TimeTable[s] = time.Now()
	}
	fmt.Println(MemberList)
}

/*
	this function is used when join the virtual ring.
	sending messages to its future neighbors to establish connection,
	those messages will also update the neighors' membership list
*/
func JoinRing() {
	messages := [4]string{"", "", "", ""}
	messages[0] = "FullUpdate\n0\n0\n0\n" + localIP + "\n"
	messages[1] = "FullUpdate\n0\n0\n" + localIP + "\n" + MemberList[2] + "\n"
	messages[2] = "FullUpdate\n" + MemberList[1] + "\n" + localIP + "\n0\n0\n"
	messages[3] = "FullUpdate\n" + localIP + "\n0\n0\n0\n"

	for i, mIP := range MemberList {
		if mIP == localIP {
			continue
		}
		msg := messages[i]
		udpAddr, err := net.ResolveUDPAddr("udp4", mIP+PORT)
		if err != nil {
			fmt.Println("join ", mIP, " fail")
			continue
		}
		conn, err := net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			fmt.Println("join ", mIP, " fail")
			continue
		}
		_, err = conn.Write([]byte(msg))
		if err != nil {
			fmt.Println("join ", mIP, " fail")
		}
	}
}

/*
	this function is used for fixing membership list when failure happens
*/
func FixRing(index int) {
	fmt.Println("FIX RING!!")
	length := len(Allmembers)
	// msgs := [4]string{"1", "1", "1", "1"}
	if index == 0 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == MemberList[index] {
				new = Allmembers[(i+length-1)%length].ip
			}
		}
		MemberList[0] = new
	} else if index == 1 {
		MemberList[1] = MemberList[0]
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == MemberList[index] {
				new = Allmembers[(i+length-2)%length].ip
			}
		}
		MemberList[0] = new
	} else if index == 2 {
		MemberList[2] = MemberList[3]
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == MemberList[index] {
				new = Allmembers[(i+2)%length].ip
			}
		}
		MemberList[3] = new
	} else if index == 3 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == MemberList[index] {
				new = Allmembers[(i+1)%length].ip
			}
		}
		MemberList[3] = new
	}

	for i, hash := range Allmembers {
		if hash.ip == MemberList[index] {
			copy(Allmembers[i:], Allmembers[i+1:])
			Allmembers[len(Allmembers)-1] = hashRecord{"", 0}
			Allmembers = Allmembers[:len(Allmembers)-1]
			break
		}
	}
}

/*
	this function is used when the node is leaving,
	before quit the group, it will inform the introducer.
*/
func InformIntroducerQuit() {
	udpAddr, err := net.ResolveUDPAddr("udp4", introducer+PORT)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	msg := "Quit\n" + localIP + "\n"
	_, err = conn.Write([]byte(msg))
}

/*
	this is the helper function using for generate Quiting message
*/
func QuitMsg() string {
	List := MemberList
	for i, v := range List {
		if v == localIP {
			List[i] = "0"
		}
	}
	msg := ""
	msg += "FullUpdate\n0\n0\n0\n" + List[2] + "\n#"
	msg += "FullUpdate\n0\n0\n" + List[2] + "\n" + List[3] + "\n#"
	msg += "FullUpdate\n" + List[0] + "\n" + List[1] + "\n0\n0" + "\n#"
	msg += "FullUpdate\n" + List[1] + "\n0\n0\n0\n"
	return msg
}

/*
	this is the helper function to respond introducer when introducer rejoin after failed
*/
func ResToIntro() {
	ut, err := net.ResolveUDPAddr("udp4", introducer+PORT)
	if err != nil {
		log.Fatal(err)
	}
	ct, err := net.DialUDP("udp", nil, ut)
	if err != nil {
		log.Fatal(err)
	}
	ct.Write([]byte("Welcome\n"))
}

/*
	this function resolves the request message from other machine,
	requests including: update membership, update failure, responding to introducer's rejoin
*/
func HandleConn(IP string, Package []byte) {
	message := string(Package)
	command := strings.Split(message, "\n")
	UpdateList(IP) // update the time table
	switch command[0] {
	case "Fail":
		t, _ := strconv.Atoi(command[2])
		FailUpdate(command[1], t)
	case "FullUpdate":
		MembershipUpdate(command[1:5])
		ConnUpdate()
	case "Alive":
		ResToIntro()
	case "Join":
		t, _ := strconv.Atoi(command[2])
		JoinUpdate(command[1], t)

	}

}

/*
	this is the Failure detector running in the background,
	it keeps tracking all the entry on Time table,
	Once indentify failures, it will update the failist and push
	the message to the node's neighbors
*/
func FailDetector() {
outer:
	for {
		select {
		case <-failDone:
			break outer
		default:
		}
		time.Sleep(time.Millisecond * 100)
		for key, value := range TimeTable {
			time_diff := time.Now().Sub(value).Nanoseconds() / 1000000
			if time_diff >= TIMEOUT {
				// IP key failed !!!
				FailUpdate(key, int(time.Now().Sub(time_base).Seconds()))
				for i, v := range MemberList {
					if v == key {
						delete(TimeTable, key)
						MemberList[i] = "0"
						conns[i] = nil
						FixRing(i)
						break
					}
				}
			}
		}
	}
	wg_signal.Done()
}

/*
	helper function for catching signals for special operation
	such as leave the group, print membership
*/
func signal_handler(sigs chan os.Signal) {
	sig := <-sigs
	fmt.Println(sig)
	channel <- QuitMsg()
	failDone <- true
	wg_signal.Wait()
	InformIntroducerQuit()
	se.Close()
	quitNow <- true
}

/*
	helper function for Printting membership list
*/
func PrintMemberList() {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		if text == "m\n" {
			fmt.Println("Membership list:")
			fmt.Println(Allmembers)
		}
	}
}

/*
	this is the function running in the background sending
	periodic heartbeats and messages to its listeners
*/
func HeartBeat() {
	ticker := time.NewTicker(HEARTBEAT * time.Millisecond)
	done := make(chan bool)
	wg_heartbeat.Add(1)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var msg string
				select {
				case msg = <-channel:

				default:
					msg = "1"
				}
				mutex_conns.Lock()
				if strings.Contains(msg, "#") {
					msgs := strings.Split(msg, "#")
					for i, quitM := range msgs {
						if conns[i] == nil {
							continue
						}
						_, err := conns[i].Write([]byte(quitM))
						if err != nil {
							fmt.Println("quit msg failed")
						}
					}
					wg_heartbeat.Done()
				} else {
					for _, conn := range conns[1:4] {
						if conn == nil {
							continue
						}
						_, err := conn.Write([]byte(msg))
						if err != nil {
							continue
						}
					}
				}
				mutex_conns.Unlock()
			}
		}
	}()
	wg_heartbeat.Wait()
	done <- true
	wg_signal.Done()
}

func main() {
	//setup environment variables
	time_base, _ = time.Parse("2006-01-02 15:04:05.0000 -0500 CDT", "2019-10-05 15:04:05.0000 -0500 CDT")
	localIP = GetIpAddr()
	joinList[localIP] = 0
	joinList[introducer] = 0
	local_hash := hash(localIP)
	intro_hash := hash(introducer)
	if local_hash < intro_hash {
		Allmembers = append(Allmembers, hashRecord{localIP, local_hash})
		Allmembers = append(Allmembers, hashRecord{introducer, intro_hash})
	} else {
		Allmembers = append(Allmembers, hashRecord{introducer, intro_hash})
		Allmembers = append(Allmembers, hashRecord{localIP, local_hash})
	}
	//register signal handler
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	go signal_handler(sigs)

	//connect to introducer
	udpAddrW, err := net.ResolveUDPAddr("udp4", introducer+INTROPORT)
	if err != nil {
		log.Fatal(err)
	}
	udpAddrR, err := net.ResolveUDPAddr("udp4", localIP+INTROPORT)
	if err != nil {
		log.Fatal(err)
	}
	connW, err := net.DialUDP("udp", nil, udpAddrW)
	if err != nil {
		log.Fatal(err)
		return
	}

	joinMsg := ""
	joinMsg = joinMsg + GetIpAddr() + "\n" + generateTimestamp() + "\n"
	_, err = connW.Write([]byte(joinMsg))
	if err != nil {
		log.Fatal(err)
	}
	connR, err := net.ListenUDP("udp", udpAddrR)
	if err != nil {
		log.Fatal(err)
	}
	readMsg := make([]byte, 1024)
	_, _, err = connR.ReadFromUDP(readMsg)
	if err != nil {
		log.Fatal(err)
	}
	// after receiving the memberlist from introducer, doing self update and informing all its neighbors
	strmsg := string(readMsg)
	members := strings.Split(strmsg, "\n")
	fmt.Println(members)
	MembershipUpdate(members[1:5])
	for _, v := range members[5:] {
		if len(v) < 2 {
			continue
		}
		fmt.Println(v)
		joinList[v] = 0
		hashval := hash(v)
		lenAllm := len(Allmembers)
		var index int
		for index = 0; index < lenAllm; index++ {
			if Allmembers[index].value < hashval {
				continue
			} else {
				temp := make([]hashRecord, 1)
				temp[0] = hashRecord{v, hashval}
				Allmembers = append(Allmembers[:index], append(temp, Allmembers[index:]...)...)
				break
			}
		}
		if index == len(Allmembers) {
			Allmembers = append(Allmembers, hashRecord{v, hashval})
		}
	}
	ConnUpdate()
	JoinRing()
	//The member has joined
	//Heartbeat
	wg_signal.Add(2)
	go HeartBeat()
	//Now prepare to recv heartbeats
	Ulisten, err := net.ResolveUDPAddr("udp4", "0.0.0.0:8080")
	if err != nil {
		log.Fatal(err)
	}
	se, err = net.ListenUDP("udp", Ulisten)
	if err != nil {
		log.Fatal(err)
	}
	go FailDetector()
	go PrintMemberList()
	channel <- ("Join\n" + localIP + "\n" + strconv.Itoa(int(time.Now().Sub(time_base).Seconds())))

outer:
	for {
		select {
		case <-quitNow:
			break outer
		default:

		}
		//infinite loop, waiting for messages
		buffer := make([]byte, 1024)
		num, IP, err := se.ReadFromUDP(buffer)
		if err != nil {
			log.Fatal(err)
		}
		if num != 0 {
			go HandleConn(IP.String(), buffer)
		}
	}
}
