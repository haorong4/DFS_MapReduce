//THIS IS THE INTRODUCER
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
var Allmembers []hashRecord

var localIP = ""
var TimeTable = make(map[string]time.Time)
var MemberList = [4]string{"", "", "", ""}
var joinList = make(map[string]int)

var conns [4]*net.UDPConn
var mutex_time = &sync.Mutex{}
var wg_heartbeat sync.WaitGroup
var mutex_conns sync.Mutex
var quitNow = make(chan bool)
var channel = make(chan string, 300)
var failDone = make(chan bool)
var failList = make(map[string]int)
var mux_failList sync.Mutex
var se *net.UDPConn
var wg_signal sync.WaitGroup
var wg_memberUpdate sync.WaitGroup
var mux_memberList sync.Mutex

type hashRecord struct {
	ip    string
	value uint32
}

/*
  Update the local file which keeps a record of Allmembers
*/
func UpdateLocalRecord() {
	f, err := os.OpenFile("./memberlist.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	for _, tmp := range Allmembers {
		f.Write([]byte(tmp.ip + "\n"))
	}
	f.Close()
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

func JoinUpdate(join_ip string, time_off int) {

	// mutex_FailList.Lock()
	for key, val := range joinList {
		if key == join_ip && (math.Abs(float64(time_off-val)) < 10.0) {
			joinList[join_ip] = time_off
			return
		}
	}
	joinList[join_ip] = time_off
	fmt.Println(join_ip, "join at ", time_off)
	msg := "join\n" + join_ip + "\n" + strconv.Itoa(time_off) + "\n"
	channel <- msg
	// mutex_FailList.Unlock()
}

/*
	Updating failure information
		fail_ip: the IP address of the fail machine
		time_off: the time stamp of the failure
	if this failure is new, pushing it to all its neighbors,
	if it have receive the same failure before, block the message
*/
func FailUpdate(fail_ip string, time_off int) {
	// fmt.Println("failtime ", time_off)
	mux_failList.Lock()
	for key, val := range failList {
		if key == fail_ip && (math.Abs(float64(time_off-val)) < 30.0) {
			mux_failList.Unlock()
			return
		}
	}
	if MemberList[3] == fail_ip {
		MemberList[3] = "0"
		conns[3] = nil
	}
	failList[fail_ip] = time_off
	joinList[fail_ip] = 1000
	delete(joinList, fail_ip)
	fmt.Println(fail_ip, " fails at ", time_off)
	msg := "Fail\n" + fail_ip + "\n" + strconv.Itoa(time_off) + "\n"
	channel <- msg
	mux_failList.Unlock()
	for i, tmp := range Allmembers {
		if tmp.ip == fail_ip {
			copy(Allmembers[i:], Allmembers[i+1:])
			Allmembers[len(Allmembers)-1] = hashRecord{"", 0}
			Allmembers = Allmembers[:len(Allmembers)-1]
			break
		}

	}
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
			fmt.Printf("ResolveUDPAddr %d failed %s\n", key, err.Error())
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
	messages[1] += "FullUpdate\n0\n0\n" + localIP + "\n" + MemberList[2] + "\n"
	messages[2] += "FullUpdate\n" + MemberList[1] + "\n" + localIP + "\n0\n0\n"
	messages[3] += "FullUpdate\n" + localIP + "\n0\n0\n0\n"

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
	msgs := [4]string{"1", "1", "1", "1"}
	if index == 1 {
		msgs[0] = "FullUpdate\n0\n0\n" + localIP + "\n" + MemberList[2] + "\n"
		msgs[2] = "FullUpdate\n" + MemberList[0] + "\n0\n0\n0\n"
	} else if index == 2 {
		msgs[3] = "FullUpdate\n" + MemberList[1] + "\n" + localIP + "\n0\n0\n"
		msgs[1] = "FullUpdate\n0\n0\n0\n" + MemberList[3] + "\n"
	} else {
		return
	}

	message := msgs[0] + "#" + msgs[1] + "#" + msgs[2] + "#" + msgs[3]
	channel <- message
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
						break
					}

				}
			}
		}
	}
	wg_signal.Done()
}

/*
  When a member quits or fails, the Allmember list is updated
*/
func UpdateMember(otherip string) {
	for i, tmp := range Allmembers {
		if tmp.ip == otherip {
			Allmembers = append(Allmembers[:i], Allmembers[i+1:]...)
		}
	}
	UpdateLocalRecord()
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
	case "Quit":
		UpdateMember(command[1])
	case "Join":
		t, _ := strconv.Atoi(command[2])
		JoinUpdate(command[1], t)
	case "Mem":
		for _, v := range command[1:] {
			joinList[v] = 0
		}
	}
}

/*
  Get local ip address
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
  Generate a timestamp in seconds
*/
func generateTimestamp() string {
	return string(time.Now().Second())
}

/*
	this is the function running in the background sending
	periodic heartbeats and messages to its listeners
*/
func heartbeat() {
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
							// fmt.Printf("write %d failed\n", i)
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

/*
  Get the hash value of an ip address
*/
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/*
  Catch SIGQUIT and quit the virtual ring
*/
func signal_handler(sigs chan os.Signal) {
	sig := <-sigs
	fmt.Println(sig)
	channel <- QuitMsg()
	failDone <- true
	wg_signal.Wait()
	se.Close()
	quitNow <- true
}

/*
  Print Membership List when user press "m\n"
*/
func PrintMemberList() {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		if text == "m\n" {
			fmt.Println("Membership list:")
			for _, val := range MemberList {
				if val != "0" {
					fmt.Println(val)
				}
			}
		}
	}
}

/*
  Introduce a member into the virtual ring
*/
func introduce() {
	udpAddrR, err := net.ResolveUDPAddr("udp4", introducer+INTROPORT)
	if err != nil {
		log.Fatal(err)
	}
	connR, err := net.ListenUDP("udp", udpAddrR)
	if err != nil {
		log.Fatal(err)
	}
	defer connR.Close()
	msg := make([]byte, 1024)
	for {
		_, addr, err := connR.ReadFromUDP(msg)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(addr, " has joined!")
		strmsg := string(msg)
		ids := strings.Split(strmsg, "\n")
		ipaddr := ids[0]
		hashval := hash(ipaddr)
		lenAllm := len(Allmembers)
		var index int
		for index = 0; index < lenAllm; index++ {
			if Allmembers[index].value < hashval {
				continue
			} else {
				temp := make([]hashRecord, 1)
				temp[0] = hashRecord{ipaddr, hashval}
				Allmembers = append(Allmembers[:index], append(temp, Allmembers[index:]...)...)
				break
			}
		}
		fmt.Println(Allmembers)
		if index == len(Allmembers) {
			Allmembers = append(Allmembers, hashRecord{ipaddr, hashval})
		}
		UpdateLocalRecord()
		lenAllm = len(Allmembers)
		connW, err := net.DialUDP("udp", nil, &net.UDPAddr{addr.IP, 8000, ""})
		if err != nil {
			log.Fatal(err)
		}
		msgSend := "Membership\n" + Allmembers[(index+lenAllm-2)%lenAllm].ip + "\n" + Allmembers[(index+lenAllm-1)%lenAllm].ip + "\n" + Allmembers[(index+1)%lenAllm].ip + "\n" + Allmembers[(index+2)%lenAllm].ip + "\n"
		for _, obj := range Allmembers {
			msgSend += obj.ip
			msgSend += "\n"
		}
		_, err = connW.Write([]byte(msgSend))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("MSG SENT")
	}
}

/*
  Helper function for handle rejoin of introducer
*/
func HelpMemberUpdate(ipaddr string, seTemp *net.UDPConn) {
	if ipaddr == introducer {
		wg_memberUpdate.Done()
		return
	}
	UDPAddr, err := net.ResolveUDPAddr("udp4", ipaddr+PORT)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, UDPAddr)
	if err != nil {
		log.Fatal(err)
	}
	conn.Write([]byte("Alive\n"))
	c1 := make(chan bool, 1)
	buffer := make([]byte, 100)
	go func() {
		_, _ = seTemp.Read(buffer)
		c1 <- true
	}()
	select {
	case <-c1:
		{
			mux_memberList.Lock()
			hashval := hash(ipaddr)
			lenAllm := len(Allmembers)
			var index int
			for index = 0; index < lenAllm; index++ {
				if Allmembers[index].value < hashval {
					continue
				} else {
					temp := make([]hashRecord, 1)
					temp[0] = hashRecord{ipaddr, hashval}
					Allmembers = append(Allmembers[:index], append(temp, Allmembers[index:]...)...)
					break
				}
			}
			if index == len(Allmembers) {
				Allmembers = append(Allmembers, hashRecord{ipaddr, hashval})
			}
			mux_memberList.Unlock()
		}
	case <-time.After(1 * time.Second):
		{
			//This member quit or fail when the introducer is not in virtual ring
		}
	}
	wg_memberUpdate.Done()
}

/*
  Update Allmembers when introducer rejoins
*/
func TryUpdateMember(f *os.File, seTemp *net.UDPConn) {
	fi, _ := f.Stat()
	fisize := fi.Size()
	if fisize == 0 {
		return
	}
	var strs []string
	reader := bufio.NewReader(f)
	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if str != "" {
			str = str[:len(str)-1]
			strs = append(strs, str)
		}
	}
	for _, ipaddr := range strs {
		wg_memberUpdate.Add(1)
		go HelpMemberUpdate(ipaddr, seTemp)
	}
	wg_memberUpdate.Wait()
}

func main() {
	//Handle rejoin or start a new virtual ring
	Allmembers = append(Allmembers, hashRecord{introducer, hash(introducer)})
	joinList[introducer] = 0
	//First add introducer into the virtual ring
	Utemp, err := net.ResolveUDPAddr("udp4", "0.0.0.0:8080")
	if err != nil {
		log.Fatal(err)
	}
	seTemp, err := net.ListenUDP("udp", Utemp)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.OpenFile("./memberlist.txt", os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	TryUpdateMember(f, seTemp)
	seTemp.Close()
	UpdateLocalRecord()
	localIP = GetIpAddr()
	fmt.Println(Allmembers)
	if len(Allmembers) == 1 {
		MemberList = [4]string{localIP, localIP, localIP, localIP}
	} else {
		lenAllm := len(Allmembers)
		for i, tmp := range Allmembers {
			if tmp.ip == introducer {
				MemberList = [4]string{Allmembers[(i+lenAllm-2)%lenAllm].ip, Allmembers[(i+lenAllm-1)%lenAllm].ip, Allmembers[(i+1)%lenAllm].ip, Allmembers[(i+2)%lenAllm].ip}
			}
		}
		JoinRing()
		ConnUpdate()
	}
	//Now the introducer is done with memberlist and Allmembers

	//Now set up the introducer
	time_base, _ = time.Parse("2006-01-02 15:04:05.0000 -0500 CDT", "2019-10-05 15:04:05.0000 -0500 CDT")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	go signal_handler(sigs)
	go introduce()
	wg_signal.Add(2)
	go heartbeat()
	go PrintMemberList()
	go FailDetector()
	//Now prepare to recv heartbeats and introduce request
	channel <- ("Join\n" + localIP + "\n" + strconv.Itoa(int(time.Now().Sub(time_base).Seconds())) + "\n")

	Ulisten, err := net.ResolveUDPAddr("udp4", "0.0.0.0:8080")
	if err != nil {
		log.Fatal(err)
	}
	se, err = net.ListenUDP("udp", Ulisten)
	if err != nil {
		log.Fatal(err)
	}
outer:
	for {
		//infinite loop, waiting for messages
		select {
		case <-quitNow:
			break outer
		default:
		}
		buffer := make([]byte, 1024)
		num, IP, err := se.ReadFromUDP(buffer)
		if err != nil {
			fmt.Print("Read Fail")
		}
		if num != 0 {
			go HandleConn(IP.String(), buffer)
		}
	}
}
