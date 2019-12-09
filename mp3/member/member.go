package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
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

//

const TIMEOUT = 3000.0
const HEARTBEAT = 300.0

const introducer = "172.22.152.46"
const PORT = ":8080"
const INTROPORT = ":8000"
const HEARTPORT = ":8088"
const FILEPORT = ":8888"

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
var mutex_Members sync.Mutex
var mutex_conns sync.Mutex
var wg_heartbeat sync.WaitGroup
var wg_signal sync.WaitGroup
var wg_putRemote sync.WaitGroup
var wg_replica sync.WaitGroup

//channels
var quitNow = make(chan bool)
var channel = make(chan string, 300)
var failDone = make(chan bool)
var commandChannel = make(chan string)
var replicaChannel = make(chan string)
var cChoice = make(chan string)

//file vars
var fileMap = make(map[string][]fileStatus)
var RLmap = make(map[string]string)

//master
var master string = "172.22.154.42"

//last Index
var lastIdx int = 2

type hashRecord struct {
	ip    string
	value uint32
}

type fileStatus struct {
	ip      string
	version int
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
func GenerateTimestamp() int {
	return int(time.Now().Sub(time_base).Seconds())
}


/*
	this function will choose the next master for this system, if the current master fails
*/
func NewElection() {
	if len(Allmembers) != 0 {
		master = Allmembers[0].ip
	} else {
		master = "0"
	}
	if master == localIP {
		fmt.Println("I am the new Leader!")
	}
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
	if fail_ip == localIP {
		return
	}
	mutex_FailList.Lock()
	for key, val := range FailList {
		if key == fail_ip && (math.Abs(float64(time_off-val)) < 5.0) {
			FailList[fail_ip] = time_off
			mutex_FailList.Unlock()
			return
		}
	}
	for i, v := range MemberList {
		if v == fail_ip {
			delete(TimeTable, fail_ip)
			FixRing(i)
			break
		}
	}
	FailList[fail_ip] = time_off
	mutex_FailList.Unlock()
	joinList[fail_ip] = 1000
	delete(joinList, fail_ip)
	fmt.Println(fail_ip, " fails at ", time_off)
	msg := "Fail\n" + fail_ip + "\n" + strconv.Itoa(time_off) + "\n"
	channel <- msg

	mutex_Members.Lock()
	for i, tmp := range Allmembers {
		if tmp.ip == fail_ip {
			copy(Allmembers[i:], Allmembers[i+1:])
			Allmembers[len(Allmembers)-1] = hashRecord{"", 0}
			Allmembers = Allmembers[:len(Allmembers)-1]
			break
		}
	}
	lastIdx = lastIdx % len(Allmembers)
	mutex_Members.Unlock()

	if fail_ip == master {
		NewElection()
	}
}

/*
	Updating join information
		join_ip: the IP address of the joining machine
		time_off: the time stamp of the failure
	if this join is new, pushing it to all its neighbors,
	if it have receive the same join message before, block the message
*/

func JoinUpdate(join_ip string, time_off int) {
	for key, val := range joinList {
		if key == join_ip && (math.Abs(float64(time_off-val)) < 10.0) {
			joinList[join_ip] = time_off
			return
		}
	}

	hashval := hash(join_ip)
	mutex_Members.Lock()
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
	mutex_Members.Unlock()

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
	mutex_Members.Lock()
	length := len(Allmembers)
	fail_ip := MemberList[index]
	fmt.Println("FIX RING!! " + fail_ip)
	if index == 0 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == fail_ip {
				new = Allmembers[(i+length-1)%length].ip
			}
		}
		MemberList[0] = new
	} else if index == 1 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == fail_ip {
				new = Allmembers[(i+length-2)%length].ip
			}
		}
		MemberList[1] = MemberList[0]
		MemberList[0] = new
	} else if index == 2 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == fail_ip {
				new = Allmembers[(i+2)%length].ip
			}
		}
		MemberList[2] = MemberList[3]
		MemberList[3] = new
	} else if index == 3 {
		new := "0"
		for i, hash := range Allmembers {
			if hash.ip == fail_ip {
				new = Allmembers[(i+1)%length].ip
			}
		}
		MemberList[3] = new
	}

	for i, hash := range Allmembers {
		if hash.ip == fail_ip {
			copy(Allmembers[i:], Allmembers[i+1:])
			Allmembers[len(Allmembers)-1] = hashRecord{"", 0}
			Allmembers = Allmembers[:len(Allmembers)-1]
			break
		}
	}
	mutex_Members.Unlock()
	ConnUpdate()
	MembershipUpdate([]string{})
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
	// fmt.Println(IP)
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
	case "FileUpdate":
		syncFileMap(command[1], command[2:len(command)-1], false)
	}
}

/*
	this function is used to check if any file needs to be re-replicated, if yes, message will be sent to commnad
	handler directly for further process, this function will only be accessed by the current master. 
*/
func CheckReplica() {
	time_now := GenerateTimestamp()
	relocat_list := make(map[string]int)
	mutex_FailList.Lock()
	for fail_ip, v := range FailList {
		if time_now-v >= 6 {
			continue
		}
		for key, val := range fileMap {
			temp := len(val) - 4
			for i, status := range val {
				if status.ip == fail_ip {
					relocat_list[key] += 1
					temp := append(val[:i], val[i+1:]...)
					fileMap[key] = temp
					break
				}
			}
			if temp > 0 {relocat_list[key] -= temp} 
		}
	}
	mutex_FailList.Unlock()
	for key, val := range relocat_list {
		temp := fileMap[key]
		for _, status := range temp {
			if status.ip != "0" {
				msg := "r" + status.ip + " " + key + " " + strconv.Itoa(val) + "\n"
				// fmt.Println("CheckReplica: " + msg)
				replicaChannel <- msg
				break
			}
		}
	}
}

/*
	this is the Failure detector running in the background,
	it keeps tracking all the entry on Time table,
	Once indentify failures, it will update the failist and push
	the message to the node's neighbors
*/
func FailDetector() {
	counter := 0
outer:
	for {
		select {
		case <-failDone:
			break outer
		default:
		}
		time.Sleep(time.Millisecond * 100)
		counter = (counter + 1) % 51
		for key, value := range TimeTable {
			time_diff := time.Now().Sub(value).Nanoseconds() / 1000000
			if time_diff >= TIMEOUT {
				// IP key failed !!!
				for i, v := range MemberList {
					if v == key {
						delete(TimeTable, key)
						// MemberList[i] = "0"
						// conns[i] = nil
						FixRing(i)
						break
					}
				}
				FailUpdate(key, int(time.Now().Sub(time_base).Seconds()))
			}
		}
		if master == localIP && counter >= 50 {
			counter = 0
			CheckReplica()

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
	this function takes care of the command enter from the stdin
	command:
		1: "ml": Print membership list
		2: "put [local file] [remote file]": Put file
		3: "get [remote file] [local file]": Get file
		4: "delete [remote file]": Delete file
		5: "ls [remote file]": List replicas's ip addr
		6: "store": List current files on local machine
*/
func CommandHandler() {
	for {
		select {
		case text := <-commandChannel:
			text = text[:len(text)-1]
			switch text[0] {
			case 'm':
				fmt.Println("Membership list:")
				fmt.Println(Allmembers)
				fmt.Println(MemberList)
			case 'p':
				putToRemote(text)
			case 'g':
				getFromRemote(text)
			case 'd':
				deleteRemote(text)
			case 'l':
				cmds := strings.Split(text, " ")
				ipList := fileMap[cmds[1]]
				fmt.Println(ipList)
			case 's':
				files, err := ioutil.ReadDir("files")
				if err != nil {
					log.Fatal(err)
				}
				for _, file := range files {
					fmt.Println(file.Name())
				}
			default:
				continue
			}
		case text := <-replicaChannel:
			text = text[:len(text)-1]
			RecoverReplica(text[1:])
		}
	}
}

func RealCommandHandler() {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, err := reader.ReadString('\n')
		if err == nil {
			switch text {
			case "y\n":
				cChoice <- "y\n"
			case "n\n":
				cChoice <- "n\n"
			default:
				commandChannel <- text
			}
		}
	}
}

/*
	this function receives command to re-replicate, it will pick one living replica, give it the ip addresses 
	for re-repicated, and ask the replica to transport certain file to these addresses.
*/
func RecoverReplica(text string) {
	cmds := strings.Split(text, " ")
	// fmt.Println("RecoverReplica: ", cmds)
	number, _ := strconv.Atoi(cmds[2])
	message := "Replica\n" + cmds[1] + "\n"
	new_list := fileMap[cmds[1]]
	version := new_list[0].version

	mutex_Members.Lock()
	for i := 0; i < number; i++ {
		ip := Allmembers[lastIdx].ip
		lastIdx = (lastIdx + 1) % len(Allmembers)
		same := false
		for _, old := range new_list {
			if old.ip == ip {
				same = true
				break
			}
		}
		if same {
			i--
			continue
		}
		new_list = append(new_list, fileStatus{ip, version})
		message += ip + " "
	}
	mutex_Members.Unlock()

	fileMap[cmds[1]] = new_list
	conn, err := net.Dial("tcp", cmds[0]+FILEPORT)
	if err != nil {
		fmt.Println("open conn failed")
		os.Exit(1)
	}
	conn.Write([]byte(message + "\n"))
	conn.Close()
	syncFileMap(cmds[1], []string{}, true)
	// setup tcp connection to cmd[0]
}

/*
  Conn types:
	1:
		in: "Query\n\[Get or Put or Delete]\n[REMOTE FILENAME]\n"
		out: Get: found: "[ip!version]\n...\n"
			 Put: found: "[ip]\n...\n"
			 Delete: send delete msg to all replicas
			 not found: "not found\n"
	2:
		in: "Get\n[REMOTE FILENAME]\n"
		out: found: "[file_length]\n[The requested file]"
			not found: "not found\n"
	3:
		in: "PUT\n[REMOTE FILENAME]\n[LOCAL FILENAME]\n[file_length]\n[data]"
		out: nothing yet
	4:
		in: "DELETE\n[FILENAME]\n"
		out: nothing yet

*/
func putToRemote(text string) {
	cmds := strings.Split(text, " ")
	if fList, ok := fileMap[cmds[2]]; !ok {
		conn, _ := net.Dial("tcp", master+FILEPORT)
		//Put a new file
		conn.Write([]byte("Query\nPut\n" + cmds[2] + "\n"))
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		conn.Close()
		ipss := string(buffer)
		ips := strings.Split(ipss, "\n")
		for _, ip := range ips[:len(ips)-1] {
			wg_putRemote.Add(1)
			go helperPut(cmds[1:], ip, false)
		}
		conn.Close()
		wg_putRemote.Wait()
	} else {
		//Update a existing file
		currentTime := GenerateTimestamp()
		flag := true
		for _, fs := range fList {
			if currentTime-fs.version <= 60 {
				fmt.Println("Do you really want to do this? (y/n)")
				select {
				case <-time.After(30 * time.Second):
					flag = false
				case str := <-cChoice:
					if str == "y\n" {
						flag = true
					} else {
						flag = false
					}
				}
				break
			}
		}
		fmt.Println("here")
		if flag == false {
			return
		}
		conn, _ := net.Dial("tcp", master+FILEPORT)
		conn.Write([]byte("Query\nPut\n" + cmds[2] + "\n"))
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		conn.Close()
		ipss := string(buffer)
		ips := strings.Split(ipss, "\n")
		for _, ip := range ips[:len(ips)-3] {
			wg_putRemote.Add(1)
			go helperPut(cmds[1:], ip, false)
		}
		//Now the special machine
		rFile := cmds[2]
		lFile := cmds[1]
		f, _ := os.OpenFile(lFile, os.O_CREATE|os.O_RDWR, 0666)
		fs, _ := f.Stat()
		length := fs.Size()
		conn, err := net.Dial("tcp", ips[len(ips)-3]+FILEPORT)
		if err != nil {
			fmt.Println("open conn failed")
			os.Exit(1)
		}
		conn.Write([]byte("PutAndSync\n" + ips[len(ips)-2] + "\n" + rFile + "\n" + lFile + "\n" + strconv.Itoa(int(length)) + "\n"))
		bytesWrite := 0
		for {
			bRead := 1024
			if int(length)-bytesWrite < 1024 {
				bRead = int(length) - bytesWrite
			}
			buffer := make([]byte, bRead)
			f.Read(buffer)
			tmp, _ := conn.Write(buffer)
			bytesWrite += tmp
			if bytesWrite >= int(length) {
				break
			}
		}
		conn.Close()
		f.Close()
		//
		wg_putRemote.Wait()
	}
}

func helperPut(cmds []string, ip string, replica bool) {
	rFile := cmds[1]
	lFile := cmds[0]
	if replica {
		cmds[0] = "files/" + cmds[0]
	}
	f, _ := os.OpenFile(cmds[0], os.O_CREATE|os.O_RDWR, 0666)
	fs, _ := f.Stat()
	length := fs.Size()
	conn, err := net.Dial("tcp", ip+FILEPORT)
	if err != nil {
		fmt.Println("open conn failed")
		os.Exit(1)
	}
	conn.Write([]byte("Put\n" + rFile + "\n" + lFile + "\n" + strconv.Itoa(int(length)) + "\n"))
	bytesWrite := 0
	for {
		bRead := 1024
		if int(length)-bytesWrite < 1024 {
			bRead = int(length) - bytesWrite
		}
		buffer := make([]byte, bRead)
		f.Read(buffer)
		tmp, _ := conn.Write(buffer)
		bytesWrite += tmp
		if bytesWrite >= int(length) {
			break
		}
	}
	conn.Close()
	f.Close()
	wg_putRemote.Done()
}

func getFromRemote(text string) {
	cmds := strings.Split(text, " ")
	if _, ok := fileMap[cmds[1]]; !ok {
		fmt.Println("NOT FOUND!")
		return
	}
	conn, err := net.Dial("tcp", master+FILEPORT)
	if err != nil {
		fmt.Println("open conn failed")
		os.Exit(1)
	}
	conn.Write([]byte("Query\nGet\n" + cmds[1] + "\n"))
	buffer := make([]byte, 1024)
	conn.Read(buffer)
	conn.Close()
	ipss := string(buffer)
	ips := strings.Split(ipss, "\n")
	var ip string
	var version int = 0
	for _, tmp := range ips[:len(ips)-1] {
		tmps := strings.Split(tmp, "!")
		cur, _ := strconv.Atoi(tmps[1])
		if cur > version {
			version = cur
			ip = tmps[0]
		}
	}
	conn, err = net.Dial("tcp", ip+FILEPORT)
	if err != nil {
		fmt.Println("open conn failed")
		os.Exit(1)
	}
	conn.Write([]byte("Get\n" + cmds[1] + "\n"))
	f, err := os.OpenFile("files/"+cmds[2], os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("err open file")
		os.Exit(1)
	}
	for {
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		if err == io.EOF {
			break
		}
		buffer = buffer[:bytesRead]
		f.Write(buffer)
	}
	f.Close()
	conn.Close()
}

func deleteRemote(text string) {
	cmds := strings.Split(text, " ")
	conn, err := net.Dial("tcp", master+FILEPORT)
	if err != nil {
		fmt.Println("open conn failed")
		os.Exit(1)
	}
	conn.Write([]byte("Query\nDelete\n" + cmds[1] + "\n"))
	conn.Close()
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

/*
 	this function is used to synchronize the file map in the system
		filename: the name of the file which needs update
		new_status: the new information for this file
		master_call: a boolean indicates whether this function call is from a master ot not
	Compares new information to the local info, if any line of information for this file is new,
	pushing it to all its neighbors.
	if all the incoming info is older or equal to local info, block the message.
*/
func syncFileMap(filename string, new_status []string, master_call bool) {
	if !master_call {
		check_list := make(map[string]int)
		for _, v := range new_status {
			pair := strings.Split(v, " ")
			ver, _ := strconv.Atoi(pair[1])
			check_list[pair[0]] = ver
		}
		if val, exist := fileMap[filename]; exist {
			old_info := true
			for _, status := range val {
				if v, ok := check_list[status.ip]; ok {
					if status.version != v {
						check_list[status.ip] = int(math.Max(float64(v), float64(status.version)))
						old_info = false
					}
				} else {
					old_info = false
				}
			}
			if len(val) != len(new_status) {
				old_info = false
			}
			if old_info {
				return
			}
		} else {
			if len(new_status) == 0 {
				return
			}
		}
		fmt.Println("syncFileMap: member:\n")
		fmt.Println(new_status)
		update := []fileStatus{}
		for key, val := range check_list {
			update = append(update, fileStatus{key, val})
		}
		fileMap[filename] = update
		if len(update) == 0 {
			delete(fileMap, filename)
		}

	}

	new_status = []string{}
	for _, status := range fileMap[filename] {
		temp := status.ip + " " + strconv.Itoa(status.version)
		new_status = append(new_status, temp)
	}
	if master_call {
		fmt.Println("syncFileMap: master:\n")
		fmt.Println(new_status)
	}

	msg := "FileUpdate\n" + filename + "\n"
	for _, str := range new_status {
		msg += str + "\n"
	}
	channel <- msg
}

/*
  Listening for TCP connections
*/
func beginServer() {
	l, err := net.Listen("tcp", FILEPORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Conn from ", conn.RemoteAddr().String())
		// Handle connections in a new goroutine.
		go handleTCPConn(conn)
	}
}

/*
  Conn types:
	1:
		in: "Query\n\[Get or Put or Delete]\n[REMOTE FILENAME]\n"
		out: Get: found: "[ip!version]\n...\n"
			 Put: found: "[ip]\n...\n"
			 Delete: send delete msg to all replicas
			 not found: "not found\n"
	2:
		in: "Get\n[REMOTE FILENAME]\n"
		out: found: "[file_length]\n[The requested file]"
			not found: "not found\n"
	3:
		in: "PUT\n[REMOTE FILENAME]\n[LOCAL FILENAME]\n[file_length]\n[data]"
		out: nothing yet
	4:
		in: "DELETE\n[FILENAME]\n"
		out: nothing yet

*/
func handleTCPConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	bytes, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Println("read err", err.Error())
		os.Exit(1)
	}
	msg := string(bytes)
	switch msg {
	case "Query\n":
		masterQuery(conn, reader)
	case "Get\n":
		getFile(conn, reader)
	case "Put\n":
		putFile(conn, reader)
	case "PutAndSync\n":
		putAndSync(conn, reader)
	case "Delete\n":
		deleteFile(conn, reader)
	case "Replica\n":
		replicaPut(conn, reader)
	case "UpdateDone\n":
		bytes, _ := reader.ReadBytes('\n')
		bytes = bytes[:len(bytes)-1]
		filename := string(bytes)
		fList := fileMap[filename]
		fmt.Println("filemap", fileMap)
		fList[3].version = fList[2].version
		fileMap[filename] = fList
		syncFileMap(filename, []string{}, true)
	default:
		fmt.Println("ERR IN HANDLETCPCONN!!!")
	}
	conn.Close()
}

func masterQuery(conn net.Conn, reader *bufio.Reader) {
	typeT, _ := reader.ReadBytes('\n')
	typep := string(typeT[:len(typeT)-1])
	filenameT, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Println("Read from conn err")
		os.Exit(1)
	}
	filename := string(filenameT[:len(filenameT)-1])
	switch typep {
	case "Get":
		if fList, ok := fileMap[filename]; ok {
			msg := ""
			for _, fs := range fList {
				tmp := fs.ip + "!" + strconv.Itoa(fs.version) + "\n"
				msg += tmp
			}
			conn.Write([]byte(msg))
			var tmp fileStatus
			tmp.ip = strings.Split(conn.RemoteAddr().String(), ":")[0]
			tmp.version = fList[0].version
			fList = append(fList, tmp)
			fileMap[filename] = fList
			syncFileMap(filename, []string{}, true)
		} else {
			fmt.Println("file not found")
		}
	case "Put":
		currentVersion := GenerateTimestamp()
		if fList, ok := fileMap[filename]; ok {
			msg := ""
			for i, _ := range fList {
				if i != len(fList)-1 {
					fList[i].version = currentVersion
				}
				tmp := fList[i].ip + "\n"
				msg += tmp
			}
			fileMap[filename] = fList
			conn.Write([]byte(msg))
		} else {
			msg := ""
			for idx := 0; idx < 4; idx++ {
				if _, ok := fileMap[filename]; !ok {
					tmp := make([]fileStatus, 1)
					var ttmp fileStatus
					ttmp.ip = Allmembers[(idx+lastIdx)%len(Allmembers)].ip
					ttmp.version = currentVersion
					tmp[0] = ttmp
					fileMap[filename] = tmp
				} else {
					var ttmp fileStatus
					ttmp.ip = Allmembers[(idx+lastIdx)%len(Allmembers)].ip
					ttmp.version = currentVersion
					fileMap[filename] = append(fileMap[filename], ttmp)
				}
				msg += Allmembers[(idx+lastIdx)%len(Allmembers)].ip + "\n"
			}
			lastIdx += 4
			fmt.Println(msg)
			conn.Write([]byte(msg))
		}
		syncFileMap(filename, []string{}, true)
	case "Delete":
		if fList, ok := fileMap[filename]; ok {
			for _, fs := range fList {
				connD, err := net.Dial("tcp", fs.ip+FILEPORT)
				if err != nil {
					fmt.Println("err delete")
					os.Exit(1)
				}
				connD.Write([]byte("Delete\n" + filename + "\n"))
				connD.Close()
			}
			delete(fileMap, filename)
		} else {
			fmt.Println("file not found")
		}
		syncFileMap(filename, []string{}, true)
	default:
		fmt.Println("Command can't be understood")
	}
}

/*
	re-replicate a file to other members
*/

func replicaPut(conn net.Conn, reader *bufio.Reader) {
	// TODO: read more command from connection.
	rFileT, _ := reader.ReadBytes('\n')
	rFile := string(rFileT[0 : len(rFileT)-1])
	lFile := RLmap[rFile]

	ipss, _ := reader.ReadBytes('\n')
	ipss = ipss[0 : len(ipss)-1]
	ips := strings.Split(string(ipss), " ")
	for _, ip := range ips[:len(ips)-1] {
		fmt.Println("replicaPut " + ip + " local: " + lFile + " remote: " + rFile)
		wg_putRemote.Add(1)
		go helperPut([]string{lFile, rFile}, ip, true)
	}
	wg_putRemote.Wait()
}

func getFile(conn net.Conn, reader *bufio.Reader) {
	filenameT, _ := reader.ReadBytes('\n')
	filename := string(filenameT[:len(filenameT)-1])
	if filename, ok := RLmap[filename]; ok {
		f, err := os.OpenFile("files/"+filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			fmt.Println("Open failed")
			os.Exit(1)
		}
		fs, _ := f.Stat()
		length := int(fs.Size())
		bytesWrite := 0
		for {
			bRead := 1024
			if length-bytesWrite < 1024 {
				bRead = length - bytesWrite
			}
			buf := make([]byte, bRead)
			f.Read(buf)
			tmp, _ := conn.Write(buf)
			bytesWrite += tmp
			if bytesWrite >= length {
				break
			}
		}

	} else {
		fmt.Println("file not found")
	}
}

func putFile(conn net.Conn, reader *bufio.Reader) {
	rFileT, _ := reader.ReadBytes('\n')
	lFileT, _ := reader.ReadBytes('\n')
	rFileT = rFileT[:len(rFileT)-1]
	lFileT = lFileT[:len(lFileT)-1]
	rFile := string(rFileT)
	lFile := string(lFileT)
	if fileTmp, ok := RLmap[rFile]; ok {
		//This is a update
		os.Remove("files/" + fileTmp)
		RLmap[rFile] = lFile
	} else {
		//This is a put
		RLmap[rFile] = lFile
	}
	f, err := os.OpenFile("files/"+lFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("err open file", err.Error())
		os.Exit(1)
	}
	lengthT, _ := reader.ReadBytes('\n')
	length, _ := strconv.Atoi(string(lengthT))
	fmt.Println("length", length) //This should be used....
	for {
		buffer := make([]byte, 1024)
		tmp, err := reader.Read(buffer)
		buffer = buffer[:tmp]
		if err == io.EOF {
			break
		}
		f.Write(buffer)
	}
}

func putAndSync(conn net.Conn, reader *bufio.Reader) {
	otherIPT, _ := reader.ReadBytes('\n')
	rFileT, _ := reader.ReadBytes('\n')
	lFileT, _ := reader.ReadBytes('\n')
	otherIPT = otherIPT[:len(otherIPT)-1]
	rFileT = rFileT[:len(rFileT)-1]
	lFileT = lFileT[:len(lFileT)-1]
	otherIP := string(otherIPT)
	rFile := string(rFileT)
	lFile := string(lFileT)
	if _, ok := RLmap[rFile]; ok {
		//This is a update
	} else {
		//This is a put
		RLmap[rFile] = lFile
	}
	f, err := os.OpenFile(lFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("err open file", err.Error())
		os.Exit(1)
	}
	lengthT, _ := reader.ReadBytes('\n')
	length, _ := strconv.Atoi(string(lengthT))
	fmt.Println("length", length) //This should be used....
	for {
		buffer := make([]byte, 1024)
		tmp, err := reader.Read(buffer)
		buffer = buffer[:tmp]
		if err == io.EOF {
			break
		}
		f.Write(buffer)
	}
	f.Close()
	wg_putRemote.Add(1)
	helperPut([]string{lFile, rFile}, otherIP, false)
	wg_putRemote.Wait()
	connM, _ := net.Dial("tcp", master+FILEPORT)
	connM.Write([]byte("UpdateDone\n" + rFile + "\n"))
	connM.Close()
}

func deleteFile(conn net.Conn, reader *bufio.Reader) {
	filenameT, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Println("Read from conn err")
		os.Exit(1)
	}
	filename := string(filenameT[:len(filenameT)-1])
	lFilename := RLmap[filename]
	os.Remove("files/" + lFilename)
}

func main() {
	//remove old file folder, if any, then create a new file folder
	err := os.RemoveAll("files")
	if err != nil {
		fmt.Println("err removing file  foler")
		os.Exit(1)
	}
	err = os.Mkdir("files", 0777)
	if err != nil {
		fmt.Println("err creating file  foler")
		os.Exit(1)
	}
	//setup environment variables
	time_base, _ = time.Parse("2006-01-02 15:04:05.0000 -0500 CDT", "2019-10-29 10:04:05.0000 -0500 CDT")
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
	joinMsg = joinMsg + GetIpAddr() + "\n" + "1" + "\n"
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
	// fmt.Println(members)
	MembershipUpdate(members[1:5])
	for _, v := range members[5 : len(members)-1] {
		// fmt.Println(v)
		if _, ok := joinList[v]; ok {
			continue
		}
		joinList[v] = 0
		hashval := hash(v)
		lenAllm := len(Allmembers)
		var index int
		for index = 0; index < lenAllm; index++ {
			if Allmembers[index].value < hashval {
				continue
			} else {
				if v == Allmembers[index].ip {
					break
				}
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
	//server listening
	go beginServer()
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
	go CommandHandler()
	go RealCommandHandler()
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
