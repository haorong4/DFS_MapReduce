package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup
var mutex = &sync.Mutex{}
var lineCount int = 0


/*
	This function reads the local IP address

	Return
		The local IP address
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
	This function sends the test request to the remote machine

	Parameters
		ipAddr: 	the IP address of the request destination
		args: 		the test arguments
		vm_num: 	the VM number
*/
func GrepRemote(ipAddr string, args []string, vm_num int) {

	defer wg.Done()
<<<<<<< HEAD
	command := "grep\n" +  strings.Join(args, " ") +"\n"
=======
	command := "grep\n" + "\"" + strings.Join(args, " ") + "\"\n"
>>>>>>> 213c020d149698c11d0383a89b221230bfb1d4c7

	conn, err := net.Dial("tcp", ipAddr+":8080")
	if err != nil {
		// fmt.Print("fail to connect with IP: ", ipAddr, " No.", num, "\n")
		return
	}
	fmt.Printf("connected to %s\n", conn.RemoteAddr().String())
	fmt.Fprintf(conn, command)
	reader := bufio.NewReader(conn)
	num_str := strconv.Itoa(vm_num)
	machineName := ipAddr + " No." + num_str + " Line"

	if err != nil {
		fmt.Print("fail to receive data from IP:", ipAddr, " No.", vm_num, "\n")
		return
	}
<<<<<<< HEAD
	count := 0
	
=======
	var count int = 0
>>>>>>> 213c020d149698c11d0383a89b221230bfb1d4c7
	mutex.Lock()
	for {
		fmt.Println("here")
		line, err := reader.ReadString('\n')
		if err != nil {

			break
		}
<<<<<<< HEAD
		count++
		line += ""
		machineName += ""
		//fmt.Printf("%s %s", machineName, line)
	}
	fmt.Printf("\nVM %d, line count: %d \n\n", vm_num, count)
=======
		count += 1
		fmt.Printf("%s %s", machineName, line)
	}
	lineCount += count
>>>>>>> 213c020d149698c11d0383a89b221230bfb1d4c7
	mutex.Unlock()
}

/*
	This function runs grep test on local machine

	Parameters
		args: 		the test arguments
		file_name: 	the name of the file which stores the correct test result (for function compareResult)
		vm_num: 	the VM number
*/
func GrepLocal(args []string, vm_num int) {
	defer wg.Done()

	fmt.Println("No.", vm_num, " local log file:")
	pattern := strings.Join(args, " ")
	input := "cat ../machine.i.log | grep -n " + pattern
	cmd := exec.Command("bash", "-c", input)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("fail to execute command")
		return
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(stdout)
	output := buf.String()
	fmt.Println(output)
}

/*
	This function reads the IP address from a local file, which stores all the IP addresses of vms

	Return
		An array of IP addresses for all the VMs
*/
func ReadIps() []string {
	file, err := os.Open("ips.txt")
	if err != nil {

		fmt.Print("Can not open ip addresses file\n")
		log.Fatal(err)
	}
	defer file.Close()
	ipAddr := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ipAddr = append(ipAddr, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Print("Can not read ip addresses\n")
	}
	return ipAddr
}

func main() {
	start := time.Now()
	if len(os.Args) <= 2 {
		fmt.Println("Usage: go run client.go grep pattern")
		return
	}
	if os.Args[1] != "grep" {
		fmt.Println("Usage: go run client.go grep pattern")
		return
	}
	local := GetIpAddr()
	args := os.Args[2:]
	fmt.Println(args)
	ipAddr := ReadIps()
	wg.Add(len(ipAddr))
	for i := range ipAddr {
		if local == ipAddr[i] {
			//go GrepLocal(args, i+1)
			go GrepRemote(ipAddr[i], args, i+1)
		} else {
			go GrepRemote(ipAddr[i], args, i+1)
		}
	}
	wg.Wait()
	fmt.Printf("Total %d lines receivec\n", lineCount)
	fmt.Printf("Used %v\n", time.Since(start))

}
