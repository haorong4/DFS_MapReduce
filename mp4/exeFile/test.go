package main

import (
	"bufio"
	"bytes"

	// "fmt"
	"io"
	"log"
	"os"
	"os/exec"
)

// func main() {
// 	fd, _ := os.Open("../wordFiles/file1")
// 	reader := bufio.NewReader(fd)

// 	for {
// 		line, _, err := reader.ReadLine()
// 		// fmt.Println(string(line))
// 		if err != nil {
// 			break
// 		}

// 		cmd := exec.Command("bash", "-c", "./a.out")
// 		stdin, err := cmd.StdinPipe()
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		stdout, err := cmd.StdoutPipe()
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		cmd.Start()
// 		io.WriteString(stdin, string(line))

// 		stdin.Close()
// 		//fmt.Println("here")
// 		//cmd.Wait()
// 		buffer := make([]byte, 1000)
// 		bytenum, _ := io.ReadFull(stdout, buffer)
// 		// fmt.Println(string(buffer))
// 		buffer = buffer[:bytenum]

// 		KVpairs := bytes.Split(buffer, []byte("\n"))
// 		// fmt.Println(KVpairs)
// 		for _, pair := range KVpairs[:len(KVpairs)-1] {
// 			entries := bytes.Split(pair, []byte(" "))
// 			fd, _ := os.OpenFile("haha"+string(entries[0]), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
// 			//fmt.Println(string(entries[0]) + " " + string(entries[1]))
// 			fd.Write(entries[0])
// 			fd.Write([]byte(" "))
// 			fd.Write(entries[1])
// 			fd.Write([]byte("\n"))

// 		}
// 		stdout.Close()
// 		cmd.Wait()
// 	}

// 	//buffer, _ := cmd.Output();

// }

func main() {
	fd, _ := os.Open("hahason")
	reader := bufio.NewReader(fd)
	cmd := exec.Command("bash", "-c", "./jwc")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	cmd.Start()
	for {
		line, _, err := reader.ReadLine()
		io.WriteString(stdin, string(line)+"\n")
		// fmt.Println(string(line))
		if err != nil {
			break
		}
	}

	stdin.Close()
	//fmt.Println("here")
	//cmd.Wait()
	buffer := make([]byte, 1000)
	bytenum, _ := io.ReadFull(stdout, buffer)
	// fmt.Println(string(buffer))
	buffer = buffer[:bytenum]

	KVpairs := bytes.Split(buffer, []byte("\n"))
	// fmt.Println(KVpairs)
	for _, pair := range KVpairs[:len(KVpairs)-1] {
		entries := bytes.Split(pair, []byte(" "))
		fd, _ := os.OpenFile("kuice", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		// fmt.Println(string(entries[0]) + " " + string(entries[1]))
		fd.Write(entries[0])
		fd.Write([]byte(" "))
		fd.Write(entries[1])
		fd.Write([]byte("\n"))

	}
	stdout.Close()
	cmd.Wait()
	//buffer, _ := cmd.Output();

}
