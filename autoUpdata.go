package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var wg sync.WaitGroup

var config [10]string = [10]string{
	"haoyuz3@fa19-cs425-g13-01.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-02.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-03.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-04.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-05.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-06.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-07.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-08.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-09.cs.illinois.edu",
	"haoyuz3@fa19-cs425-g13-10.cs.illinois.edu",
}

func update(addr string, command string) {
	defer wg.Done()
	cmd := exec.Command("ssh", addr, command)
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	cmd.Wait()
}

func main() {
	args := os.Args[1:]
	command := strings.ReplaceAll(strings.Join(args, " "), "=", ";")
	fmt.Print(command)
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go update(config[i], command)
	}
	wg.Wait()
}
