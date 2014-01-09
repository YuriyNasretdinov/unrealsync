// +build darwin

package main

import (
	"bufio"
	"os"
	"os/exec"
	"time"
)

const (
	LOCAL_WATCHER_READY = "Initialized"
)

func runFsChangesThread(path string) {
	notifyPath := unrealsyncDir + "/notify-darwin"

	cmd := exec.Command(notifyPath, path)
	debugLn(notifyPath, path)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fatalLn("Cannot get stdout pipe: ", err.Error())
	}

	cmd.Stdin, err = os.Open("/dev/null")
	cmd.Stderr = os.Stderr

	if err = cmd.Start(); err != nil {
		panic("Cannot start notify: " + err.Error())
	}

	defer cmd.Wait()

	r := bufio.NewReader(stdout)

	time.Sleep(time.Second)

	fschanges <- LOCAL_WATCHER_READY

	for {
		lineBytes, _, err := r.ReadLine()
		if err != nil {
			fatalLn("Could not read line from notify utility: " + err.Error())
		}

		line := string(lineBytes)

		if line == "-" {
			continue
		}

		fschanges <- line[2:]
	}
}
