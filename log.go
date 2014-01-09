package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	READ_STATUS_OK = iota
	READ_STATUS_NO_DATA
	READ_STATUS_SKIP_ENTRY
	READ_STATUS_FAILURE
)

var (
	outLogFp    *os.File
	outLogName  string
	outLogMutex sync.Mutex
	outLogPos   int64
)

func initializeLogs() {
	var err error

	outLogName = REPO_LOG_OUT + "out.log"
	outLogFp, err = os.OpenFile(outLogName, os.O_APPEND|os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fatalLn("Cannot open ", outLogName, ": ", err.Error())
	}
}

func writeToOutLog(action string, buf []byte, key string) (err error) {
	outLogMutex.Lock()
	defer outLogMutex.Unlock()
	_, err = fmt.Fprintf(outLogFp, "%10d%s%s%10d%s", len(key), key, action, len(buf), buf)
	if err != nil {
		return
	}

	outLogPos, err = outLogFp.Seek(0, os.SEEK_CUR)
	return
}

func getOutLogPosition() (n int64) {
	outLogMutex.Lock()
	n, _ = outLogFp.Seek(0, os.SEEK_CUR)
	outLogMutex.Unlock()
	return
}

func doSendChanges(stream io.Writer, pos int64, key string) (err error) {
	fp, err := os.Open(outLogName)
	if err != nil {
		return
	}
	defer fp.Close()

	if _, err = fp.Seek(pos, os.SEEK_SET); err != nil {
		return
	}

	buf := make([]byte, MAX_DIFF_SIZE+20) // MAX_DIFF_SIZE limits only diff itself, so extra action+len required, each 10 bytes
	lenbuf := make([]byte, 10)

	for {
		var localOutLogPos int64

		outLogMutex.Lock()
		localOutLogPos = outLogPos
		outLogMutex.Unlock()

		if pos == localOutLogPos {
			time.Sleep(time.Millisecond * 20)
			continue
		}

		if pos, err = fp.Seek(0, os.SEEK_CUR); err != nil {
			return
		}

		var status, bufLen int
		status, bufLen, err = readLogEntry(fp, key, lenbuf, buf)

		if status == READ_STATUS_FAILURE {
			progressLn("Could not read from out log: ", err.Error())
			return
		}

		if status == READ_STATUS_SKIP_ENTRY {
			continue
		}

		if status == READ_STATUS_NO_DATA {
			err = nil
			fp.Seek(pos, os.SEEK_SET)
			time.Sleep(time.Millisecond * 100)
			continue
		}

		if _, err = stream.Write(buf[0:bufLen]); err != nil {
			return
		}
	}
}

// read a single entry from log into buf
// if key equals the one in log entry then entry is skipped and READ_STATUS_SKIP_ENTRY is returned

func readLogEntry(fp *os.File, key string, lenbuf, buf []byte) (status, bufLen int, err error) {
	outLogMutex.Lock()
	defer outLogMutex.Unlock()

	var n int
	n, err = io.ReadFull(fp, lenbuf)
	if n == 0 {
		status = READ_STATUS_NO_DATA
		return
	}

	keyLength, err := strconv.Atoi(strings.TrimSpace(string(lenbuf)))
	if err != nil {
		status = READ_STATUS_FAILURE
		return
	}

	if keyLength > 0 {
		keyBuf := make([]byte, keyLength)
		_, err = io.ReadFull(fp, keyBuf)
		if err != nil {
			status = READ_STATUS_FAILURE
			return
		}

		if string(keyBuf) == key {
			status = READ_STATUS_SKIP_ENTRY
		}
	}

	// Read action
	if n, err = io.ReadFull(fp, buf[bufLen:bufLen+10]); err != nil {
		status = READ_STATUS_FAILURE
		return
	}

	bufLen += n

	// Read diff length
	if n, err = io.ReadFull(fp, buf[bufLen:bufLen+10]); err != nil {
		status = READ_STATUS_FAILURE
		return
	}

	diffLen, err := strconv.Atoi(strings.TrimSpace(string(buf[bufLen : bufLen+n])))
	if err != nil {
		status = READ_STATUS_FAILURE
		return
	}

	if diffLen > MAX_DIFF_SIZE {
		panic("Internal consistency error: diff length in output log is greater than max diff size")
	}

	bufLen += n

	if diffLen <= 0 {
		return
	}

	if status == READ_STATUS_SKIP_ENTRY {
		if _, err = fp.Seek(int64(diffLen), os.SEEK_CUR); err != nil {
			status = READ_STATUS_FAILURE
			return
		}
	} else {
		if n, err = io.ReadFull(fp, buf[bufLen:bufLen+diffLen]); err != nil {
			status = READ_STATUS_FAILURE
			return
		}

		bufLen += n
	}

	return
}
