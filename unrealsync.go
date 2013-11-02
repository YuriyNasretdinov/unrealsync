package main

import (
	"bytes"
	"container/list"
	"crypto/md5"
	"flag"
	"fmt"
	ini "github.com/glacjay/goini"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type (
	Settings struct {
		excludes map[string]bool

		username string
		host     string
		port     int

		dir string
		os  string

		bidirectional bool
		compression   bool
	}

	UnrealStat struct {
		isDir  bool
		isLink bool
		mode   int16
		mtime  int64
		size   int64
	}

	BigFile struct {
		fp      *os.File
		tmpName string
	}

	OutMsg struct {
		action       string
		data         interface{}
		sourceStream interface{}
	}

	ChangeReceiver struct {
		changeschan chan OutMsg
		stream      io.WriteCloser
	}
)

const (
	ERROR_FATAL = 1

	GENERAL_SECTION = "general_settings"

	REPO_DIR           = ".unrealsync/"
	REPO_CLIENT_CONFIG = REPO_DIR + "client_config"
	REPO_SERVER_CONFIG = REPO_DIR + "server_config"
	REPO_FILES         = REPO_DIR + "files/"
	REPO_TMP           = REPO_DIR + "tmp/"
	REPO_LOCK          = REPO_DIR + "lock"
	REPO_PID           = REPO_DIR + "pid"

	REPO_SEP = "/\n"
	DIFF_SEP = "\n------------\n"

	// all actions must be 10 symbols length
	ACTION_PING       = "PING      "
	ACTION_PONG       = "PONG      "
	ACTION_DIFF       = "DIFF      "
	ACTION_BIG_INIT   = "BIGINIT   "
	ACTION_BIG_RCV    = "BIGRCV    "
	ACTION_BIG_COMMIT = "BIGCOMMIT "
	ACTION_BIG_ABORT  = "BIGABORT  "

	ACTION_ADD_STREAM = "ADDSTREAM " // special action to add new send stream (after host is ready)
	ACTION_DEL_STREAM = "DELSTREAM " // special action to delete send stream (after losing connection to host)
	ACTION_STOP       = "STOP      " // special action to stop send stream thread

	DB_FILE = "Unreal.db"

	MAX_DIFF_SIZE           = 2 * 1024 * 1204
	DEFAULT_CONNECT_TIMEOUT = 10
	RETRY_INTERVAL          = 10
	SERVER_ALIVE_INTERVAL   = 3
	SERVER_ALIVE_COUNT_MAX  = 4

	PING_INTERVAL = 60e9
)

var (
	sourceDir      string
	unrealsyncDir  string
	localDiff      [MAX_DIFF_SIZE]byte
	localDiffPtr   int
	fschanges      = make(chan string, 1000)
	dirschan       = make(chan string, 1000)
	sendchan       = make(chan OutMsg)
	rcvchan        = make(chan bool)
	remotediffchan = make(chan []byte, 1)
	excludes       = map[string]bool{}
	servers        = map[string]Settings{}
	isServerPtr    = flag.Bool("server", false, "Internal parameter used on remote side")
	isServer       = false
	isDebugPtr     = flag.Bool("debug", false, "Turn on debugging information")
	isDebug        = false
	noWatcherPtr   = flag.Bool("no-watcher", false, "Internal parameter used on remote side to disable local watcher")
	noWatcher      = false
	noRemotePtr    = flag.Bool("no-remote", false, "Internal parameter used on remote side to disable external events")
	noRemote       = false
	hostnamePtr    = flag.String("hostname", "unknown", "Internal parameter used on remote side")
	hostname       = ""
	sendstreamlist = list.New()
)

func (p UnrealStat) Serialize() (res string) {
	res = ""
	if p.isDir {
		res += "dir "
	}
	if p.isLink {
		res += "symlink "
	}

	res += fmt.Sprintf("mode=%o mtime=%d size=%v", p.mode, p.mtime, p.size)
	return
}

func StatsEqual(orig os.FileInfo, repo UnrealStat) bool {
	if repo.isDir != orig.IsDir() {
		debugLn(orig.Name(), " is not dir")
		return false
	}

	if repo.isLink != (orig.Mode()&os.ModeSymlink == os.ModeSymlink) {
		debugLn(orig.Name(), " symlinks different")
		return false
	}

	// TODO: better handle symlinks :)
	// do not check filemode for symlinks because we cannot chmod them either
	if !repo.isLink && (repo.mode&0777) != int16(uint32(orig.Mode())&0777) {
		debugLn(orig.Name(), " modes different")
		return false
	}

	// you cannot set mtime for a symlink and we do not set mtime for directories
	if !repo.isLink && !repo.isDir && repo.mtime != orig.ModTime().Unix() {
		debugLn(orig.Name(), " modification time different")
		return false
	}

	if !repo.isDir && repo.size != orig.Size() {
		debugLn(orig.Name(), " size different")
		return false
	}

	return true
}

func UnrealStatUnserialize(input string) (result UnrealStat) {
	for _, part := range strings.Split(input, " ") {
		if part == "dir" {
			result.isDir = true
		} else if part == "symlink" {
			result.isLink = true
		} else if strings.HasPrefix(part, "mode=") {
			tmp, _ := strconv.ParseInt(part[len("mode="):], 8, 16)
			result.mode = int16(tmp)
		} else if strings.HasPrefix(part, "mtime=") {
			result.mtime, _ = strconv.ParseInt(part[len("mtime="):], 10, 64)
		} else if strings.HasPrefix(part, "size=") {
			result.size, _ = strconv.ParseInt(part[len("size="):], 10, 64)
		}
	}

	return
}

func UnrealStatFromStat(info os.FileInfo) UnrealStat {
	return UnrealStat{
		info.IsDir(),
		(info.Mode()&os.ModeSymlink == os.ModeSymlink),
		int16(uint32(info.Mode()) & 0777),
		info.ModTime().Unix(),
		info.Size(),
	}
}

func _progress(a []interface{}, withEol bool) {
	repeatLen := 15 - len(hostname)
	if repeatLen <= 0 {
		repeatLen = 1
	}
	msg := fmt.Sprint(time.Now().Format("15:04:05"), " ", hostname, "$ ", strings.Repeat(" ", repeatLen))
	msg += fmt.Sprint(a...)
	if withEol {
		msg += fmt.Sprint("\n")
	}
	fmt.Fprint(os.Stderr, msg)
}

func progress(a ...interface{}) {
	_progress(a, false)
}

func progressLn(a ...interface{}) {
	_progress(a, true)
}

func fatalLn(a ...interface{}) {
	progressLn(a...)
	os.Exit(1)
}

func debugLn(a ...interface{}) {
	if isDebug {
		progressLn(a...)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "   unrealsync # no parameters if config is present")
	os.Exit(1)
}

func runWizard() {
	fatalLn("Run Wizard Not implemented yet")
}

func parseExcludes(excl string) map[string]bool {
	result := make(map[string]bool)

	for _, filename := range strings.Split(excl, "|") {
		result[filename] = true
	}

	return result
}

func parseServerSettings(section string, serverSettings map[string]string) Settings {

	var (
		port int = 0
		err  error
	)

	if serverSettings["port"] != "" {
		port, err = strconv.Atoi(serverSettings["port"])
		if err != nil {
			fatalLn("Cannot parse 'port' property in [" + section + "] section of " + REPO_CLIENT_CONFIG + ": " + err.Error())
		}
	}

	localExcludes := make(map[string]bool)

	if serverSettings["exclude"] != "" {
		localExcludes = parseExcludes(serverSettings["exclude"])
	}

	host, ok := serverSettings["host"]
	if !ok {
		host = section
	}

	bidirectional := (serverSettings["bidirectional"] == "true")
	compression := (serverSettings["compression"] != "false")

	return Settings{
		localExcludes,
		serverSettings["username"],
		host,
		port,
		serverSettings["dir"],
		serverSettings["os"],
		bidirectional,
		compression,
	}

}

func parseConfig() {
	dict, err := ini.Load(REPO_CLIENT_CONFIG)

	if err != nil {
		fatalLn(err)
	}

	general, ok := dict[GENERAL_SECTION]
	if !ok {
		fatalLn("Section " + GENERAL_SECTION + " of config file " + REPO_CLIENT_CONFIG + " is empty")
	}

	if general["exclude"] != "" {
		excludes = parseExcludes(general["exclude"])
	}

	excludes[".unrealsync"] = true

	delete(dict, GENERAL_SECTION)

	for key, serverSettings := range dict {
		if key == "" {
			continue
		}

		if _, ok := serverSettings["disabled"]; ok {
			progressLn("Skipping [" + key + "] as disabled")
			continue
		}

		for generalKey, generalValue := range general {
			if serverSettings[generalKey] == "" {
				serverSettings[generalKey] = generalValue
			}
		}
		servers[key] = parseServerSettings(key, serverSettings)
	}
}

func sshOptions(settings Settings) []string {
	options := []string{"-o", fmt.Sprint("ConnectTimeout=", DEFAULT_CONNECT_TIMEOUT)}
	options = append(options, "-o", fmt.Sprint("ServerAliveInterval=", SERVER_ALIVE_INTERVAL))
	options = append(options, "-o", fmt.Sprint("ServerAliveCountMax=", SERVER_ALIVE_COUNT_MAX))

	// Batch mode settings for ssh to prevent it from asking its' stupid questions
	options = append(options, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no")
	options = append(options, "-o", "UserKnownHostsFile=/dev/null")

	if settings.port > 0 {
		options = append(options, "-o", fmt.Sprintf("Port=%d", settings.port))
	}
	if settings.username != "" {
		options = append(options, "-o", "User="+settings.username)
	}
	if settings.compression {
		options = append(options, "-o", "Compression=yes")
	}

	return options
}

func execOrPanic(cmd string, args []string) string {
	debugLn(cmd, args)

	output, err := exec.Command(cmd, args...).Output()
	if err != nil {
		progressLn("Cannot ", cmd, " ", args, ", got error: ", err.Error())
		progressLn("Command output:\n", string(output))
		panic("Command exited with non-zero code")
	}

	return string(output)
}

func startServer(settings Settings) {
	defer func() {
		if err := recover(); err != nil {
			progressLn("Failed to start for server ", settings.host, ": ", err)

			go func() {
				time.Sleep(RETRY_INTERVAL * time.Second)
				progressLn("Reconnecting to " + settings.host)
				startServer(settings)
			}()
		}
	}()

	if settings.bidirectional {
		progressLn("Bidirectional synchronization to " + settings.host + " is enabled")
	}
	progressLn("Creating directories at " + settings.host + "...")

	args := sshOptions(settings)
	// TODO: escaping
	dir := settings.dir + "/.unrealsync"
	args = append(args, settings.host, "if [ ! -d "+dir+" ]; then mkdir -p "+dir+"; fi; rm -f "+dir+"/unrealsync && uname")

	output := execOrPanic("ssh", args)
	uname := strings.TrimSpace(output)

	if uname != "Darwin" && uname != "Linux" {
		fatalLn("Unknown os at " + settings.host + ":'" + uname + "'")
	}

	progressLn("Copying unrealsync binary to " + settings.host + "...")

	args = sshOptions(settings)
	source := unrealsyncDir + "/unrealsync-" + strings.ToLower(uname)
	destination := settings.host + ":" + dir + "/unrealsync"
	args = append(args, source, destination)
	execOrPanic("scp", args)

	progressLn("Initial file sync using rsync at " + settings.host + "...")

	// TODO: escaping
	args = []string{"-e", "ssh " + strings.Join(sshOptions(settings), " ")}
	for mask := range settings.excludes {
		args = append(args, "--exclude="+mask)
	}

	// TODO: escaping of remote dir
	args = append(args, "-a", "--delete", sourceDir+"/", settings.host+":"+settings.dir+"/")
	execOrPanic("rsync", args)

	progressLn("Launching unrealsync at " + settings.host + "...")

	args = sshOptions(settings)
	// TODO: escaping
	flags := "--server --hostname=" + settings.host
	if isDebug {
		flags += " --debug"
	}
	if !settings.bidirectional {
		flags += " --no-watcher"
	}
	args = append(args, settings.host, dir+"/unrealsync "+flags+" "+settings.dir)

	cmd := exec.Command("ssh", args...)

	debugLn("ssh", args)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fatalLn("Cannot get stdout pipe: ", err.Error())
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		fatalLn("Cannot get stdin pipe: ", err.Error())
	}

	cmd.Stderr = os.Stderr

	if err = cmd.Start(); err != nil {
		panic("Cannot start command ssh " + strings.Join(args, " ") + ": " + err.Error())
	}

	sendchan <- OutMsg{ACTION_ADD_STREAM, nil, stdin}
	go applyThread(stdout, stdin, settings)

	if _, err = stdin.Write([]byte(ACTION_PING)); err != nil {
		panic("Cannot ping " + settings.host)
	}

	cmd.Wait()
}

func readResponse(inStream io.ReadCloser) []byte {
	lengthBytes := make([]byte, 10)

	if _, err := io.ReadFull(inStream, lengthBytes); err != nil {
		panic("Cannot read diff length in applyThread from " + hostname + ": " + err.Error())
	}

	length, err := strconv.Atoi(strings.TrimSpace(string(lengthBytes)))
	if err != nil {
		panic("Incorrect diff length in applyThread from " + hostname + ": " + err.Error())
	}

	buf := make([]byte, length)
	if length == 0 {
		return buf
	}

	if length > MAX_DIFF_SIZE {
		panic("Too big diff from " + hostname + ", probably communication error")
	}

	if _, err := io.ReadFull(inStream, buf); err != nil {
		panic("Cannot read diff from " + hostname)
	}

	return buf
}

func tmpBigName(filename string) string {
	h := md5.New()
	io.WriteString(h, filename)
	return REPO_TMP + "big_" + fmt.Sprintf("%x", h.Sum(nil))
}

func processBigInit(buf []byte, bigFps map[string]BigFile) {
	filename := string(buf)
	tmpName := tmpBigName(filename)
	fp, err := os.OpenFile(tmpName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic("Cannot open tmp file " + tmpName + ": " + err.Error())
	}

	bigFps[filename] = BigFile{fp, tmpName}
}

func processBigRcv(buf []byte, bigFps map[string]BigFile) {
	bufOffset := 0

	filenameLen, err := strconv.ParseInt(string(buf[bufOffset:10]), 10, 32)
	if err != nil {
		panic("Cannot parse big filename length")
	}

	bufOffset += 10
	filename := string(buf[bufOffset : bufOffset+int(filenameLen)])
	bufOffset += int(filenameLen)

	bigFile, ok := bigFps[filename]
	if !ok {
		panic("Received big chunk for unknown file: " + filename)
	}

	if _, err = bigFile.fp.Write(buf[bufOffset:]); err != nil {
		panic("Cannot write to tmp file " + bigFile.tmpName + ": " + err.Error())
	}
}

func processBigCommit(buf []byte, bigFps map[string]BigFile) {
	bufOffset := 0

	filenameLen, err := strconv.ParseInt(string(buf[bufOffset:10]), 10, 32)
	if err != nil {
		panic("Cannot parse big filename length")
	}

	bufOffset += 10
	filename := string(buf[bufOffset : bufOffset+int(filenameLen)])
	bufOffset += int(filenameLen)

	bigFile, ok := bigFps[filename]
	if !ok {
		panic("Received big commit for unknown file: " + filename)
	}

	bigstat := UnrealStatUnserialize(string(buf[bufOffset:]))
	if err = bigFile.fp.Close(); err != nil {
		panic("Cannot close tmp file " + bigFile.tmpName + ": " + err.Error())
	}

	if err = os.Chmod(bigFile.tmpName, os.FileMode(bigstat.mode)); err != nil {
		panic("Cannot chmod " + bigFile.tmpName + ": " + err.Error())
	}

	if err = os.Chtimes(bigFile.tmpName, time.Unix(bigstat.mtime, 0), time.Unix(bigstat.mtime, 0)); err != nil {
		panic("Cannot set mtime for " + bigFile.tmpName + ": " + err.Error())
	}

	lockRepo()
	defer unlockRepo()

	os.MkdirAll(filepath.Dir(filename), 0777)
	if err = os.Rename(bigFile.tmpName, filename); err != nil {
		panic("Cannot rename " + bigFile.tmpName + " to " + filename + ": " + err.Error())
	}

	commitSingleFile(filename, &bigstat)
}

func processBigAbort(buf []byte, bigFps map[string]BigFile) {
	filename := string(buf)
	bigFile, ok := bigFps[filename]
	if !ok {
		panic("Received big commit for unknown file: " + filename)
	}

	bigFile.fp.Close()
	os.Remove(bigFile.tmpName)
}

func applyThread(inStream io.ReadCloser, outStream io.WriteCloser, settings Settings) {
	hostname := settings.host
	bigFps := make(map[string]BigFile)

	defer func() {
		for _, bigFile := range bigFps {
			bigFile.fp.Close()
			os.Remove(bigFile.tmpName)
		}

		if err := recover(); err != nil {
			if isServer {
				progressLn("Server error at ", hostname, ": ", err)
				fatalLn("Lost connection to remote side (you should not see this message)")
			}

			progressLn("Error from ", hostname, ": ", err)
			progressLn("Lost connection to ", hostname)
			sendchan <- OutMsg{ACTION_DEL_STREAM, true, outStream}

			go func() {
				time.Sleep(RETRY_INTERVAL * time.Second)
				progressLn("Reconnecting to " + hostname)
				startServer(settings)
			}()
		}
	}()

	action := make([]byte, 10)

	for {
		_, err := io.ReadFull(inStream, action)
		if err != nil {
			panic("Cannot read action in applyThread from " + hostname + ": " + err.Error())
		}

		actionStr := string(action)
		debugLn("Received ", actionStr)
		if isServer {
			rcvchan <- true
		}

		if actionStr == ACTION_PING {
			sendchan <- OutMsg{ACTION_PONG, nil, outStream}
		} else if actionStr == ACTION_PONG {
			debugLn(hostname, " reported that it is alive")
		} else {
			buf := readResponse(inStream)

			if actionStr == ACTION_DIFF {
				applyRemoteDiff(buf)
			} else if actionStr == ACTION_BIG_INIT {
				processBigInit(buf, bigFps)
			} else if actionStr == ACTION_BIG_RCV {
				processBigRcv(buf, bigFps)
			} else if actionStr == ACTION_BIG_COMMIT {
				processBigCommit(buf, bigFps)
			} else if actionStr == ACTION_BIG_ABORT {
				processBigAbort(buf, bigFps)
			}

			if !isServer {
				debugLn("Resending diff")
				sendchan <- OutMsg{actionStr, buf, outStream}
			}
		}
	}
}

func writeContents(file string, unrealStat UnrealStat, contents []byte) {
	stat, err := os.Lstat(file)

	if err == nil {
		// file already exists, we must delete it if it is symlink or dir because of inability to make atomic rename
		if stat.IsDir() != unrealStat.isDir || stat.Mode()&os.ModeSymlink == os.ModeSymlink {
			if err = os.RemoveAll(file); err != nil {
				progressLn("Cannot remove ", file, ": ", err.Error())
				return
			}
		}
	} else if !os.IsNotExist(err) {
		progressLn("Error doing lstat for ", file, ": ", err.Error())
		return
	}

	if unrealStat.isDir {
		if err = os.MkdirAll(file, 0777); err != nil {
			progressLn("Cannot create dir ", file, ": ", err.Error())
			return
		}
	} else if unrealStat.isLink {
		if err = os.Symlink(string(contents), file); err != nil {
			progressLn("Cannot create symlink ", file, ": ", err.Error())
			return
		}
	} else {
		writeFile(file, unrealStat, contents)
	}
}

func writeFile(file string, unrealStat UnrealStat, contents []byte) {
	tempnam := REPO_TMP + path.Base(file)

	fp, err := os.OpenFile(tempnam, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(unrealStat.mode))
	if err != nil {
		progressLn("Cannot open ", tempnam)
		return
	}

	if _, err = fp.Write(contents); err != nil {
		// TODO: more accurate error handling
		progressLn("Cannot write contents to ", tempnam, ": ", err.Error())
		fp.Close()
		return
	}

	if err = fp.Chmod(os.FileMode(unrealStat.mode)); err != nil {
		progressLn("Cannot chmod ", tempnam, ": ", err.Error())
		fp.Close()
		return
	}

	fp.Close()

	dir := path.Dir(file)
	if err = os.MkdirAll(dir, 0777); err != nil {
		progressLn("Cannot create dir ", dir, ": ", err.Error())
		os.Remove(tempnam)
		return
	}

	if err = os.Chtimes(tempnam, time.Unix(unrealStat.mtime, 0), time.Unix(unrealStat.mtime, 0)); err != nil {
		progressLn("Failed to change modification time for ", file, ": ", err.Error())
	}

	if err = os.Rename(tempnam, file); err != nil {
		progressLn("Cannot rename ", tempnam, " to ", file)
		os.Remove(tempnam)
		return
	}

	if isDebug {
		debugLn("Wrote ", file, " ", unrealStat.Serialize())
	}
}

func applyDiff(buf []byte, writeChanges bool) {
	var (
		sepBytes = []byte(DIFF_SEP)
		offset   = 0
		endPos   = 0
	)

	dirs := make(map[string]map[string]*UnrealStat)

	for {
		if offset >= len(buf)-1 {
			break
		}

		if endPos = bytes.Index(buf[offset:], sepBytes); endPos < 0 {
			break
		}

		endPos += offset
		chunk := buf[offset:endPos]
		offset = endPos + len(sepBytes)
		op := chunk[0]

		var (
			diffstat UnrealStat
			file     []byte
			contents []byte
		)

		if op == 'A' {
			firstLinePos := bytes.IndexByte(chunk, '\n')
			if firstLinePos < 0 {
				fatalLn("No new line in file diff: ", string(chunk))
			}

			file = chunk[2:firstLinePos]
			diffstat = UnrealStatUnserialize(string(chunk[firstLinePos+1:]))
		} else if op == 'D' {
			file = chunk[2:]
		} else {
			fatalLn("Unknown operation in diff: ", op)
		}

		// TODO: path check

		if op == 'A' && !diffstat.isDir && diffstat.size > 0 {
			contents = buf[offset : offset+int(diffstat.size)]
			offset += int(diffstat.size)
		}

		fileStr := string(file)
		dir := path.Dir(fileStr)

		if dirs[dir] == nil {
			dirs[dir] = make(map[string]*UnrealStat)
		}

		if op == 'A' {
			if writeChanges {
				writeContents(fileStr, diffstat, contents)
			}
			dirs[dir][path.Base(fileStr)] = &diffstat
		} else if op == 'D' {
			if writeChanges {
				err := os.RemoveAll(string(file))
				if err != nil {
					// TODO: better error handling than just print :)
					progressLn("Cannot remove ", string(file))
				}
			}

			dirs[dir][path.Base(fileStr)] = nil
		} else {
			fatalLn("Unknown operation in diff:", op)
		}
	}

	writeRepoChanges(dirs)
}

func applyRemoteDiff(buf []byte) {
	kilobytes := len(buf) / 1024

	progressLn("Received diff, length ", kilobytes, " KiB")

	lockRepo()
	applyDiff(buf, true)
	unlockRepo()

	progressLn("Applied diff, length ", kilobytes, " KiB")
}

func sendChangesToStreamThread(stream io.WriteCloser, changeschan chan OutMsg) {
	defer func() {
		if err := recover(); err != nil {
			sendchan <- OutMsg{ACTION_STOP, false, stream}
		}
		stream.Close()
	}()

	for {
		msg := <-changeschan
		sourceStream, ok := msg.sourceStream.(io.WriteCloser)
		if ok && sourceStream == stream {
			continue
		}

		if msg.action == ACTION_STOP {
			return
		} else if msg.action == ACTION_PONG || msg.action == ACTION_PING {
			if _, err := stream.Write([]byte(msg.action)); err != nil {
				panic("Cannot write " + msg.action + ": " + err.Error())
			}
		} else {
			buf := msg.data.([]byte)
			if _, err := stream.Write([]byte(msg.action)); err != nil {
				panic("Cannot write " + msg.action + ": " + err.Error())
			}

			if _, err := stream.Write([]byte(fmt.Sprintf("%10d", len(buf)))); err != nil {
				panic("Cannot write length for " + msg.action + ": " + err.Error())
			}

			if _, err := stream.Write(buf); err != nil {
				panic("Cannot write data for " + msg.action + ": " + err.Error())
			}
		}
	}
}

func sendChangesThread() {
	for {
		msg := <-sendchan

		if msg.action == ACTION_ADD_STREAM {
			changeschan := make(chan OutMsg)
			sourceStream := msg.sourceStream.(io.WriteCloser)
			sendstreamlist.PushBack(ChangeReceiver{changeschan, sourceStream})
			go sendChangesToStreamThread(sourceStream, changeschan)
			continue
		} else if msg.action == ACTION_DEL_STREAM {
			sendStop := msg.data.(bool)
			sourceStream := msg.sourceStream.(io.WriteCloser)

			for e := sendstreamlist.Front(); e != nil; e = e.Next() {
				receiver := e.Value.(ChangeReceiver)
				if receiver.stream == sourceStream {
					if sendStop {
						receiver.changeschan <- OutMsg{ACTION_STOP, nil, receiver.stream}
					}
					sendstreamlist.Remove(e)
				}
			}
			continue
		}

		for e := sendstreamlist.Front(); e != nil; e = e.Next() {
			e.Value.(ChangeReceiver).changeschan <- msg
		}
	}
}

func initialize() {
	var err error

	flag.Parse()
	isServer = *isServerPtr
	isDebug = *isDebugPtr
	noRemote = *noRemotePtr
	noWatcher = *noWatcherPtr

	if isServer {
		hostname = *hostnamePtr
	}

	args := flag.Args()

	unrealsyncDir, err = filepath.Abs(path.Dir(os.Args[0]))
	if err != nil {
		fatalLn("Cannot determine unrealsync binary location: " + err.Error())
	}

	if len(args) == 1 {
		if err := os.Chdir(args[0]); err != nil {
			fatalLn("Cannot chdir to ", args[0])
		}
	} else if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "Usage: unrealsync [<flags>] [<dir>]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	sourceDir, err = os.Getwd()
	if err != nil {
		fatalLn("Cannot get current directory")
	}

	if isServer {
		progressLn("Unrealsync server starting at ", sourceDir)
	} else {
		progressLn("Unrealsync starting from ", sourceDir)
	}

	os.RemoveAll(REPO_TMP)

	for _, dir := range []string{REPO_DIR, REPO_FILES, REPO_TMP} {
		_, err = os.Stat(dir)
		if err != nil {
			err = os.Mkdir(dir, 0777)
			if err != nil {
				fatalLn("Cannot create " + dir + ": " + err.Error())
			}
		}
	}

	if _, err := os.Stat(REPO_PID); err == nil {
		pid_file, err := os.Open(REPO_PID)
		if err != nil {
			fatalLn("Cannot open " + REPO_PID + " for reading: " + err.Error())
		}

		var pid int
		_, err = fmt.Fscanf(pid_file, "%d", &pid)
		if err != nil {
			fatalLn("Cannot read pid from " + REPO_PID + ": " + err.Error())
		}

		proc, err := os.FindProcess(pid)
		if err == nil {
			proc.Kill()
		}

		pid_file.Close()
	}

	pid_file, err := os.OpenFile(REPO_PID, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fatalLn("Cannot open " + REPO_PID + " for writing: " + err.Error())
	}

	_, err = fmt.Fprint(pid_file, os.Getpid())
	if err != nil {
		fatalLn("Cannot write current pid to " + REPO_PID + ": " + err.Error())
	}

	pid_file.Close()

	go sendChangesThread()

	if !isServer {
		_, err = os.Stat(REPO_CLIENT_CONFIG)
		if err != nil {
			runWizard()
		}

		parseConfig()
		for _, settings := range servers {
			go startServer(settings)
		}
	} else {
		excludes[".unrealsync"] = true
		sendchan <- OutMsg{ACTION_ADD_STREAM, nil, os.Stdout}
		go applyThread(os.Stdin, os.Stdout, Settings{nil, "local", hostname, 0, sourceDir, "local-os", true, true})
	}

	if noWatcher {
		fschanges <- LOCAL_WATCHER_READY
	} else {
		go runFsChangesThread(sourceDir)
	}

	go pingThread()
}

func pingThread() {
	for {
		sendchan <- OutMsg{ACTION_PING, nil, nil}
		time.Sleep(PING_INTERVAL)
	}
}

func timeoutThread() {
	for {
		select {
		case <-rcvchan:
		case <-time.After(PING_INTERVAL * 2):
			os.Create(REPO_TMP + "deadlock")
			progressLn("Server timeout")
			os.Exit(1)
		}
	}
}

func syncThread() {
	allDirs := make(map[string]bool)
	for {
		for len(dirschan) > 0 {
			allDirs[<-dirschan] = true
		}

		if len(allDirs) > 0 && doSync(allDirs) {
			continue
		}

		time.Sleep(time.Millisecond * 100)
		allDirs = make(map[string]bool)
	}
}

func shouldIgnore(path string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == "" {
			continue
		}

		if excludes[part] {
			// progressLn("Ignored: ", path)
			return true
		}
	}

	return false
}

// Send big file in chunks:

// ACTION_BIG_INIT  = filename
// ACTION_BIG_RCV   = filename length (10 bytes) | filename | chunk contents
// ACTION_BIG_ABORT = filename

func sendBigFile(fileStr string, stat *UnrealStat) (unrealErr int) {
	progressLn("Sending big file: ", fileStr, " (", (stat.size / 1024 / 1024), " MiB)")

	fp, err := os.Open(fileStr)
	if err != nil {
		progressLn("Could not open ", fileStr, ": ", err)
		return
	}
	defer fp.Close()

	commitSingleFile(fileStr, stat)

	unlockRepo()
	defer lockRepo()

	file := []byte(fileStr)

	sendchan <- OutMsg{ACTION_BIG_INIT, file, nil}
	bytesLeft := stat.size

	for {
		buf := make([]byte, MAX_DIFF_SIZE/2)
		bufOffset := 0

		copy(buf[bufOffset:10], fmt.Sprintf("%010d", len(file)))
		bufOffset += 10

		copy(buf[bufOffset:len(file)+bufOffset], file)
		bufOffset += len(file)

		fileStat, err := fp.Stat()
		if err != nil {
			progressLn("Cannot stat ", fileStr, " that we are reading right now: ", err.Error())
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			unrealErr = ERROR_FATAL
			return
		}

		if !StatsEqual(fileStat, *stat) {
			progressLn("File ", fileStr, " has changed, aborting transfer")
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			return
		}

		n, err := fp.Read(buf[bufOffset:])
		if err != nil && err != io.EOF {
			// if we were unable to read file that we just opened then probably there are some problems with the OS
			progressLn("Cannot read ", file, ": ", err)
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			unrealErr = ERROR_FATAL
			return
		}

		if n != len(buf)-bufOffset && int64(n) != bytesLeft {
			progressLn("Read different number of bytes than expected from ", file)
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			return
		}

		sendchan <- OutMsg{ACTION_BIG_RCV, buf[0 : bufOffset+n], nil}

		if bytesLeft -= int64(n); bytesLeft == 0 {
			break
		}
	}

	sendchan <- OutMsg{ACTION_BIG_COMMIT, []byte(fmt.Sprintf("%010d%s%s", len(file), fileStr, stat.Serialize())), nil}

	progressLn("Big file ", fileStr, " successfully sent")

	return
}

func sendDiff() (unrealErr int) {
	if localDiffPtr == 0 {
		return
	}

	unlockRepo()

	buf := make([]byte, localDiffPtr)
	copy(buf, localDiff[0:localDiffPtr])
	sendchan <- OutMsg{ACTION_DIFF, buf, nil}

	lockRepo()
	applyDiff(buf, false)
	localDiffPtr = 0

	return
}

func addToDiff(file string, stat *UnrealStat) (unrealErr int) {
	diffHeaderStr := ""
	var diffLen int64
	var buf []byte

	if stat == nil {
		diffHeaderStr += "D " + file + DIFF_SEP
	} else {
		diffHeaderStr += "A " + file + "\n" + stat.Serialize() + DIFF_SEP
		if stat.isDir == false {
			diffLen = stat.size
		}
	}

	diffHeader := []byte(diffHeaderStr)

	if diffLen > MAX_DIFF_SIZE/2 {
		unrealErr = sendBigFile(file, stat)
		return
	}

	if localDiffPtr+int(diffLen)+len(diffHeader) >= MAX_DIFF_SIZE-1 {
		if unrealErr = sendDiff(); unrealErr > 0 {
			return
		}
	}

	if stat != nil && diffLen > 0 {
		if stat.isLink {
			bufStr, err := os.Readlink(file)
			if err != nil {
				progressLn("Could not read link " + file)
				return
			}

			buf = []byte(bufStr)

			if len(buf) != int(diffLen) {
				progressLn("Readlink different number of bytes than expected from ", file)
				return
			}
		} else {
			fp, err := os.Open(file)
			if err != nil {
				progressLn("Could not open ", file, ": ", err)
				return
			}
			defer fp.Close()

			buf = make([]byte, diffLen)
			n, err := fp.Read(buf)
			if err != nil && err != io.EOF {
				// if we were unable to read file that we just opened then probably there are some problems with the OS
				progressLn("Cannot read ", file, ": ", err)
				unrealErr = ERROR_FATAL
				return
			}

			if n != int(diffLen) {
				progressLn("Read different number of bytes than expected from ", file)
				return
			}
		}
	}

	localDiffPtr += copy(localDiff[localDiffPtr:], diffHeader)

	if stat != nil && diffLen > 0 {
		localDiffPtr += copy(localDiff[localDiffPtr:], buf)
	}

	return
}

func syncDir(dir string, recursive, sendChanges bool) (unrealErr int) {

	if shouldIgnore(dir) {
		return
	}

	fp, err := os.Open(dir)
	if err != nil {
		progressLn("Cannot open ", dir, ": ", err.Error())
		return
	}

	defer fp.Close()

	stat, err := fp.Stat()
	if err != nil {
		progressLn("Cannot stat ", dir, ": ", err.Error())
		return
	}

	if !stat.IsDir() {
		progressLn("Suddenly ", dir, " stopped being a directory")
		return
	}

	repoInfo := getRepoInfo(dir)
	changesCount := 0

	// Detect deletions: we need to do it first because otherwise change from dir to file will be impossible
	for name, _ := range repoInfo {
		_, err := os.Lstat(dir + "/" + name)
		if os.IsNotExist(err) {
			delete(repoInfo, name)

			debugLn("Deleted: ", dir, "/", name)

			if sendChanges {
				if unrealErr = addToDiff(dir+"/"+name, nil); unrealErr > 0 {
					return
				}
			}

			changesCount++
		} else if err != nil {
			progressLn("Could not lstat ", dir, "/", name, ": ", err)
			unrealErr = ERROR_FATAL // we do not want to try to recover from Permission denied and other weird errors
			return
		}
	}

	for {
		res, err := fp.Readdir(10)
		if err != nil {
			if err == io.EOF {
				break
			}

			progressLn("Could not read directory names from " + dir + ": " + err.Error())
			break
		}

		for _, info := range res {
			if shouldIgnore(info.Name()) {
				continue
			}
			repoEl, ok := repoInfo[info.Name()]
			if !ok || !StatsEqual(info, repoEl) {

				if info.IsDir() && (recursive || !ok || !repoEl.isDir) {
					if unrealErr = syncDir(dir+"/"+info.Name(), true, sendChanges); unrealErr > 0 {
						return
					}
				}

				unrealStat := UnrealStatFromStat(info)

				repoInfo[info.Name()] = unrealStat

				prefix := "Changed: "
				if !ok {
					prefix = "Added: "
				}
				debugLn(prefix, dir, "/", info.Name())

				if sendChanges {
					if unrealErr = addToDiff(dir+"/"+info.Name(), &unrealStat); unrealErr > 0 {
						return
					}
				}

				changesCount++
			}
		}
	}

	// initial commit is done when we do not send any changes
	if !sendChanges && changesCount > 0 {
		writeRepoInfo(dir, repoInfo)
	}

	return
}

func doSync(dirs map[string]bool) (shouldRetry bool) {
	dirsList := []string{}
	for dir := range dirs {
		if shouldIgnore(dir) {
			delete(dirs, dir)
			continue
		}
		dirsList = append(dirsList, dir)
	}

	if len(dirs) == 0 {
		return
	}

	progressLn("Changed dirs: ", strings.Join(dirsList, "; "))

	lockRepo()
	defer unlockRepo()

	for dir := range dirs {
		// Upon receiving event we can have 'dir' vanish or become a file
		// We should not even try to process them
		stat, err := os.Lstat(dir)
		if err != nil || !stat.IsDir() {
			delete(dirs, dir)
			continue
		}
		unrealErr := syncDir(dir, false, true)
		if unrealErr == ERROR_FATAL {
			fatalLn("Unrecoverable error, exiting (this should never happen! please file a bug report)")
		}
	}

	if unrealErr := sendDiff(); unrealErr > 0 {
		return
	}

	return
}

func main() {
	initialize()
	watcherReady := false
	var err error

	for {
		path := <-fschanges
		if !watcherReady {
			if path == LOCAL_WATCHER_READY {
				watcherReady = true
				if syncDir(".", true, false) > 0 {
					fatalLn("Cannot commit changes at .")
				}
				progressLn("Initial commit done")
				go syncThread()
				if isServer {
					go timeoutThread()
				}

				progressLn("Watcher ready")
			}
			continue
		}

		if filepath.IsAbs(path) {
			path, err = filepath.Rel(sourceDir, path)
			if err != nil {
				progressLn("Cannot compute relative path: ", err)
				continue
			}
		}

		stat, err := os.Lstat(path)

		if err != nil {
			if !os.IsNotExist(err) {
				progressLn("Stat failed for ", path, ": ", err.Error())
				continue
			}

			path = filepath.Dir(path)
		} else if !stat.IsDir() {
			path = filepath.Dir(path)
		}

		dirschan <- path
	}
}
