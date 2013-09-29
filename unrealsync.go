package main

import (
	"bytes"
	"container/list"
	"crypto/md5"
	"flag"
	"fmt"
	ini "github.com/glacjay/goini"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Settings struct {
	excludes map[string]bool

	username string
	host     string
	port     int

	dir string
	os  string
}

type UnrealStat struct {
	is_dir  bool
	is_link bool
	mode    int16
	mtime   int64
	size    int64
}

type BigFile struct {
	fp       *os.File
	tmp_name string
}

type OutMsg struct {
	action        string
	data          interface{}
	source_stream interface{}
}

type ChangeReceiver struct {
	changeschan chan OutMsg
	stream      io.WriteCloser
}

const (
	ERROR_FATAL = 1

	GENERAL_SECTION = "general_settings"

	REPO_DIR           = ".unrealsync/"
	REPO_CLIENT_CONFIG = REPO_DIR + "client_config"
	REPO_SERVER_CONFIG = REPO_DIR + "server_config"
	REPO_FILES         = REPO_DIR + "files/"
	REPO_TMP           = REPO_DIR + "tmp/"
	REPO_LOCK          = REPO_DIR + "lock"

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

	PING_INTERVAL = 5e9
)

var (
	source_dir     string
	unrealsync_dir string
	repo_mutex     sync.Mutex
	local_diff     [MAX_DIFF_SIZE]byte
	local_diff_ptr int
	fschanges      = make(chan string, 1000)
	dirschan       = make(chan string, 1000)
	sendchan       = make(chan OutMsg)
	rcvchan        = make(chan bool)
	remotediffchan = make(chan []byte, 100)
	excludes       = map[string]bool{}
	servers        = map[string]Settings{}
	is_server_ptr  = flag.Bool("server", false, "Internal parameter used on remote side")
	is_server      = false
	is_debug_ptr   = flag.Bool("debug", false, "Turn on debugging information")
	is_debug       = false
	hostname_ptr   = flag.String("hostname", "unknown", "Internal parameter used on remote side")
	hostname       = ""
	sendstreamlist = list.New()
)

func (p UnrealStat) Serialize() (res string) {
	res = ""
	if p.is_dir {
		res += "dir "
	}
	if p.is_link {
		res += "symlink "
	}

	res += fmt.Sprintf("mode=%o mtime=%d size=%v", p.mode, p.mtime, p.size)
	return
}

func StatsEqual(orig os.FileInfo, repo UnrealStat) bool {
	if repo.is_dir != orig.IsDir() {
		debugLn(orig.Name(), " is not dir")
		return false
	}

	if (repo.mode & 0777) != int16(uint32(orig.Mode())&0777) {
		debugLn(orig.Name(), " modes different")
		return false
	}

	if repo.is_link != (orig.Mode()&os.ModeSymlink == os.ModeSymlink) {
		debugLn(orig.Name(), " symlinks different")
		return false
	}

	// you cannot set mtime for a symlink and we do not set mtime for directories
	if !repo.is_link && !repo.is_dir && repo.mtime != orig.ModTime().Unix() {
		debugLn(orig.Name(), " modification time different")
		return false
	}

	if !repo.is_dir && repo.size != orig.Size() {
		debugLn(orig.Name(), " size different")
		return false
	}

	return true
}

func UnrealStatUnserialize(input string) (result UnrealStat) {
	for _, part := range strings.Split(input, " ") {
		if part == "dir" {
			result.is_dir = true
		} else if part == "symlink" {
			result.is_link = true
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

func _progress(a []interface{}, with_eol bool) {
	repeat_len := 10 - len(hostname)
	if repeat_len <= 0 {
		repeat_len = 1
	}
	msg := fmt.Sprint(time.Now().Format("15:04:05"), " ", hostname, "$ ", strings.Repeat(" ", repeat_len))
	msg += fmt.Sprint(a...)
	if with_eol {
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
	if is_debug {
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

func parseServerSettings(section string, server_settings map[string]string) Settings {

	var (
		port int = 0
		err  error
	)

	if server_settings["port"] != "" {
		port, err = strconv.Atoi(server_settings["port"])
		if err != nil {
			fatalLn("Cannot parse 'port' property in [" + section + "] section of " + REPO_CLIENT_CONFIG + ": " + err.Error())
		}
	}

	local_excludes := make(map[string]bool)

	if server_settings["exclude"] != "" {
		local_excludes = parseExcludes(server_settings["exclude"])
	}

	host, ok := server_settings["host"]
	if !ok {
		host = section
	}

	return Settings{
		local_excludes,
		server_settings["username"],
		host,
		port,
		server_settings["dir"],
		server_settings["os"],
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

	for key, server_settings := range dict {
		if key == "" {
			continue
		}

		if _, ok := server_settings["disabled"]; ok {
			progressLn("Skipping [" + key + "] as disabled")
			continue
		}

		for general_key, general_value := range general {
			if server_settings[general_key] == "" {
				server_settings[general_key] = general_value
			}
		}
		servers[key] = parseServerSettings(key, server_settings)
	}
}

func sshOptions(settings Settings) []string {
	options := []string{"-o", "Compression=yes", "-o", fmt.Sprint("ConnectTimeout=", DEFAULT_CONNECT_TIMEOUT)}
	options = append(options, "-o", fmt.Sprint("ServerAliveInterval=", SERVER_ALIVE_INTERVAL))
	options = append(options, "-o", fmt.Sprint("ServerAliveCountMax=", SERVER_ALIVE_COUNT_MAX))

	if settings.port > 0 {
		options = append(options, "-o", fmt.Sprintf("Port=%d", settings.port))
	}
	if settings.username != "" {
		options = append(options, "-o", "User="+settings.username)
	}

	return options
}

func execOrPanic(cmd string, args []string) string {
	output, err := exec.Command(cmd, args...).CombinedOutput()
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
	source := unrealsync_dir + "/unrealsync-" + strings.ToLower(uname)
	destination := settings.host + ":" + dir + "/unrealsync"
	args = append(args, source, destination)
	execOrPanic("scp", args)

	progressLn("Initial file sync using rsync at " + settings.host + "...")

	// TODO: escaping
	args = []string{"-e", "ssh " + strings.Join(sshOptions(settings), " ")}
	for mask := range excludes {
		args = append(args, "--exclude="+mask)
	}

	// TODO: escaping of remote dir
	args = append(args, "-a", "--delete", source_dir+"/", settings.host+":"+settings.dir+"/")
	execOrPanic("rsync", args)

	progressLn("Launching unrealsync at " + settings.host + "...")

	args = sshOptions(settings)
	// TODO: escaping
	flags := "--server --hostname=" + settings.host
	if is_debug {
		flags += " --debug"
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

func readResponse(in_stream io.ReadCloser) []byte {
	length_bytes := make([]byte, 10)

	if _, err := io.ReadFull(in_stream, length_bytes); err != nil {
		panic("Cannot read diff length in applyThread from " + hostname + ": " + err.Error())
	}

	length, err := strconv.Atoi(strings.TrimSpace(string(length_bytes)))
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

	if _, err := io.ReadFull(in_stream, buf); err != nil {
		panic("Cannot read diff from " + hostname)
	}

	return buf
}

func tmpBigName(filename string) string {
	h := md5.New()
	return REPO_TMP + "big_" + fmt.Sprintf("%x", h.Sum([]byte(filename)))
}

func processBigInit(buf []byte, big_fps map[string]BigFile) {
	filename := string(buf)
	tmp_name := tmpBigName(filename)
	fp, err := os.OpenFile(tmp_name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic("Cannot open tmp file " + tmp_name + ": " + err.Error())
	}

	big_fps[filename] = BigFile{fp, tmp_name}
}

func processBigRcv(buf []byte, big_fps map[string]BigFile) {
	buf_offset := 0

	filename_len, err := strconv.ParseInt(string(buf[buf_offset:10]), 10, 32)
	if err != nil {
		panic("Cannot parse big filename length")
	}

	buf_offset += 10
	filename := string(buf[buf_offset : buf_offset+int(filename_len)])
	buf_offset += int(filename_len)

	big_file, ok := big_fps[filename]
	if !ok {
		panic("Received big chunk for unknown file: " + filename)
	}

	if _, err = big_file.fp.Write(buf[buf_offset:]); err != nil {
		panic("Cannot write to tmp file " + big_file.tmp_name + ": " + err.Error())
	}
}

func processBigCommit(buf []byte, big_fps map[string]BigFile) {
	buf_offset := 0

	filename_len, err := strconv.ParseInt(string(buf[buf_offset:10]), 10, 32)
	if err != nil {
		panic("Cannot parse big filename length")
	}

	buf_offset += 10
	filename := string(buf[buf_offset : buf_offset+int(filename_len)])
	buf_offset += int(filename_len)

	big_file, ok := big_fps[filename]
	if !ok {
		panic("Received big commit for unknown file: " + filename)
	}

	bigstat := UnrealStatUnserialize(string(buf[buf_offset:]))
	if err = big_file.fp.Close(); err != nil {
		panic("Cannot close tmp file " + big_file.tmp_name + ": " + err.Error())
	}

	if err = os.Chmod(big_file.tmp_name, os.FileMode(bigstat.mode)); err != nil {
		panic("Cannot chmod " + big_file.tmp_name + ": " + err.Error())
	}

	if err = os.Chtimes(big_file.tmp_name, time.Unix(bigstat.mtime, 0), time.Unix(bigstat.mtime, 0)); err != nil {
		panic("Cannot set mtime for " + big_file.tmp_name + ": " + err.Error())
	}

	repo_mutex.Lock()
	defer repo_mutex.Unlock()

	info_map := getRepoInfo(filepath.Dir(filename))
	if err = os.Rename(big_file.tmp_name, filename); err != nil {
		panic("Cannot rename " + big_file.tmp_name + " to " + filename + ": " + err.Error())
	}

	info_map[filepath.Base(filename)] = bigstat
	writeRepoInfo(filepath.Dir(filename), info_map)
}

func processBigAbort(buf []byte, big_fps map[string]BigFile) {
	filename := string(buf)
	big_file, ok := big_fps[filename]
	if !ok {
		panic("Received big commit for unknown file: " + filename)
	}

	big_file.fp.Close()
	os.Remove(big_file.tmp_name)
}

func applyThread(in_stream io.ReadCloser, out_stream io.WriteCloser, settings Settings) {
	hostname := settings.host
	big_fps := make(map[string]BigFile)

	defer func() {
		for _, big_file := range big_fps {
			big_file.fp.Close()
			os.Remove(big_file.tmp_name)
		}

		if err := recover(); err != nil {
			if is_server {
				progressLn("Server error at ", hostname, ": ", err)
				fatalLn("Lost connection to remote side (you should not see this message)")
			}

			progressLn("Error from ", hostname, ": ", err)
			progressLn("Lost connection to ", hostname)
			sendchan <- OutMsg{ACTION_DEL_STREAM, true, out_stream}

			go func() {
				time.Sleep(RETRY_INTERVAL * time.Second)
				progressLn("Reconnecting to " + hostname)
				startServer(settings)
			}()
		}
	}()

	action := make([]byte, 10)

	for {
		_, err := io.ReadFull(in_stream, action)
		if err != nil {
			panic("Cannot read action in applyThread from " + hostname + ": " + err.Error())
		}

		action_str := string(action)
		if is_server {
			debugLn("Received ", action_str)
			rcvchan <- true
		}

		if action_str == ACTION_PING {
			sendchan <- OutMsg{ACTION_PONG, nil, out_stream}
		} else if action_str == ACTION_PONG {
			debugLn(hostname, " reported that it is alive")
		} else {
			buf := readResponse(in_stream)

			if action_str == ACTION_DIFF {
				applyRemoteDiff(buf)
			} else if action_str == ACTION_BIG_INIT {
				processBigInit(buf, big_fps)
			} else if action_str == ACTION_BIG_RCV {
				processBigRcv(buf, big_fps)
			} else if action_str == ACTION_BIG_COMMIT {
				processBigCommit(buf, big_fps)
			} else if action_str == ACTION_BIG_ABORT {
				processBigAbort(buf, big_fps)
			}

			sendchan <- OutMsg{action_str, buf, out_stream}
		}
	}
}

func writeContents(file string, unreal_stat UnrealStat, contents []byte) {
	stat, err := os.Lstat(file)

	if err == nil {
		// file already exists, we must delete it if it is symlink or dir because of inability to make atomic rename
		if stat.IsDir() != unreal_stat.is_dir || stat.Mode()&os.ModeSymlink == os.ModeSymlink {
			if err = os.RemoveAll(file); err != nil {
				progressLn("Cannot remove ", file, ": ", err.Error())
				return
			}
		}
	} else if !os.IsNotExist(err) {
		progressLn("Error doing lstat for ", file, ": ", err.Error())
		return
	}

	if unreal_stat.is_dir {
		if err = os.MkdirAll(file, 0777); err != nil {
			progressLn("Cannot create dir ", file, ": ", err.Error())
			return
		}
	} else if unreal_stat.is_link {
		if err = os.Symlink(string(contents), file); err != nil {
			progressLn("Cannot create symlink ", file, ": ", err.Error())
			return
		}
	} else {
		writeFile(file, unreal_stat, contents)
	}
}

func writeFile(file string, unreal_stat UnrealStat, contents []byte) {
	tempnam := REPO_TMP + path.Base(file)

	fp, err := os.OpenFile(tempnam, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(unreal_stat.mode))
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

	if err = fp.Chmod(os.FileMode(unreal_stat.mode)); err != nil {
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

	if err = os.Chtimes(tempnam, time.Unix(unreal_stat.mtime, 0), time.Unix(unreal_stat.mtime, 0)); err != nil {
		progressLn("Failed to change modification time for ", file, ": ", err.Error())
	}

	if err = os.Rename(tempnam, file); err != nil {
		progressLn("Cannot rename ", tempnam, " to ", file)
		os.Remove(tempnam)
		return
	}

	debugLn("Wrote ", file, " ", unreal_stat.Serialize())
}

func applyRemoteDiff(buf []byte) {
	repo_mutex.Lock()
	defer repo_mutex.Unlock()

	progressLn("Received diff, length ", len(buf))
	if len(buf) < 500 {
		debugLn("Diff: ", string(buf))
	}

	var (
		sep_bytes = []byte(DIFF_SEP)
		offset    = 0
		end_pos   = 0
	)

	dirs := make(map[string]map[string]*UnrealStat)

	for {
		if offset >= len(buf)-1 {
			break
		}

		if end_pos = bytes.Index(buf[offset:], sep_bytes); end_pos < 0 {
			break
		}

		end_pos += offset
		chunk := buf[offset:end_pos]
		offset = end_pos + len(sep_bytes)
		op := chunk[0]

		var (
			diffstat UnrealStat
			file     []byte
			contents []byte
		)

		if op == 'A' {
			first_line_pos := bytes.IndexByte(chunk, '\n')
			if first_line_pos < 0 {
				fatalLn("No new line in file diff: ", string(chunk))
			}

			file = chunk[2:first_line_pos]
			diffstat = UnrealStatUnserialize(string(chunk[first_line_pos+1:]))
		} else if op == 'D' {
			file = chunk[2:]
		} else {
			fatalLn("Unknown operation in diff: ", op)
		}

		// TODO: path check

		if op == 'A' && !diffstat.is_dir && diffstat.size > 0 {
			contents = buf[offset : offset+int(diffstat.size)]
			offset += int(diffstat.size)
		}

		file_str := string(file)
		dir := path.Dir(file_str)

		if dirs[dir] == nil {
			dirs[dir] = make(map[string]*UnrealStat)
		}

		if op == 'A' {
			writeContents(file_str, diffstat, contents)
			dirs[dir][path.Base(file_str)] = &diffstat
		} else if op == 'D' {
			err := os.RemoveAll(string(file))
			if err != nil {
				// TODO: better error handling than just print :)
				progressLn("Cannot remove ", string(file))
			}

			dirs[dir][path.Base(file_str)] = nil
		} else {
			fatalLn("Unknown operation in diff:", op)
		}
	}

	for dir, filemap := range dirs {
		info_map := getRepoInfo(dir)

		for file, stat := range filemap {
			dir_path := REPO_FILES + dir + "/" + file

			if stat == nil {
				if _, ok := info_map[file]; ok {
					delete(info_map, file)
				} else if err := os.RemoveAll(dir_path); err != nil && !os.IsNotExist(err) {
					progressLn("Cannot remove ", dir_path, ": ", err.Error())
				}
			} else {
				info_map[file] = *stat
			}
		}

		writeRepoInfo(dir, info_map)
	}
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
		source_stream, ok := msg.source_stream.(io.WriteCloser)
		if ok && source_stream == stream {
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
			source_stream := msg.source_stream.(io.WriteCloser)
			sendstreamlist.PushBack(ChangeReceiver{changeschan, source_stream})
			go sendChangesToStreamThread(source_stream, changeschan)
			continue
		} else if msg.action == ACTION_DEL_STREAM {
			send_stop := msg.data.(bool)
			source_stream := msg.source_stream.(io.WriteCloser)

			for e := sendstreamlist.Front(); e != nil; e = e.Next() {
				receiver := e.Value.(ChangeReceiver)
				if receiver.stream == source_stream {
					if send_stop {
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
	is_server = *is_server_ptr
	is_debug = *is_debug_ptr

	if is_server {
		hostname = *hostname_ptr
	}

	args := flag.Args()

	unrealsync_dir, err = filepath.Abs(path.Dir(os.Args[0]))
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

	source_dir, err = os.Getwd()
	if err != nil {
		fatalLn("Cannot get current directory")
	}

	if is_server {
		progressLn("Unrealsync server starting at ", source_dir)
	} else {
		progressLn("Unrealsync starting from ", source_dir)
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

	go sendChangesThread()

	if !is_server {
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
		go applyThread(os.Stdin, os.Stdout, Settings{nil, "local", hostname, 0, source_dir, "local-os"})
	}

	go runFsChangesThread(source_dir)
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
			panic("Double ping interval exceeded: probably a deadlock")
		}
	}
}

func syncThread() {
	all_dirs := make(map[string]bool)
	for {
		for len(dirschan) > 0 {
			all_dirs[<-dirschan] = true
		}

		if len(all_dirs) > 0 && do_sync(all_dirs) {
			continue
		}

		time.Sleep(time.Millisecond * 100)
		all_dirs = make(map[string]bool)
	}
}

func writeRepoInfo(dir string, info_map map[string]UnrealStat) {

	debugLn("Commiting changes at ", dir)

	old_info_map := getRepoInfo(dir)

	repo_dir := REPO_FILES + dir
	err := os.MkdirAll(repo_dir, 0777)
	if err != nil {
		progressLn("Cannot mkdir(", repo_dir, "): ", err)
		return
	}

	filename := repo_dir + "/" + DB_FILE
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		progressLn("Cannot open ", filename, " for writing: ", err)
		return
	}
	defer fp.Close()

	result := make([]string, len(info_map)*2)

	i := 0
	for k, v := range info_map {
		result[i] = k
		result[i+1] = v.Serialize()
		i += 2
	}

	// Delete deleted directories ;)
	for k, v := range old_info_map {
		_, ok := info_map[k]
		if !ok && v.is_dir {
			err := os.RemoveAll(repo_dir + "/" + k)
			if err != nil {
				progressLn("Cannot delete ", repo_dir, "/", k, ": ", err)
				return
			}
		}
	}

	debugLn("Writing to ", filename, ": ", result)

	_, err = fp.WriteString(strings.Join(result, REPO_SEP))
	if err != nil {
		progressLn("Cannot write to ", filename, ": ", err)
	}
}

func getRepoInfo(dir string) (result map[string]UnrealStat) {
	result = make(map[string]UnrealStat)
	filename := REPO_FILES + dir + "/" + DB_FILE
	fp, err := os.Open(filename)
	if err != nil {
		// progressLn("Cannot open ", filename, ": ", err)
		return
	}
	defer fp.Close()

	contents, err := ioutil.ReadAll(fp)
	if err != nil || len(contents) < 2 {
		// progressLn("Cannot read ", filename, ": ", err)
		return
	}

	elements := strings.Split(string(contents), REPO_SEP)

	if len(elements)%2 != 0 {
		fatalLn("Broken repository file (inconsistent data): ", filename)
	}

	for i := 0; i < len(elements); i += 2 {
		result[elements[i]] = UnrealStatUnserialize(elements[i+1])
	}

	return
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

func sendBigFile(file_str string, stat *UnrealStat) (unreal_err int) {
	progressLn("Sending big file: ", file_str, " (", (stat.size / 1024 / 1024), " MiB)")

	fp, err := os.Open(file_str)
	if err != nil {
		progressLn("Could not open ", file_str, ": ", err)
		return
	}
	defer fp.Close()

	file := []byte(file_str)

	sendchan <- OutMsg{ACTION_BIG_INIT, file, nil}
	bytes_left := stat.size

	for {
		buf := make([]byte, MAX_DIFF_SIZE/2)
		buf_offset := 0

		copy(buf[buf_offset:10], fmt.Sprintf("%010d", len(file)))
		buf_offset += 10

		copy(buf[buf_offset:len(file)+buf_offset], file)
		buf_offset += len(file)

		file_stat, err := fp.Stat()
		if err != nil {
			progressLn("Cannot stat ", file_str, " that we are reading right now: ", err.Error())
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			unreal_err = ERROR_FATAL
			return
		}

		if !StatsEqual(file_stat, *stat) {
			progressLn("File ", file_str, " has changed, aborting transfer")
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			return
		}

		n, err := fp.Read(buf[buf_offset:])
		if err != nil && err != io.EOF {
			// if we were unable to read file that we just opened then probably there are some problems with the OS
			progressLn("Cannot read ", file, ": ", err)
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			unreal_err = ERROR_FATAL
			return
		}

		if n != len(buf)-buf_offset && int64(n) != bytes_left {
			progressLn("Read different number of bytes than expected from ", file)
			sendchan <- OutMsg{ACTION_BIG_ABORT, []byte(file), nil}
			return
		}

		sendchan <- OutMsg{ACTION_BIG_RCV, buf[0 : buf_offset+n], nil}

		if bytes_left -= int64(n); bytes_left == 0 {
			break
		}
	}

	sendchan <- OutMsg{ACTION_BIG_COMMIT, []byte(fmt.Sprintf("%010d%s%s", len(file), file_str, stat.Serialize())), nil}

	progressLn("Big file ", file_str, " successfully sent")

	return
}

func sendDiff() (unreal_err int) {
	if local_diff_ptr == 0 {
		return
	}

	buf := make([]byte, local_diff_ptr)
	copy(buf, local_diff[0:local_diff_ptr])
	sendchan <- OutMsg{ACTION_DIFF, buf, nil}
	local_diff_ptr = 0
	return
}

func addToDiff(file string, stat *UnrealStat) (unreal_err int) {
	diff_header_str := ""
	var diff_len int64
	var buf []byte

	if stat == nil {
		diff_header_str += "D " + file + DIFF_SEP
	} else {
		diff_header_str += "A " + file + "\n" + stat.Serialize() + DIFF_SEP
		if stat.is_dir == false {
			diff_len = stat.size
		}
	}

	diff_header := []byte(diff_header_str)

	if diff_len > MAX_DIFF_SIZE/2 {
		unreal_err = sendBigFile(file, stat)
		return
	}

	if local_diff_ptr+int(diff_len)+len(diff_header) >= MAX_DIFF_SIZE-1 {
		if unreal_err = sendDiff(); unreal_err > 0 {
			return
		}
	}

	if stat != nil && diff_len > 0 {
		if stat.is_link {
			buf_str, err := os.Readlink(file)
			if err != nil {
				progressLn("Could not read link " + file)
				return
			}

			buf = []byte(buf_str)

			if len(buf) != int(diff_len) {
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

			buf = make([]byte, diff_len)
			n, err := fp.Read(buf)
			if err != nil && err != io.EOF {
				// if we were unable to read file that we just opened then probably there are some problems with the OS
				progressLn("Cannot read ", file, ": ", err)
				unreal_err = ERROR_FATAL
				return
			}

			if n != int(diff_len) {
				progressLn("Read different number of bytes than expected from ", file)
				return
			}
		}
	}

	local_diff_ptr += copy(local_diff[local_diff_ptr:], diff_header)

	if stat != nil && diff_len > 0 {
		local_diff_ptr += copy(local_diff[local_diff_ptr:], buf)
	}

	return
}

func syncDir(dir string, recursive, send_changes bool) (unreal_err int) {

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

	repo_info := getRepoInfo(dir)
	changes_count := 0

	// Detect deletions: we need to do it first because otherwise change from dir to file will be impossible
	for name, _ := range repo_info {
		_, err := os.Lstat(dir + "/" + name)
		if os.IsNotExist(err) {
			delete(repo_info, name)

			debugLn("Deleted: ", dir, "/", name)

			if send_changes {
				if unreal_err = addToDiff(dir+"/"+name, nil); unreal_err > 0 {
					return
				}
			}

			changes_count++
		} else if err != nil {
			progressLn("Could not lstat ", dir, "/", name, ": ", err)
			unreal_err = ERROR_FATAL // we do not want to try to recover from Permission denied and other weird errors
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
			repo_el, ok := repo_info[info.Name()]
			if !ok || !StatsEqual(info, repo_el) {

				if info.IsDir() && (recursive || !ok || !repo_el.is_dir) {
					if unreal_err = syncDir(dir+"/"+info.Name(), true, send_changes); unreal_err > 0 {
						return
					}
				}

				unreal_stat := UnrealStatFromStat(info)

				repo_info[info.Name()] = unreal_stat

				prefix := "Changed: "
				if !ok {
					prefix = "Added: "
				}
				debugLn(prefix, dir, "/", info.Name())

				if send_changes {
					if unreal_err = addToDiff(dir+"/"+info.Name(), &unreal_stat); unreal_err > 0 {
						return
					}
				}

				changes_count++
			}
		}
	}

	if changes_count > 0 {
		writeRepoInfo(dir, repo_info)
	}

	return
}

func do_sync(dirs map[string]bool) (should_retry bool) {
	dirs_list := []string{}
	for dir := range dirs {
		if shouldIgnore(dir) {
			delete(dirs, dir)
			continue
		}
		dirs_list = append(dirs_list, dir)
	}

	if len(dirs) == 0 {
		return
	}

	progressLn("Changed dirs: ", strings.Join(dirs_list, "; "))

	repo_mutex.Lock()
	defer repo_mutex.Unlock()

	for dir := range dirs {
		// Upon receiving event we can have 'dir' vanish or become a file
		// We should not even try to process them
		stat, err := os.Lstat(dir)
		if err != nil || !stat.IsDir() {
			delete(dirs, dir)
			continue
		}
		unreal_err := syncDir(dir, false, true)
		if unreal_err == ERROR_FATAL {
			fatalLn("Unrecoverable error, exiting (this should never happen! please file a bug report)")
		}
	}

	if unreal_err := sendDiff(); unreal_err > 0 {
		return
	}

	return
}

func main() {
	initialize()
	watcher_ready := false
	var err error

	for {
		path := <-fschanges
		if !watcher_ready {
			if path == LOCAL_WATCHER_READY {
				watcher_ready = true
				if syncDir(".", true, false) > 0 {
					fatalLn("Cannot commit changes at .")
				}
				go syncThread()
				if is_server {
					go timeoutThread()
				}

				progressLn("Watcher ready")
			}
			continue
		}

		if filepath.IsAbs(path) {
			path, err = filepath.Rel(source_dir, path)
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
