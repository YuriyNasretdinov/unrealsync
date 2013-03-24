package main

import (
	"fmt"
	"flag"
	"os"
	"log"
	ini "github.com/glacjay/goini"
	"strings"
	"strconv"
	"path"
	"path/filepath"
	"time"
	"io/ioutil"
	"io"
)

type Settings struct {
	excludes map[string]bool

	username string
	host     string
	port     int

	dir string
	os string
}

type Diff struct {
	dir  string
	info os.FileInfo
}

type UnrealStat struct {
	is_dir  bool
	is_link bool
	mode    int16
	mtime   int64
	size    int64
}

const (
	ERROR_RECOVERABLE = 1
	ERROR_FATAL = 2

	GENERAL_SECTION = "general_settings"

	REPO_DIR = ".unrealsync/"
	REPO_CLIENT_CONFIG = REPO_DIR + "client_config"
	REPO_SERVER_CONFIG = REPO_DIR + "server_config"
	REPO_FILES = REPO_DIR + "files/"
	REPO_TMP = REPO_DIR + "tmp/"
	REPO_LOCK = REPO_DIR + "lock"

	DB_FILE = "Unreal.db"
)

var (
	source_dir string
	unrealsync_dir string
	fschanges = make(chan string, 1000)
	dirschan = make(chan string, 100)
	excludes = map[string]bool{}
	servers = map[string]Settings{}
)

func (p UnrealStat) Serialize() (res string) {
	res = ""
	if p.is_dir {
		res += "dir "
	}
	if p.is_link {
		res += "symlink "
	}

	res += fmt.Sprint("mode=", p.mode, " mtime=", p.mtime, " size=", p.size)
	return
}

func StatsEqual(orig os.FileInfo, repo UnrealStat) bool {
	if repo.is_dir != orig.IsDir() {
		progressLn(orig.Name(), " is not dir")
		return false
	}

	if (repo.mode & 0777) != int16(uint32(orig.Mode()) & 0777) {
		progressLn(orig.Name(), " modes different")
		return false
	}

	if repo.is_link != (orig.Mode() & os.ModeSymlink == os.ModeSymlink) {
		progressLn(orig.Name(), " symlinks different")
		return false
	}

	if repo.mtime != orig.ModTime().Unix() {
		progressLn(orig.Name(), " modification time different")
		return false
	}

	if repo.size != orig.Size() {
		progressLn(orig.Name(), " size different")
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
			tmp, _ := strconv.ParseInt(part[len("mode="):], 10, 16)
			result.mode = int16(tmp)
		} else if strings.HasPrefix(part, "mtime=") {
			result.mtime, _ = strconv.ParseInt(part[len("mtime="):], 10, 64)
		} else if strings.HasPrefix(part, "size=") {
			result.size, _ = strconv.ParseInt(part[len("size="):], 10, 64)
		}
	}

	return
}

func progress(a ...interface{}) {
	fmt.Fprint(os.Stderr, time.Now().Format("15:04:05"), " ")
	fmt.Fprint(os.Stderr, a...)
}

func progressLn(a ...interface{}) {
	progress(a...)
	fmt.Fprint(os.Stderr, "\n")
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "   unrealsync # no parameters if config is present")
	os.Exit(1)
}

func runWizard() {
	log.Fatal("Not implemented yet")
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
		err error
	)

	if server_settings["port"] != "" {
		port, err = strconv.Atoi(server_settings["port"])
		if err != nil {
			log.Fatal("Cannot parse 'port' property in [" + section + "] section of " + REPO_CLIENT_CONFIG + ": " + err.Error())
		}
	}

	local_excludes := make(map[string]bool)

	if server_settings["exclude"] != "" {
		local_excludes = parseExcludes(server_settings["exclude"])
	}

	return Settings{
		local_excludes,
		server_settings["username"],
		server_settings["host"],
		port,
		server_settings["dir"],
		server_settings["os"],
	}

}

func parseConfig() {
	dict, err := ini.Load(REPO_CLIENT_CONFIG)

	if err != nil {
		log.Fatal(err)
	}

	general, ok := dict[GENERAL_SECTION]
	if !ok {
		log.Fatal("Section " + GENERAL_SECTION + " of config file " + REPO_CLIENT_CONFIG + " is empty")
	}

	if general["exclude"] != "" {
		excludes = parseExcludes(general["exclude"])
	}
	excludes["."] = true
	excludes[".."] = true

	delete(dict, GENERAL_SECTION)

	for key, server_settings := range dict {
		if key == "" {
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

func initialize() {
	var err error

	source_dir, err = os.Getwd()
	if err != nil {
		log.Fatal("Cannot get current directory")
	}
	progressLn("Unrealsync starting from " + source_dir)

	unrealsync_dir, err = filepath.Abs(path.Dir(os.Args[0]))
	if err != nil {
		log.Fatal("Cannot determine unrealsync binary location: " + err.Error())
	}

	flag.Parse()

	for _, dir := range []string{REPO_DIR, REPO_FILES, REPO_TMP} {
		_, err = os.Stat(dir)
		if err != nil {
			err = os.Mkdir(dir, 0777)
			if err != nil {
				log.Fatal("Cannot create " + dir + ": " + err.Error())
			}
		}
	}

	_, err = os.Stat(REPO_CLIENT_CONFIG)
	if err != nil {
		runWizard()
	}

	parseConfig()
	go runFsChangesThread(".")
}

func syncThread() {
	for {
		all_dirs := make(map[string]bool)
		for len(dirschan) > 0 {
			all_dirs[<-dirschan] = true
		}

		if len(all_dirs) > 0 {
			sync(all_dirs)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func writeRepoInfo(dir string, info_map map[string]UnrealStat) {

//	progressLn("Commiting changes at ", dir)

	repo_dir := REPO_FILES + dir
	err := os.MkdirAll(repo_dir, 0777)
	if err != nil {
		progressLn("Cannot mkdir(", repo_dir, "): ", err)
		return
	}

	filename := repo_dir + "/" + DB_FILE
	fp, err := os.OpenFile(filename, os.O_CREATE | os.O_TRUNC | os.O_RDWR, 0666)
	if err != nil {
		progressLn("Cannot open ", filename, " for writing: ", err)
		return
	}
	defer fp.Close()

	result := make([]string, len(info_map) * 2)

	i := 0
	for k, v := range info_map {
		result[i] = k
		result[i + 1] = v.Serialize()
		i += 2
	}

	_, err = fp.WriteString(strings.Join(result, "/"))
	if err != nil {
		progressLn("Cannot write to ", filename, ": ", err)
	}
}

func getRepoInfo(dir string) (result map[string]UnrealStat) {
	result = make(map[string]UnrealStat)
	filename := REPO_FILES + dir + "/" + DB_FILE
	fp, err := os.Open(filename)
	if err != nil {
//		progressLn("Cannot open ", filename, ": ", err)
		return
	}
	defer fp.Close()

	contents, err := ioutil.ReadAll(fp)
	if err != nil {
//		progressLn("Cannot read ", filename, ": ", err)
		return
	}

	elements := strings.Split(string(contents), "/")

	if len(elements) == 0 || len(elements) % 2 != 0 {
		log.Fatal("Broken repository file (inconstent data): " + filename)
	}

	for i := 0; i < len(elements); i += 2 {
		result[elements[i]] = UnrealStatUnserialize(elements[i + 1])
	}

	return
}

func shouldIgnore (path string) bool {
	if path == "." {
		return false
	}

	for _, part := range strings.Split(path, "/") {
		if part == "" {
			continue
		}

		if excludes[part] {
//			progressLn("Ignored: ", path)
			return true
		}
	}

	return false
}

func syncDir(dir string, recursive bool) (unreal_err int) {

	// TODO: support recursive as well

	if shouldIgnore(dir) {
		return
	}

	fp, err := os.Open(dir)
	if err != nil {
		progressLn("Cannot open " + dir + ": " + err.Error())
		unreal_err = ERROR_RECOVERABLE
		return
	}

	defer fp.Close()

	stat, err := fp.Stat()
	if err != nil {
		progressLn("Cannot stat " + dir + ": " + err.Error())
		unreal_err = ERROR_RECOVERABLE
		return
	}

	if !stat.IsDir() {
		progressLn("Suddenly " + dir + " stopped being a directory")
		unreal_err = ERROR_RECOVERABLE
		return
	}

	repo_info := getRepoInfo(dir)

	for {
		res, err := fp.Readdir(10)
		if err != nil {
			if err == io.EOF {
				break
			}

			progressLn("Could not read directory names from " + dir + ": " + err.Error())
			unreal_err = ERROR_RECOVERABLE // it is debatable whether or not this is ok to try to recover from that
			return
		}

		for _, info := range res {
			if shouldIgnore(info.Name()) {
				continue
			}
			repo_el, ok := repo_info[info.Name()]
			if !ok || !StatsEqual(info, repo_el) {
				repo_info[info.Name()] = UnrealStat{
					info.IsDir(),
					(info.Mode() & os.ModeSymlink == os.ModeSymlink),
					int16(uint32(info.Mode()) & 0777),
					info.ModTime().Unix(),
					info.Size(),
				}

				progressLn("Changed: ", dir, "/", info.Name())

				// TODO: add whole directories when they do not exist
				// TODO: should actually send files as well ;)

				progressLn("Pretending to send file using network ;)")
			}
		}

		// TODO: should check for file deletions
	}

	writeRepoInfo(dir, repo_info)

	return
}

func sync(dirs map[string]bool) {
	i := 0
	dirs_list := make([]string, len(dirs))
	for dir := range dirs {
		if shouldIgnore(dir) {
			delete(dirs, dir)
			continue
		}
		dirs_list[i] = dir
		i += 1
	}

	if len(dirs) == 0 {
		return
	}

	progressLn("Changed dirs: ", strings.Join(dirs_list, "; "))

	success := false
	for !success {
		for dir := range dirs {
			// Upon receiving event we can have 'dir' vanish or become a file
			// We should not even try to process them
			stat, err := os.Lstat(dir)
			if err != nil || !stat.IsDir() {
				delete(dirs, dir)
				continue
			}
			unreal_err := syncDir(dir, false)
			if unreal_err == ERROR_RECOVERABLE {
				progressLn("Got recoverable error, trying again in a second")
				time.Sleep(time.Second)
				break
			} else if unreal_err == ERROR_FATAL {
				log.Fatal("Unrecoverable error, exiting (this should never happen! please file a bug report)")
			}
		}

		success = true
	}
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
				go syncThread()
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

		dirschan <- path
	}
}
