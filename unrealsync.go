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
)

type Settings struct {
	excludes map[string]bool

	username string
	host     string
	port     int

	dir string
	os string
}

const (
	GENERAL_SECTION = "general_settings"

	REPO_DIR = ".unrealsync/"
	REPO_CLIENT_CONFIG = REPO_DIR + "client_config"
	REPO_SERVER_CONFIG = REPO_DIR + "server_config"
	REPO_FILES = REPO_DIR + "files"
	REPO_TMP = REPO_DIR + "tmp"
	REPO_LOCK = REPO_DIR + "lock"
)

var (
	source_dir string
	unrealsync_dir string
	fschanges = make(chan string, 1000)
	dirschan = make(chan string, 100)
	excludes = map[string]bool{}
	servers = map[string]Settings{}
)

func progress(a ...interface{}) {
	fmt.Fprint(os.Stderr, a...)
}

func progressLn(a ...interface{}) {
	progress(a...)
	progress("\n")
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
	dir := ""

	for {
		all_dirs := make(map[string]bool)

		COLLECT:
		for {
			select {
			case dir = <-dirschan:
				all_dirs[dir] = true
			default:
				break COLLECT
			}
		}

		if len(all_dirs) > 0 {
			sync(all_dirs)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func syncDir(dir string) {
	progressLn("Synchronizing ", dir)
}

func sync(dirs map[string]bool) {
	progress("Changed dirs: ")
	for dir := range dirs {
		progress(dir, "; ")
	}
	progress("\n")

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
			syncDir(dir)
		}
	}

	time.Sleep(time.Second * 10)
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
