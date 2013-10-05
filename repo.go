package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	fsLock sync.Mutex
)

func writeRepoInfo(dir string, infoMap map[string]UnrealStat) {

	debugLn("Commiting changes at ", dir)

	oldInfoMap := getRepoInfo(dir)

	repoDir := REPO_FILES + dir
	err := os.MkdirAll(repoDir, 0777)
	if err != nil {
		progressLn("Cannot mkdir(", repoDir, "): ", err)
		return
	}

	filename := repoDir + "/" + DB_FILE
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		progressLn("Cannot open ", filename, " for writing: ", err)
		return
	}
	defer fp.Close()

	result := make([]string, len(infoMap)*2)

	i := 0
	for k, v := range infoMap {
		result[i] = k
		result[i+1] = v.Serialize()
		i += 2
	}

	// Delete deleted directories ;)
	for k, v := range oldInfoMap {
		_, ok := infoMap[k]
		if !ok && v.isDir {
			err := os.RemoveAll(repoDir + "/" + k)
			if err != nil {
				progressLn("Cannot delete ", repoDir, "/", k, ": ", err)
				return
			}
		}
	}

	debugLn("Writing to ", filename)

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

func writeRepoChanges(dirs map[string]map[string]*UnrealStat) {
	for dir, filemap := range dirs {
		infoMap := getRepoInfo(dir)

		for file, stat := range filemap {
			dirPath := REPO_FILES + dir + "/" + file

			if stat == nil {
				if _, ok := infoMap[file]; ok {
					delete(infoMap, file)
				} else if err := os.RemoveAll(dirPath); err != nil && !os.IsNotExist(err) {
					progressLn("Cannot remove ", dirPath, ": ", err.Error())
				}
			} else {
				infoMap[file] = *stat
			}
		}

		writeRepoInfo(dir, infoMap)
	}
}

func commitSingleFile(filename string, bigstat *UnrealStat) {
	dir := filepath.Dir(filename)
	changes := make(map[string]map[string]*UnrealStat)
	changes[dir] = make(map[string]*UnrealStat)
	changes[dir][filepath.Base(filename)] = bigstat
	writeRepoChanges(changes)
}

func lockRepo() {
	fsLock.Lock()
}

func unlockRepo() {
	fsLock.Unlock()
}
