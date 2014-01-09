// +build linux

package main

/*
extern void doRun(char *path);
extern void free(void *pointer);
*/
import "C"

import (
	"unsafe"
)

const (
	LOCAL_WATCHER_READY = "Initialized"
)

//export receiveChange
func receiveChange(path *C.char) {
	fschanges <- C.GoString(path)
}

func runFsChangesThread(path string) {
	p := C.CString(path)
	C.doRun(p)
	C.free(unsafe.Pointer(p))
}
