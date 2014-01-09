// +build linux

#include "_cgo_export.h"

// Watcher is based on fsnotifier for linux for IntelliJ IDEA
#define bool int
#define true 1
#define false 0

#include "src/linux/fsnotifier.h"
#include "src/linux/util.c"
#include "src/linux/inotify.c"
#include "src/linux/main.c"
