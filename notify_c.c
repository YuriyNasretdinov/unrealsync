#include "_cgo_export.h"

#ifdef __DARWIN__

	#include <CoreServices/CoreServices.h>
	#include <CoreFoundation/CoreFoundation.h>
	#include <sys/stat.h>

	static void printChangesFunc(ConstFSEventStreamRef streamRef, void *clientCallBackInfo, size_t numEvents, void *eventPaths, const FSEventStreamEventFlags eventFlags[], const FSEventStreamEventId eventIds[]) {
		char **paths = eventPaths;
		int i;
		for (i = 0; i < numEvents; i++) {
			receiveChange(paths[i]);
		}
	}

	void initFSEvents(const char *path) {
		CFStringRef mypath = CFStringCreateWithCString(NULL, path, kCFStringEncodingUTF8);
		CFArrayRef pathsToWatch = CFArrayCreate(NULL, (const void **)&mypath, 1, NULL);
		void *callbackInfo = NULL;
		FSEventStreamRef stream;
		CFAbsoluteTime latency = 0.1;

		stream = FSEventStreamCreate(NULL, &printChangesFunc, callbackInfo, pathsToWatch, kFSEventStreamEventIdSinceNow, latency, kFSEventStreamCreateFlagNone);

		CFRelease(pathsToWatch);
		CFRelease(mypath);

		FSEventStreamScheduleWithRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
		FSEventStreamStart(stream);
		receiveChange("Initialized");
	}

	void doRun(char *path) {
		initFSEvents(path);
		CFRunLoopRun();
	}

#endif
