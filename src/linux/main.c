/*
 * Copyright 2000-2012 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <errno.h>
#include <limits.h>
#include <mntent.h>
#include <paths.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/inotify.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <syslog.h>
#include <unistd.h>

#define LOG_ENV "FSNOTIFIER_LOG_LEVEL"
#define LOG_ENV_DEBUG "debug"
#define LOG_ENV_INFO "info"
#define LOG_ENV_WARNING "warning"
#define LOG_ENV_ERROR "error"
#define LOG_ENV_OFF "off"

#define VERSION "20130715.1353"
#define VERSION_MSG "fsnotifier " VERSION "\n"

#define USAGE_MSG \
    "fsnotifier - IntelliJ IDEA companion program for watching and reporting file and directory structure modifications.\n\n" \
    "fsnotifier utilizes \"user\" facility of syslog(3) - messages usually can be found in /var/log/user.log.\n" \
    "Verbosity is regulated via " LOG_ENV " environment variable, possible values are: " \
    LOG_ENV_DEBUG ", " LOG_ENV_INFO ", " LOG_ENV_WARNING ", " LOG_ENV_ERROR ", " LOG_ENV_OFF "; default is " LOG_ENV_WARNING ".\n\n" \
    "Use 'fsnotifier --selftest' to perform some self-diagnostics (output will be logged and printed to console).\n"

#define HELP_MSG \
    "Try 'fsnotifier --help' for more information.\n"

#define INSTANCE_LIMIT_TEXT \
    "The <b>inotify</b>(7) instances limit reached. " \
    "<a href=\"http://confluence.jetbrains.net/display/IDEADEV/Inotify+Instances+Limit\">More details.</a>\n"

#define WATCH_LIMIT_TEXT \
    "The current <b>inotify</b>(7) watch limit is too low. " \
    "<a href=\"http://confluence.jetbrains.net/display/IDEADEV/Inotify+Watches+Limit\">More details.</a>\n"

#define MISSING_ROOT_TIMEOUT 1

#define UNFLATTEN(root) (root[0] == '|' ? root + 1 : root)

typedef struct {
  char* path;
  int id;  // negative value means missing root
} watch_root;

static array* roots = NULL;

static int log_level = 0;
static bool self_test = false;

static void init_log();
static void run_self_test();
static bool main_loop();
static int read_input();
static bool update_roots(array* new_roots);
static void unregister_roots();
static bool register_roots(array* new_roots, array* unwatchable, array* mounts);
static array* unwatchable_mounts();
static void inotify_callback(const char* path, int event);
static void report_event(const char* event, const char* path);
static void output(const char* format, ...);
static void check_missing_roots();
static void check_root_removal(const char*);


void doRun(char *path) {
  init_log();
  if (!self_test) {
    userlog(LOG_INFO, "started (v." VERSION ")");
  }
  else {
    userlog(LOG_INFO, "started (self-test mode) (v." VERSION ")");
  }

  int rv = 0;
  roots = array_create(20);
  if (roots != NULL && init_inotify()) {
    set_inotify_callback(&inotify_callback);

    array* new_roots = array_create(20);
    array_push(new_roots, strdup(path));
    update_roots(new_roots);

    if (!self_test) {
      if (!main_loop()) {
        rv = 3;
      }
    }
    else {
      run_self_test();
    }

    unregister_roots();
  }
  else {
    output("GIVEUP\n");
    rv = 2;
  }
  close_inotify();
  array_delete(roots);

  userlog(LOG_INFO, "finished (%d)", rv);
  closelog();

  exit(rv);
}


static void init_log() {
  int level = LOG_WARNING;

  char* env_level = getenv(LOG_ENV);
  if (env_level != NULL) {
    if (strcmp(env_level, LOG_ENV_DEBUG) == 0)  level = LOG_DEBUG;
    else if (strcmp(env_level, LOG_ENV_INFO) == 0)  level = LOG_INFO;
    else if (strcmp(env_level, LOG_ENV_WARNING) == 0)  level = LOG_WARNING;
    else if (strcmp(env_level, LOG_ENV_ERROR) == 0)  level = LOG_ERR;
  }

  if (self_test) {
    level = LOG_DEBUG;
  }

  char ident[32];
  snprintf(ident, sizeof(ident), "fsnotifier[%d]", getpid());
  openlog(ident, 0, LOG_USER);
  log_level = level;
}


void message(MSG id) {
  if (id == MSG_INSTANCE_LIMIT) {
    output("MESSAGE\n" INSTANCE_LIMIT_TEXT);
  }
  else if (id == MSG_WATCH_LIMIT) {
    output("MESSAGE\n" WATCH_LIMIT_TEXT);
  }
  else {
    userlog(LOG_ERR, "unknown message: %d", id);
  }
}


void userlog(int priority, const char* format, ...) {
  if (priority > log_level) {
    return;
  }

  va_list ap;
  va_start(ap, format);
  vfprintf(stderr, format, ap);
  va_end(ap);

  fputc('\n', stderr);

  fflush(stderr);

  if (self_test) {
    const char* level = "debug";
    switch (priority) {
      case LOG_ERR:  level = "error"; break;
      case LOG_WARNING:  level = " warn"; break;
      case LOG_INFO:  level = " info"; break;
    }
    printf("fsnotifier[%d] %s: ", getpid(), level);

    va_start(ap, format);
    vprintf(format, ap);
    va_end(ap);

    printf("\n");
  }
}


static void run_self_test() {
  array* test_roots = array_create(1);
  char* cwd = malloc(PATH_MAX);
  if (getcwd(cwd, PATH_MAX) == NULL) {
    strncpy(cwd, ".", PATH_MAX);
  }
  array_push(test_roots, cwd);
  update_roots(test_roots);
}


static bool main_loop() {
  int inotify_fd = get_inotify_fd();
  int nfds = inotify_fd + 1;

  while (true) {
    usleep(50000);
    if (!process_inotify_input()) return false;
  }
}


static bool update_roots(array* new_roots) {
  userlog(LOG_INFO, "updating roots (curr:%d, new:%d)", array_size(roots), array_size(new_roots));

  unregister_roots();

  if (array_size(new_roots) == 0) {
    array_delete(new_roots);
    return true;
  }
  else if (array_size(new_roots) == 1 && strcmp(array_get(new_roots, 0), "/") == 0) {  // refuse to watch entire tree
    output("UNWATCHEABLE\n/\n#\n");
    array_delete_vs_data(new_roots);
    return true;
  }

  array* mounts = unwatchable_mounts();
  if (mounts == NULL) {
    return false;
  }

  array* unwatchable = array_create(20);
  if (!register_roots(new_roots, unwatchable, mounts)) {
    return false;
  }

  array_delete_vs_data(unwatchable);
  array_delete_vs_data(mounts);
  array_delete_vs_data(new_roots);

  receiveChange("Initialized");

  return true;
}


static void unregister_roots() {
  watch_root* root;
  while ((root = array_pop(roots)) != NULL) {
    userlog(LOG_INFO, "unregistering root: %s", root->path);
    unwatch(root->id);
    free(root->path);
    free(root);
  };
}


static bool register_roots(array* new_roots, array* unwatchable, array* mounts) {
  int i;

  for (i=0; i<array_size(new_roots); i++) {
    char* new_root = array_get(new_roots, i);
    char* unflattened = UNFLATTEN(new_root);
    userlog(LOG_INFO, "registering root: %s", new_root);

    if (unflattened[0] != '/') {
      userlog(LOG_WARNING, "invalid root: %s", new_root);
      continue;
    }

    array* inner_mounts = array_create(5);
    CHECK_NULL(inner_mounts, false);

    bool skip = false;
    int j;
    for (j=0; j<array_size(mounts); j++) {
      char* mount = array_get(mounts, j);
      if (is_parent_path(mount, unflattened)) {
        userlog(LOG_INFO, "watch root '%s' is under mount point '%s' - skipping", unflattened, mount);
        CHECK_NULL(array_push(unwatchable, strdup(unflattened)), false);
        skip = true;
        break;
      }
      else if (is_parent_path(unflattened, mount)) {
        userlog(LOG_INFO, "watch root '%s' contains mount point '%s' - partial watch", unflattened, mount);
        char* copy = strdup(mount);
        CHECK_NULL(array_push(unwatchable, copy), false);
        CHECK_NULL(array_push(inner_mounts, copy), false);
      }
    }
    if (skip) {
      continue;
    }

    int id = watch(new_root, inner_mounts);
    array_delete(inner_mounts);

    if (id >= 0 || id == ERR_MISSING) {
      watch_root* root = malloc(sizeof(watch_root));
      CHECK_NULL(root, false);
      root->id = id;
      root->path = strdup(new_root);
      CHECK_NULL(root->path, false);
      CHECK_NULL(array_push(roots, root), false);
    }
    else if (id == ERR_ABORT) {
      return false;
    }
    else if (id != ERR_IGNORE) {
      userlog(LOG_WARNING, "watch root '%s' cannot be watched: %d", unflattened, id);
      CHECK_NULL(array_push(unwatchable, strdup(unflattened)), false);
    }
  }

  return true;
}


static bool is_watchable(const char* fs) {
  // don't watch special and network filesystems
  return !(strncmp(fs, "dev", 3) == 0 || strcmp(fs, "proc") == 0 || strcmp(fs, "sysfs") == 0 || strcmp(fs, MNTTYPE_SWAP) == 0 ||
           (strncmp(fs, "fuse", 4) == 0 && strcmp(fs, "fuseblk") != 0) ||
           strcmp(fs, "cifs") == 0 || strcmp(fs, MNTTYPE_NFS) == 0);
}

static array* unwatchable_mounts() {
  FILE* mtab = setmntent(_PATH_MOUNTED, "r");
  if (mtab == NULL) {
    userlog(LOG_ERR, "cannot open " _PATH_MOUNTED);
    return NULL;
  }

  array* mounts = array_create(20);
  CHECK_NULL(mounts, NULL);

  struct mntent* ent;
  while ((ent = getmntent(mtab)) != NULL) {
    userlog(LOG_DEBUG, "mtab: %s : %s", ent->mnt_dir, ent->mnt_type);
    if (strcmp(ent->mnt_type, MNTTYPE_IGNORE) != 0 && !is_watchable(ent->mnt_type)) {
      CHECK_NULL(array_push(mounts, strdup(ent->mnt_dir)), NULL);
    }
  }

  endmntent(mtab);
  return mounts;
}


static void inotify_callback(const char* path, int event) {
  if (event & (IN_CREATE | IN_MOVED_TO)) {
    report_event("CREATE", path);
    report_event("CHANGE", path);
  }
  else if (event & IN_MODIFY) {
    report_event("CHANGE", path);
  }
  else if (event & IN_ATTRIB) {
    report_event("STATS", path);
  }
  else if (event & (IN_DELETE | IN_MOVED_FROM)) {
    report_event("DELETE", path);
  }
  if (event & (IN_DELETE_SELF | IN_MOVE_SELF)) {
    check_root_removal(path);
  }
  else if (event & IN_UNMOUNT) {
    output("RESET\n");
    userlog(LOG_DEBUG, "RESET");
  }
}

static void report_event(const char* event, const char* path) {
  userlog(LOG_DEBUG, "%s: %s", event, path);
  receiveChange((char*)path);
}


static void output(const char* format, ...) {
  if (self_test) {
    return;
  }

  va_list ap;
  va_start(ap, format);
  vfprintf(stderr, format, ap);
  va_end(ap);

  fflush(stderr);
}


static void check_missing_roots() {
  struct stat st;
  int i;
  for (i=0; i<array_size(roots); i++) {
    watch_root* root = array_get(roots, i);
    if (root->id < 0) {
      char* unflattened = UNFLATTEN(root->path);
      if (stat(unflattened, &st) == 0) {
        root->id = watch(root->path, NULL);
        userlog(LOG_INFO, "root restored: %s\n", root->path);
        report_event("CREATE", unflattened);
        report_event("CHANGE", unflattened);
      }
    }
  }
}

static void check_root_removal(const char* path) {
  int i;
  for (i=0; i<array_size(roots); i++) {
    watch_root* root = array_get(roots, i);
    if (root->id >= 0 && strcmp(path, UNFLATTEN(root->path)) == 0) {
      unwatch(root->id);
      root->id = -1;
      userlog(LOG_INFO, "root deleted: %s\n", root->path);
      report_event("DELETE", path);
    }
  }
}
