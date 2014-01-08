unrealsync
==========

Utility that can perform bidirectional synchronization between several servers

Prerequisites
=============

 - Linux or Mac OS X (more OS support will be coming in some distant future)

All these tools present on both your machine *and remote server(s)*:

 - go language version 1+
 - clang/gcc available for cgo
 - ssh
 - rsync

Build
=====

Build unrealsync, using "go get && go build" on both your machine and target server(s).
Put target server(s) binaries into unrealsync folder on your machine using the following names:

 - unrealsync-linux for Linux binary
 - unrealsync-darwin for Mac OS X binary

So you should have something like this in your unrealsync directory:

 - src/
 - README.md
 - unrealsync.go
 - unrealsync       # unrealsync binary built for your machine
 - unrealsync-linux # unrealsync binary built for your target server(s)
 - ...

Config
======

1. Create ".unrealsync" directory in the directory that you want to be synchronized
2. Create and edit ".unrealsync/client_config" file:

```
; this is a comment (it is actually parsed as .ini file)
; general settings section (must be present):
[general_settings]
exclude = excludes string ; (optional) excludes, in form "string1|string2|...|stringN"

; you can also put any settings that are common between all servers

; then, create one or more sections (put your name instead of "section")
[section]
dir = remote directory ; target directory on remote server

host = hostname ; (optional) hostname, if it is different from section name
port = port ; (optional) custom ssh port, if needed (default is taken from .ssh/config by ssh utility)
username = username ; (optional) custom ssh login, if needed
bidirectional = true ; (optional) turn on bidirectional synchronization for the specified server (default is false)
compression = false ; (optional) turn off ssh compression, if you have really fast connection (like 1 GBit/s) and unrealsync becomes CPU-bound
disabled = true ; (optional) temporarily disable the specified host and skip synchronization with it
```

Config example
==============

My config looks like this:

```
[general_settings]
exclude = .git
dir = /home/yuriy/project/

[server1]

[server2]

[server3]
bidirectional = true
```

I exclude ".git" directory and synchronize "/home/yuriy/project/" with my local copy. I also have bidirectional support enabled at server3. The two other servers (server1 and server2) have exactly the same settings and thus have completely empty sections in config.

Usage
=====

In order to start unrealsync, there are two supported options:

 - Go to directory to be synchronized and run "unrealsync" without arguments
 - Launch "unrealsync &lt;dir&gt;"

Please note that *unrealsync cannot run as daemon* yet, so you need to have a separate console window open in order for it to work.

After initial synchronization is done, you should be able to edit your files on your local machine and have them synchronized to remote servers with about 100-300 ms delay. Numbers can get higher if you have just made a large number of changes or have slow server connection.

If you want bidirectional synchronization, you must turn it on in config by adding "bidirectional = true" at required servers sections (or put it into general_settings, although it is probably not what you want).
