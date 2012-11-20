<?php

/* strlen() can be overloaded in mbstring extension, so always using mb_orig_strlen */
if (!function_exists('mb_orig_strlen')) { function mb_orig_strlen($str) { return strlen($str); } }
if (!function_exists('mb_orig_substr')) { function mb_orig_substr() { return call_user_func_array('substr', func_get_args()); } }

try {
    $unrealsync = new Unrealsync();
    $unrealsync->init($argv);
    $unrealsync->run();
} catch (UnrealsyncException $e) {
    fwrite(STDERR, "$unrealsync->hostname\$ unrealsync fatal: " . $e->getMessage() . "\n");
    $unrealsync->debug($e);
    exit(1);
}

class UnrealsyncException extends Exception {}
class UnrealsyncIOException extends UnrealsyncException {}

class Unrealsync
{
    const OS_WIN = 'windows';
    const OS_MAC = 'darwin';
    const OS_LIN = 'linux';

    const REPO = '.unrealsync';
    const REPO_CLIENT_CONFIG = '.unrealsync/client_config';
    const REPO_SERVER_CONFIG = '.unrealsync/server_config';
    const REPO_FILES = '.unrealsync/files';
    const REPO_TMP = '.unrealsync/tmp';
    const REPO_LOCK = '.unrealsync/lock';

    /* Remote commands (length not more than 10 symbols) -> constants map to _cmdValue, e.g. CMD_STAT = 'stat' => _cmdStat */
    const CMD_STAT = 'stat';
    const CMD_SHUTDOWN = 'shutdown';
    const CMD_DIFF = 'diff';
    const CMD_PING = 'ping';

    const MAX_MEMORY = 16777216; // max 16 Mb per read

    var $os, $is_unix = false, $isatty = true, $is_debug = false;
    var $user = 'user';
    var $hostname = 'localhost';
    var $is_server = false; // 'server' mode is when we run at remote server and do not talk to other servers

    /* options from config */
    var $servers = array();
    var $excludes = array();

    var $watcher = array(
        'pp'   => null, // proc_open handle
        'pipe' => null, // stdout pipe
    );

    var $remotes = array(
        // srv => array(pp => ..., read_pipe => ..., write_pipe => ...)
    );

    private $lockfp = null;

    function init($argv)
    {
        unset($argv[0]);
        foreach ($argv as $k => $v) {
            if ($v === '--server') $this->is_server = true;
            else                   continue;
            unset($argv[$k]);
        }

        if ($argv) {
            fwrite(STDERR, "Unrecognized parameters: " . implode(", ", $argv) . "\n");
            fwrite(STDERR, "Usage: unrealsync.php [--server]\n");
            exit(1);
        }

        $this->_setupOS();
        $this->_setupSettings();
    }

    function __destruct()
    {
        if ($this->watcher['pp']) {
            proc_terminate($this->watcher['pp']);
            proc_close($this->watcher['pp']);
        }

        foreach ($this->remotes as $srv => $remote) {
            if ($remote) {
                $this->_remoteExecute($srv, self::CMD_SHUTDOWN);
            }
        }
    }

    private function _setupOS()
    {
        if (PHP_OS == 'WINNT') {
            $this->os = self::OS_WIN;
            return;
        }

        $this->is_unix = true;
        if (!function_exists('posix_isatty') && is_callable('dl')) @dl('posix.' . PHP_SHLIB_SUFFIX);

        if (is_callable('posix_isatty')) $this->isatty = posix_isatty(0);

        if (PHP_OS == 'Darwin') $this->os = self::OS_MAC;
        else if (PHP_OS == 'Linux') $this->os = self::OS_LIN;
        else throw new UnrealsyncException("Unsupported client OS: " . PHP_OS);

        if (isset($_SERVER['USER'])) $this->user = $_SERVER['USER'];
        $this->hostname = trim(`hostname -f`);
    }

    private function _setupUnrealsyncDir()
    {
        if ($this->is_server) chdir(dirname(__DIR__));
        if (is_dir(self::REPO)) return true;
        $old_cwd = getcwd();
        while (realpath('..') != realpath(getcwd())) {
            if (!chdir('..')) break;
            if (is_dir(self::REPO)) break;
        }
        if (!is_dir(self::REPO)) {
            chdir($old_cwd);
            return false;
        }
        return true;
    }

    private function _lock()
    {
        if (!$this->lockfp = fopen(self::REPO_LOCK, 'a')) throw new UnrealsyncException("Cannot open " . self::REPO_LOCK);
        if (defined('LOCK_NB')) {
            if (!flock($this->lockfp, LOCK_EX | LOCK_NB, $wouldblock)) {
                if ($wouldblock) {
                    throw new UnrealsyncException("Another instance of unrealsync already running for this repo");
                } else {
                    throw new UnrealsyncException("Cannot do flock() for " . self::REPO_LOCK);
                }
            }
        } else {
            echo "Trying to obtain lock for " . self::REPO_LOCK . "\n";
            if (!flock($this->lockfp, LOCK_EX)) throw new UnrealsyncException("Cannot do flock for " . self::REPO_LOCK);
        }
    }

    private function _setupSettings()
    {
        if (getenv('UNREALSYNC_DEBUG')) $this->is_debug = true;
        if (!@$this->_setupUnrealsyncDir() && !$this->is_server) $this->_configWizard();
        $this->_lock();
        $this->_loadConfig();
    }

    private function _loadConfig()
    {
        if ($this->is_server) {
            if (file_exists(self::REPO_SERVER_CONFIG)) {
                throw new UnrealsyncException("Server config is not yet implemented");
            }
            return;
        }

        $file = self::REPO_CLIENT_CONFIG;
        $config = parse_ini_file($file, true);
        if ($config === false) throw new UnrealsyncException("Cannot parse ini file $file");
        if (!isset($config['general_settings'])) throw new UnrealsyncException("Section [general_settings] not found in $file");
        $core_settings = $config['general_settings'];
        unset($config['general_settings']);
        if (isset($core_settings['exclude'])) $this->excludes = $core_settings['exclude'];
        if (!count($config)) throw new UnrealsyncException("No server sections in $file");
        $this->servers = $config;
    }

    /**
     * @param $q
     * @param string $default
     * @param mixed $validation Either array of valid answers or function to check the result
     * @throws UnrealsyncException
     * @return string
     */
    function ask($q, $default = '', $validation = null)
    {
        if (!$this->isatty) throw new UnrealsyncException("Cannot ask '$q' because STDIN is not TTY");

        $msg = $q;
        if ($default) $msg .= " [$default]";

        while (true) {
            echo $msg . ": ";
            $answer = fgets(STDIN);
            if ($answer === false || strpos($answer, "\n") === false) {
                throw new UnrealsyncException("Could not read line from STDIN");
            }
            $answer = rtrim($answer);
            if (!strlen($answer)) $answer = $default;
            if (!$validation) return $answer;

            if (is_array($validation)) {
                if (!in_array($answer, $validation)) {
                    fwrite(STDERR, "Valid options are: " . implode(", ", $validation) . "\n");
                    continue;
                }
                return $answer;
            } else if (is_callable($validation)) {
                if (!call_user_func($validation, $answer)) continue;
                return $answer;
            } else {
                throw new UnrealsyncException("Internal error: Incorrect validation argument");
            }
        }
    }

    function askYN($q, $default = 'yes')
    {
        $answer = $this->ask(
            $q,
            $default,
            function ($answer) {
                if (in_array(strtolower($answer), array('yes', 'no', 'y', 'n'))) return true;
                fwrite(STDERR, "Please write either yes or no\n");
                return false;
            }
        );

        return in_array(strtolower($answer), array('yes', 'y'));
    }

    private function _getSshOptions($options)
    {
        $cmd = '';
        if (!empty($options['username'])) $cmd .= " -l " . escapeshellarg($options['username']);
        if (!empty($options['port']))     $cmd .= " -p " . intval($options['port']);
        return $cmd;
    }

    function ssh($hostname, $remote_cmd, $options = array())
    {
        if ($this->is_debug) $remote_cmd = "export UNREALSYNC_DEBUG=1; $remote_cmd";
        $cmd = "ssh -C " . $this->_getSshOptions($options) . " " . escapeshellarg($hostname) . " " . escapeshellarg($remote_cmd);
        $this->debug($cmd);
        if (empty($options['proc_open'])) {
            exec($cmd, $out, $retval);
            if ($retval) return false;
            return implode("\n", $out);
        }

        $result = array();
        $result['pp'] = proc_open($cmd, array(array('pipe', 'r'), array('pipe', 'w'), STDERR), $pipes);
        if (!$result['pp']) return false;

        $result['write_pipe'] = $pipes[0];
        $result['read_pipe'] = $pipes[1];

        return $result;
    }

    function scp($hostname, $local_file, $remote_file, $options = array())
    {
        $file_args = array();
        if (is_array($local_file)) {
            foreach ($local_file as $file) $file_args[] = escapeshellarg($file);
        } else {
            $file_args[] = escapeshellarg($local_file);
        }
        $cmd = "scp " . $this->_getSshOptions($options) . " " . implode(" ", $file_args) . " " . escapeshellarg("$hostname:$remote_file");
        $this->debug($cmd);
        exec($cmd, $out, $retval);
        return $retval ? false : true;
    }

    private function _configWizard()
    {
        echo "Welcome to unrealsync setup wizard\n";
        echo "Unrealsync is utility to do bidirectional sync between several computers\n\n";

        echo "It is highly recommended to have SSH keys set up for passwordless authentication\n";
        echo "Read more about it at http://mah.everybody.org/docs/ssh\n\n";

        $ssh_options = $this->_configWizardSSH();
        $remote_dir = $this->_configWizardRemoteDir($ssh_options);

        if ($this->askYN('Do you want to configure additional settings?', 'no')) {
            throw new UnrealsyncException("Sorry, additional settings are not implemented yet :P");
        }

        if (!mkdir(self::REPO)) throw new UnrealsyncException("Cannot create directory " . self::REPO);
        $config  = "[general_settings]\nexclude[] = .unrealsync\n\n";
        $config .= "[$ssh_options[host]]\n";
        $config .= "host = $ssh_options[host]\n";
        $config .= "dir = $remote_dir\n";
        $config .= "os = $ssh_options[os]\n";
        if (!empty($ssh_options['port'])) $config .= "port = $ssh_options[port]\n";
        if (!empty($ssh_options['username'])) $config .= "username = $ssh_options[username]\n";
        if (!empty($ssh_options['php'])) $config .= "php = $ssh_options[php]\n";
        $config .= "\n";

        if (file_put_contents(self::REPO_CLIENT_CONFIG, $config) !== strlen($config)) {
            throw new UnrealsyncException("Cannot write to " . self::REPO_CLIENT_CONFIG);
        }

        if (!$this->askYN("Going to begin sync now. Continue?")) exit(0);
    }

    private function _configWizardRemoteDir($ssh_options)
    {
        $self = $this;
        return $this->ask(
            "Remote directory to be synced",
            getcwd(),
            function ($dir) use ($ssh_options, $self)
            {
                echo "\rChecking...\r";
                $dir = escapeshellarg($dir);
                $cmd = "if [ -d $dir ]; then echo Exists; else exit 1; fi; if [ -w $dir ]; then echo Writable; fi";
                $result = $self->ssh($ssh_options['host'], $cmd, $ssh_options);
                if (strpos($result, "Exists") === false) {
                    fwrite(STDERR, "\nRemote path $dir is not a directory\n");
                    return false;
                }
                if (strpos($result, "Writable") === false) {
                    fwrite(STDERR, "\nRemote directory $dir exists but it is not writable for you\n");
                    return false;
                }
                echo "Remote directory is OK          \n";
                return true;
            }
        );
    }

    private function _configWizardSSH()
    {
        $os = $username = $host = $port = $php_location = '';
        $self = $this;

        $this->ask(
            "Remote SSH server address",
            "",
            function ($str) use (&$username, &$host, &$port, &$php_location, &$os, $self)
            {
                $os = $php_location = $username = $host = $port = '';
                if (strpos($str, ":") !== false) {
                    list($str, $port) = explode(":", $str, 2);
                    if (!ctype_digit($port)) {
                        fwrite(STDERR, "Port must be numeric\n");
                        return false;
                    }
                }

                if (strpos($str, "@") !== false) {
                    list($username, $host) = explode("@", $str, 2);
                } else {
                    $host = $str;
                }

                $ssh_options = array('username' => $username, 'port' => $port);

                echo "\rChecking connection...\r";
                $result = $self->ssh($host, 'echo uname=`uname`; echo php=`which php`', $ssh_options);
                if ($result === false) {
                    fwrite(STDERR, "\nCannot connect\n");
                    echo "Connection string examples: '$self->hostname', '$self->user@$self->hostname', '$self->user@$self->hostname:2222'\n";
                    return false;
                }

                echo "Connection is OK                 \n";

                $variables = array('uname' => '', 'php' => '');
                foreach (explode("\n", $result) as $ln) {
                    list($k, $v) = explode("=", $ln, 2);
                    $variables[$k] = $v;
                }

                if (!in_array($os = $variables['uname'], array('Linux', 'Darwin'))) {
                    fwrite(STDERR, "Remote OS '$result' is not supported, sorry\n");
                    return false;
                }

                if (!$variables['php']) {
                    $php_location = $self->ask(
                        'Where is PHP? Provide path to "php" binary',
                        '/usr/local/bin/php',
                        function ($path) use ($self, $ssh_options, $host)
                        {
                            $result = $self->ssh($host, escapeshellarg($path) . " --run 'echo PHP_SAPI;'", $ssh_options);
                            if ($result === false) return false;

                            if (trim($result) != "cli") {
                                fwrite(STDERR, "It is not PHP CLI binary ;)\n");
                                return false;
                            }

                            return true;
                        }
                    );
                }

                return true;
            }
        );

        return array('username' => $username, 'host' => $host, 'port' => $port, 'php' => $php_location, 'os' => $os);
    }

    private function _startLocalWatcher()
    {
        $binary = 'exec ' . __DIR__ . '/bin/' . $this->os . '/notify ';
        if ($this->os == self::OS_LIN) {
            $binary .= 'watch .';
        } else if ($this->os == self::OS_MAC) {
            $binary .= '.';
        } else {
            throw new UnrealsyncException("Start local watcher for $this->os is not yet implemented, sorry");
        }
        $pp = proc_open($binary, array(array('file', '/dev/null', 'r'), array('pipe', 'w'), STDERR), $pipes);
        if (!$pp) throw new UnrealsyncException("Cannot start local watcher ($binary)");

        $this->watcher = array(
            'pp' => $pp,
            'pipe' => $pipes[1],
        );
    }

    function debug($msg)
    {
        if (!$this->is_debug) return;
        fwrite(STDERR, $this->hostname . "\$ $msg\n");
    }

    private function _fullRead($fp, $len, $srv = null)
    {
        if ($len <= 0) return '';
        if ($len > self::MAX_MEMORY) {
            throw new UnrealsyncException("Going to read from socket over memory limit ($len bytes)");
        }
        if (!$srv) $srv = $this->hostname;
        $buf = '';
        $chunk_size = 65536;

        $read = array($fp);
        $write = $except = null;
        while (stream_select($read, $write, $except, null)) {
            $result = fread($fp, min($chunk_size, $len - mb_orig_strlen($buf)));
            if (false && $this->is_debug) {
                $this->debug("Read result from $srv");
                ob_start();
                var_dump($result);
                fwrite(STDERR, ob_get_clean());
            }
            if ($result === false || !mb_orig_strlen($result)) {
                throw new UnrealsyncIOException("Cannot read from '$srv'");
            }

            $buf .= $result;
            if (mb_orig_strlen($buf) >= $len) break;
            $read = array($fp);
        }

        return $buf;
    }

    private function _fullWrite($fp, $str, $srv = null)
    {
        if (!mb_orig_strlen($str)) return;
        if (!$srv) $srv = $this->hostname;
        $bytes_sent = 0;
        $chunk_size = 65536;
        $write = array($fp);
        $read = $except = null;
        while (stream_select($read, $write, $except, null)) {
            $chunk = mb_orig_substr($str, $bytes_sent, $chunk_size);
            if (false && $this->is_debug) $this->debug("Writing to $srv: $chunk");
            $result = fwrite($fp, $chunk);
            if (false && $this->is_debug) {
                $this->debug("Write result from $srv: ");
                ob_start();
                var_dump($result);
                fwrite(STDERR, ob_get_clean());
            }

            if ($result === false || $result === 0) {
                throw new UnrealsyncIOException("Cannot write to '$srv'");
            }

            $bytes_sent += $result;
            if ($bytes_sent >= mb_orig_strlen($str)) return;
            $write = array($fp);
        }
    }

    private function _remoteExecute($srv, $cmd, $data = null)
    {
        if (!$this->remotes[$srv]) throw new UnrealsyncException("Incorrect remote server '$srv'");
        $write_pipe = $this->remotes[$srv]['write_pipe'];
        $read_pipe = $this->remotes[$srv]['read_pipe'];

        $start = microtime(true);
//        $this->debug("Remote execute of '$cmd' on '$srv' with data '" . mb_orig_substr($data, 0, 100)  . "'");
        $this->_fullWrite($write_pipe, sprintf("%10s%10u%s", $cmd, mb_orig_strlen($data), $data), $srv);
//        $this->debug("Receiving response from $srv");
        $len = intval($this->_fullRead($read_pipe, 10, $srv));
        $this->debug("Remote '$cmd' executed for " . round((microtime(true) - $start) * 1000) . "ms");
        return $this->_fullRead($read_pipe, $len, $srv);
    }

    private function _bootstrap($srv)
    {
        $data = $this->servers[$srv];
        if (!$data) throw new UnrealsyncException("Internal error: no data for server '$srv'");
        if (!$host = $data['host']) throw new UnrealsyncException("No 'host' entry for '$srv'");
        if (!$dir  = $data['dir']) throw new UnrealsyncException("No 'dir' entry for '$srv'");
        $dir = rtrim($dir, '/');
        $repo_dir = $dir . '/' . self::REPO;
        $dir_esc = escapeshellarg($repo_dir);
        echo "Checking for " . self::REPO . " directory\n";
        $result = $this->ssh($host, "if [ ! -d $dir_esc ]; then exec mkdir $dir_esc; fi; exit 0", $data);
        if ($result === false) throw new UnrealsyncException("Cannot create $dir for '$srv'");
        if (empty($data['os'])) {
            echo "Retreiving OS because it was not specified in config\n";
            $data['os'] = $this->ssh($host, "uname", $data);
            if ($data['os'] === false) throw new UnrealsyncException("Cannot get uname for '$srv");
        }
        $data['os'] = ucfirst(strtolower($data['os']));
        if (!in_array($data['os'], array('Linux', 'Darwin'))) throw new UnrealsyncException("Unsupported remote os '$data[os]' for '$srv'");
        $watcher_path = __DIR__ . '/bin/' . strtolower($data['os']) . '/notify';
        $result = $this->scp($host, array(__FILE__, $watcher_path), $repo_dir . '/');
        if ($result === false) throw new UnrealsyncException("Cannot scp unrealsync.php and watcher to '$srv'");

        echo "Starting unrealsync server on '$srv'\n";

        $result = $this->ssh(
            $host,
            (!empty($data['php']) ? $data['php'] : 'php') . " " . escapeshellarg($repo_dir . '/' . basename(__FILE__)) . " --server",
            $data + array('proc_open' => true)
        );
        if ($result === false) throw new UnrealsyncException("Cannot start unrealsync daemon on '$srv'");

        $this->remotes[$srv] = $result;
        if ($this->_remoteExecute($srv, self::CMD_PING) != "pong") {
            throw new UnrealsyncException("Ping failed. Something went wrong.");
        }

        echo "Getting remote diff for '$srv'\n";
        $remote_diff = $this->_remoteExecute($srv, self::CMD_DIFF);
        $local_diff = $this->_cmdDiff();
    }

    private function _cmdDiff()
    {
        if (!is_dir(self::REPO_FILES) && !mkdir(self::REPO_FILES)) {
            throw new UnrealsyncException("Cannot create directory " . self::REPO_FILES);
        }

        $diff = "";
        $this->_appendDiff(".", $diff);

        return $diff;
    }

    private function _appendDiff($dir, &$diff)
    {
        $dh = opendir($dir);
        $files = array();
        while (false !== ($rel_path = readdir($dh))) {
            if ($rel_path === "." || $rel_path === ".." || $dir === "." && $rel_path === ".realsync") continue;
            $files[] = $rel_path;
        }
        closedir($dh);
        sort($files);
        foreach ($files as $rel_path) {
            $file = "$dir/$rel_path";
            $rfile = self::REPO_FILES . "/$file";
            $stat = $this->_cmdStat($file);
            if (!$stat) {
                $this->debug("Cannot compute lstat for $file");
                continue;
            }

            if (!file_exists($rfile) || is_dir($file) && !is_dir($rfile) || is_file($file) && !is_file($rfile)) {
                $diff .= "file=$file\n\n$stat\n\n\n";
                continue;
            }

            if (!is_link($file) && is_dir($file)) {
                $this->_appendDiff($file, $diff);
                continue;
            }

            $rstat = file_get_contents($rfile);
            if ($stat != $rstat) {
                $diff .= "file=$file\n\n$rstat\n\n$stat\n\n\n";
            }
        }

    }

    private function _cmdPing()
    {
        return "pong";
    }

    private function _cmdStat($filename)
    {
        $result = @lstat($filename);
        if ($result === false) return '';
        return sprintf("mode=%d\nsize=%d\nmtime=%d\nctime=%d", $result['mode'], $result['size'], $result['mtime'], $result['ctime']);
    }

    function runServer()
    {
        while (true) {
            $cmd = trim($this->_fullRead(STDIN, 10));
            $len = intval($this->_fullRead(STDIN, 10));
            $data = $this->_fullRead(STDIN, $len);

            if ($cmd !== self::CMD_SHUTDOWN) {
                $result = call_user_func(array($this, '_cmd' . ucfirst($cmd)), $data);
            } else {
                $result = "";
            }

            $this->_fullWrite(STDOUT, sprintf("%10u", mb_orig_strlen($result)) . $result);
            if ($cmd === self::CMD_SHUTDOWN) exit(0);
        }

        return false;
    }

    function run()
    {
        if ($this->is_server) return $this->runServer();

        $this->_startLocalWatcher();
        foreach ($this->servers as $srv => $srv_data) {
            echo "Doing initial synchronization for $srv\n";
            $this->_bootstrap($srv);
        }
    }
}
