<?php

/* strlen() can be overloaded in mbstring extension, so always using mb_orig_strlen */
if (!function_exists('mb_orig_strlen')) {
    function mb_orig_strlen($str)
    {
        return strlen($str);
    }
}
if (!function_exists('mb_orig_substr')) {
    function mb_orig_substr($str, $offset, $len = null)
    {
        return isset($len) ? substr($str, $offset, $len) : substr($str, $offset);
    }
}
if (!function_exists('mb_orig_strpos')) {
    function mb_orig_strpos($haystack, $needle, $offset = 0)
    {
        return strpos($haystack, $needle, $offset);
    }
}

function force_unrealsync_destruct()
{
    global $unrealsync;
    if ($unrealsync) {
        $unrealsync->finish();
        $unrealsync = null;
    }
}

register_shutdown_function('force_unrealsync_destruct');

try {
    $unrealsync = new Unrealsync();
    $unrealsync->init($argv);
    $unrealsync->run();
} catch (UnrealsyncException $e) {
    fwrite(STDERR, "$unrealsync->hostname\$ unrealsync fatal: " . $e->getMessage() . "\n");
    $unrealsync->debug($e);
    exit(1);
}

class UnrealsyncException extends Exception
{
}

class UnrealsyncIOException extends UnrealsyncException
{
}

class UnrealsyncFileException extends UnrealsyncException
{
}

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
    const REPO_PID = '.unrealsync/pid';
    const REPO_NOTIFY_PID = '.unrealsync/notify_pid';

    /* Remote commands (length not more than 10 symbols) -> constants map to _cmdValue, e.g. CMD_STAT = 'stat' => _cmdStat */
    const CMD_STAT = 'stat';
    const CMD_SHUTDOWN = 'shutdown';
    const CMD_DIFF = 'diff';
    const CMD_PING = 'ping';
    const CMD_GET_FILES = 'getfiles';
    const CMD_COMMIT = "commit";
    const CMD_APPLY_DIFF = "applydiff";
    const CMD_BIG_INIT = "biginit";
    const CMD_CHUNK_RCV = "chunkrcv";
    const CMD_BIG_COMMIT = "bigcommit";
    const CMD_BIG_ABORT = "bigabort";

    /* diff answers */
    const EMPTY_LOCAL  = "local\n";
    const EMPTY_REPO   = "repo\n";
    const EMPTY_HEADER = "EMPTY:";

    const MAX_MEMORY = 2097152; // max 2 Mb per read
    const MAX_CONFLICTS = 20;

    const CONTENT_HEADER_LENGTH = 10;
    const SEPARATOR = "\n------------\n"; // separator for diff records

    var $os, $is_unix = false, $isatty = true, $is_debug = false;
    var $user = 'user';
    var $hostname = 'localhost';
    var $is_server = false; // 'server' mode is when we run at remote server and do not talk to other servers

    /* options from config */
    var $servers = array();

    var $watcher = array(
        'pp' => null, // proc_open handle
        'pipe' => null, // stdout pipe
    );

    var $remotes = array(
        // srv => array(pp => ..., read_pipe => ..., write_pipe => ...)
    );

    /* default ignores */
    var $exclude = array(
        '.' => true, '..' => true, '.unrealsync' => true
    );

    var $onsync = false; // command executed upon successul sync

    private $lockfp = null;
    private $timers = array();

    private $finished = false;
    private $tmp;

    private $diff = ""; // current diff
    private $chunk_filename = "", $chunk_tmp_filename = "", $chunk_fp;
    private $had_changes = false;

    function init($argv)
    {
        unset($argv[0]);
        foreach ($argv as $k => $v) {
            if ($v === '--server')     $this->is_server = true;
            else if ($v === '--debug') $this->is_debug = true;
            else                       continue;
            unset($argv[$k]);
        }

        $argv = array_values($argv);

        if (count($argv) === 1) {
            if (!chdir($argv[0])) {
                fwrite(STDERR, "Cannot chdir to $argv[0]\n");
                exit(1);
            }

            $argv = array();
        }

        if ($argv) {
            fwrite(STDERR, "Unrecognized parameters: " . implode(", ", $argv) . "\n");
            fwrite(STDERR, "Usage: unrealsync.php [--debug] [<dir>]\n");
            exit(1);
        }

        $this->_setupPHP();
        $this->_setupOS();
        $this->_setupSettings();
    }

    function __destruct()
    {
        $this->finish();
    }

    function finish()
    {
        if ($this->finished) return;
        $this->finished = true;
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

    function log($msg)
    {
        echo (@date('H:i:s')) . " $msg\n";
    }

    /* PHP must write all errors to STDERR as otherwise it will interfere with our communication protocol that uses STDOUT */
    private function _setupPHP()
    {
        error_reporting(E_ALL | E_STRICT);
        ini_set('display_errors', 0);
        ini_set('log_errors', 1);
        ini_set('error_log', null);
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
        if ($this->is_server) chdir(dirname(__FILE__));

        if (file_exists(self::REPO_PID)) unlink(self::REPO_PID);
        if (file_exists(self::REPO_NOTIFY_PID)) unlink(self::REPO_NOTIFY_PID);

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
        if (!is_dir(self::REPO_TMP) && !mkdir(self::REPO_TMP)) {
            throw new UnrealsyncException("Cannot create directory " . self::REPO_TMP);
        }
        if (!is_dir(self::REPO_FILES) && !mkdir(self::REPO_FILES)) {
            throw new UnrealsyncException("Cannot create directory " . self::REPO_FILES);
        }
        return true;
    }

    private function _lock()
    {
        $this->lockfp = fopen(self::REPO_LOCK, 'a');
        if (!$this->lockfp) throw new UnrealsyncException("Cannot open " . self::REPO_LOCK);
        if (defined('LOCK_NB')) {
            if (!flock($this->lockfp, LOCK_EX | LOCK_NB, $wouldblock)) {
                if ($wouldblock) {
                    throw new UnrealsyncException("Another instance of unrealsync already running for this repo");
                } else {
                    throw new UnrealsyncException("Cannot do flock() for " . self::REPO_LOCK);
                }
            }
        } else {
            $this->log("Trying to obtain lock for " . self::REPO_LOCK);
            if (!flock($this->lockfp, LOCK_EX)) throw new UnrealsyncException("Cannot do flock for " . self::REPO_LOCK);
        }

        if (!file_put_contents(self::REPO_PID, getmypid())) {
            throw new UnrealsyncException("Cannot write pid to " . self::REPO_PID);
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
        if (isset($core_settings['exclude'])) {
            foreach ($core_settings['exclude'] as $excl) {
                $this->exclude[$excl] = true;
            }
        }

        foreach ($config as $section => &$row) {
            if (!empty($row['disabled'])) {
                unset($config[$section]);
                continue;
            }

            foreach ($core_settings as $k => $v) {
                if (empty($row[$k])) $row[$k] = $v; //allow default values
            }

            if (!isset($row['host'])) $row['host'] = $section;
        }
        unset($row);

        if (!empty($core_settings['onsync'])) $this->onsync = $core_settings['onsync'];

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

            if (is_callable($validation)) {
                if (!call_user_func($validation, $answer)) continue;
                return $answer;
            } else if (is_array($validation)) {
                if (!in_array($answer, $validation)) {
                    fwrite(STDERR, "Valid options are: " . implode(", ", $validation) . "\n");
                    continue;
                }
                return $answer;
            } else {
                throw new UnrealsyncException("Internal error: Incorrect validation argument");
            }
        }
    }

    private function _checkYN($answer)
    {
        if (in_array(strtolower($answer), array('yes', 'no', 'y', 'n'))) return true;
        fwrite(STDERR, "Please write either yes or no\n");
        return false;
    }

    function askYN($q, $default = 'yes')
    {
        $answer = $this->ask($q, $default, array($this, '_checkYN'));
        return in_array(strtolower($answer), array('yes', 'y'));
    }

    private function _getSshOptions($options)
    {
        $cmd = " -C -o BatchMode=yes -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ";
        if (!empty($options['username'])) $cmd .= " -o User=" . escapeshellarg($options['username']);
        if (!empty($options['port']))     $cmd .= " -o Port=" . intval($options['port']);
        return $cmd;
    }

    function ssh($hostname, $remote_cmd, $options = array())
    {
        if ($this->is_debug) $remote_cmd = "export UNREALSYNC_DEBUG=1; $remote_cmd";
        $cmd = "ssh " . $this->_getSshOptions($options) . " " . escapeshellarg($hostname) . " " . escapeshellarg($remote_cmd);
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

    private function _verifyRemoteDir($dir)
    {
        echo "\rChecking...\r";
        $dir = escapeshellarg($dir);
        $cmd = "if [ -d $dir ]; then echo Exists; else exit 1; fi; if [ -w $dir ]; then echo Writable; fi";
        $result = $this->ssh($this->tmp['host'], $cmd, $this->tmp['ssh_options']);
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

    private function _configWizardRemoteDir($ssh_options)
    {
        $this->tmp['ssh_options'] = $ssh_options;
        return $this->ask(
            "Remote directory to be synced",
            getcwd(),
            array($this, '_verifyRemoteDir')
        );
    }

    private function _configWizardAskRemoteAddress($str)
    {
        $os = $php_location = $username = $host = $port = '';
        $this->tmp = array(
            'os' => &$os, 'php' => &$php_location, 'username' => &$username, 'host' => &$host, 'port' => &$port
        );

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

        $this->tmp['ssh_options'] = $ssh_options = array('username' => $username, 'port' => $port);

        echo "\rChecking connection...\r";
        $result = $this->ssh($host, 'echo uname=`uname`; echo php=`which php`', $ssh_options);
        if ($result === false) {
            fwrite(STDERR, "\nCannot connect\n");
            echo "Connection string examples: '$this->hostname', '$this->user@$this->hostname', '$this->user@$this->hostname:2222'\n";
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
            $this->tmp['php'] = $this->ask(
                'Where is PHP? Provide path to "php" binary',
                '/usr/local/bin/php',
                array($this, '_checkSSHBinary')
            );
        }

        return true;
    }

    private function _checkSSHBinary($path)
    {
        $host = $this->tmp['host'];
        $ssh_options = $this->tmp['ssh_options'];
        $result = $this->ssh($host, escapeshellarg($path) . " --run 'echo PHP_SAPI;'", $ssh_options);
        if ($result === false) return false;

        if (trim($result) != "cli") {
            fwrite(STDERR, "It is not PHP CLI binary ;)\n");
            return false;
        }

        return true;
    }

    private function _configWizardSSH()
    {
        $this->ask("Remote SSH server address", "", array($this, '_configWizardAskRemoteAddress'));
        return $this->tmp;
    }

    private function _startLocalWatcher()
    {
        $binary = 'exec ' . __DIR__ . '/bin/' . $this->os . '/notify ';
        if ($this->os == self::OS_LIN || $this->os == self::OS_MAC) {
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

        $status = proc_get_status($pp);
        if (!$status) throw new UnrealsyncException("Cannot get notify proc status");
        if (!file_put_contents(self::REPO_NOTIFY_PID, $status['pid'])) {
            throw new UnrealsyncException("Cannot write " . self::REPO_NOTIFY_PID);
        }
    }

    function debug($msg)
    {
        if (!$this->is_debug) return;
        fwrite(STDERR, "\033[1;35m" . $this->hostname . "\033[0m\$ $msg\n");
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
                throw new UnrealsyncIOException("Cannot read from $srv");
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
                throw new UnrealsyncIOException("Cannot write to $srv");
            }

            $bytes_sent += $result;
            if ($bytes_sent >= mb_orig_strlen($str)) return;
            $write = array($fp);
        }
    }

    private function _remoteExecuteAll($cmd, $data = null)
    {
        foreach ($this->remotes as $srv => $_) $this->_remoteExecute($srv, $cmd, $data);
    }

    private function _remoteExecute($srv, $cmd, $data = null)
    {
        if (!$this->remotes[$srv]) throw new UnrealsyncException("Incorrect remote server $srv");
        $write_pipe = $this->remotes[$srv]['write_pipe'];
        $read_pipe = $this->remotes[$srv]['read_pipe'];

        $this->_timerStart();
//        $this->debug("Remote execute of '$cmd' on $srv with data '" . mb_orig_substr($data, 0, 100)  . "'");
        $this->_fullWrite($write_pipe, sprintf("%10s%10u%s", $cmd, mb_orig_strlen($data), $data), $srv);
//        $this->debug("Receiving response from $srv");
        $len = intval($this->_fullRead($read_pipe, self::CONTENT_HEADER_LENGTH, $srv));
        $this->_timerStop("$srv: exec $cmd");
        $this->_timerStart();
        $result = $this->_fullRead($read_pipe, $len, $srv);
        $this->_timerStop("$srv: recv $cmd");
        return $result;
    }

    private function _bootstrap($srv)
    {
        $data = $this->servers[$srv];
        if (!$data) throw new UnrealsyncException("Internal error: no data for server $srv");
        if (!$host = $data['host']) throw new UnrealsyncException("No 'host' entry for $srv");
        if (!$dir  = $data['dir']) throw new UnrealsyncException("No 'dir' entry for $srv");

        $dir = rtrim($dir, '/');
        $repo_dir = $dir . '/' . self::REPO;
        $dir_esc = escapeshellarg($repo_dir);
        $this->log("  Copying files...");
        $php_bin = (!empty($data['php']) ? $data['php'] : 'php');
        $cmd = "if [ ! -d $dir_esc ]; then mkdir $dir_esc; fi; ";
        $remote_files = array(__FILE__);
        foreach ($remote_files as $k => $f) {
            $rf = escapeshellarg("$repo_dir/" . basename($f));
            $cmd .= "if [ -f $rf ]; then $php_bin -r 'echo \"$k=\" . md5(file_get_contents(\"'$rf'\")) . \"\\n\";'; fi;";
        }

        $result = $this->ssh($host, $cmd, $data);
        $lines = explode("\n", $result);
        foreach ($lines as $ln) {
            if (!$ln) continue;
            list($idx, $rest) = explode("=", $ln, 2);
            $local_md5 = md5(file_get_contents($remote_files[$idx]));
            if (!preg_match('/[a-f0-9]{32}/s', $rest, $m)) continue;
            if ($local_md5 === $m[0]) unset($remote_files[$idx]);
        }

        foreach ($remote_files as $f) {
            $result = $this->scp($host, $f, $repo_dir . '/', $data);
            if ($result === false) throw new UnrealsyncException("Cannot scp $f to $srv");
        }

        $this->log("  Starting unrealsync server...");

        $result = $this->ssh(
            $host,
            $php_bin . " " . escapeshellarg($repo_dir . '/' . basename(__FILE__)) . " --server",
            $data + array('proc_open' => true)
        );
        if ($result === false) throw new UnrealsyncException("Cannot start unrealsync daemon on $srv");

        $this->remotes[$srv] = $result;
        if ($this->_remoteExecute($srv, self::CMD_PING) != "pong") {
            throw new UnrealsyncException("Ping failed. Something went wrong.");
        }
    }

    private function _timerStart($msg = null)
    {
        if ($msg) $this->debug($msg);
        $this->timers[] = microtime(true);
    }

    private function _timerStop($msg)
    {
        $time = microtime(true) - array_pop($this->timers);
        $this->debug(sprintf("%4s ms %s", round($time * 1000), $msg));
    }

    private function _directSystem($cmd)
    {
        $proc = proc_open($cmd, array(STDIN, STDOUT, STDERR), $pipes);
        if (!$proc) return 255;
        return proc_close($proc);
    }

    const SYNC_FROM_LOCAL = "local";
    const SYNC_FROM_REMOTE = "remote";

    private function _sync($srv, $sync_direction)
    {
        $rsync_cmd = "rsync -a --delete ";
        foreach ($this->exclude as $excl => $_) {
            if ($excl == '.' || $excl == '..') continue;
            $rsync_cmd .= "--exclude=" . escapeshellarg($excl) . " ";
        }
        $remote_arg = escapeshellarg($this->servers[$srv]['host'] . ":" . rtrim($this->servers[$srv]['dir'], "/") . "/");
        switch ($sync_direction) {
            case self::SYNC_FROM_LOCAL:
                $this->log("  Rsync to remote server...");
                $cmd = "$rsync_cmd ./ $remote_arg";
                $this->_exec($cmd, $out, $retval);
                if ($retval) throw new UnrealsyncException("Cannot do '$cmd'");
                break;

            case self::SYNC_FROM_REMOTE:
                $this->log("  Rsync from remote server...");
                $cmd = "$rsync_cmd $remote_arg ./";
                $this->_exec($cmd, $out, $retval);
                if ($retval) throw new UnrealsyncException("Cannot do '$cmd'");
                break;

            default:
                throw new UnrealsyncException("Unknown sync direction: $sync_direction");
        }

        return true;
    }

    private function _exec($cmd, &$out, &$retval)
    {
        $this->debug($cmd);
        return exec($cmd, $out, $retval);
    }

    /* go through remote diff, apply diff and report about conflicts
       if $with_contents = true, then diff with contents is expected and any conflicts are ignored
     */
    private function _applyRemoteDiff($diff)
    {
        fwrite(STDERR, "$this->hostname\$  Applying remote diff: ");
        $this->_timerStart();
        $offset = 0;
        $stats = array('A' => 0, 'D' => 0, 'M' => 0);
        // We do not use explode() in order to save memory, because we need about 3 times more memory for our case
        // Diff can be large (e.g. 10 Mb) so it is totally worth it

        $recv_list = $conf_list = $conflicts = "";

        while (true) {
            if (($end_pos = mb_orig_strpos($diff, self::SEPARATOR, $offset)) === false) break;
            $chunk = mb_orig_substr($diff, $offset, $end_pos - $offset);
            $offset = $end_pos + mb_orig_strlen(self::SEPARATOR);
            $op = $chunk[0];
            $stats[$op]++;
            $first_line_pos = mb_orig_strpos($chunk, "\n");
            if ($first_line_pos === false) throw new UnrealsyncException("No new line in diff chunk: $chunk");
            $first_line = mb_orig_substr($chunk, 0, $first_line_pos);
            $file = mb_orig_substr($first_line, 2);
            if (!$file) throw new UnrealsyncException("No filename in diff chunk: $chunk");
            $chunk = mb_orig_substr($chunk, $first_line_pos + 1);
            $stat = $this->_stat($file);
            $contents = false;
            if ($op === 'A' || $op === 'M') {
                if ($op === 'A') $diffstat = $chunk;
                else list ($oldstat, $diffstat) = explode("\n\n", $chunk);

                if ($diffstat !== "dir" && strpos($diffstat, "symlink=") === false) {
                    $length = intval(mb_orig_substr($diff, $offset, self::CONTENT_HEADER_LENGTH));
                    if ($length > self::MAX_MEMORY) throw new UnrealsyncException("Too big file, probably commucation error");
                    $offset += self::CONTENT_HEADER_LENGTH;
                    $contents = mb_orig_substr($diff, $offset, $length);
                    $offset += $length;
                }
            }

            /* TODO: write all possible cases, not just simple ones */
            if ($op === 'A') {
                $diffstat = $chunk;
                if ($stat === $diffstat) continue; // the same file was added
                // we did not have file, we need to retrieve its contents
                $this->_writeFile($file, $diffstat, $contents);
                continue;
            } else if ($op === 'D') {
                if ($stat) $this->_removeRecursive($file);
            } else if ($op === 'M') {
                list ($oldstat, $diffstat) = explode("\n\n", $chunk);
                if ($stat === $diffstat) continue; // identical changes
                $this->_writeFile($file, $diffstat, $contents);
            } else {
                throw new UnrealsyncException("Unexpected diff chunk: $chunk");
            }
        }

        if ($stats['A']) fwrite(STDERR, "$stats[A] files added ");
        if ($stats['D']) fwrite(STDERR, "$stats[D] files deleted ");
        if ($stats['M']) fwrite(STDERR, "$stats[M] files changed ");
        fwrite(STDERR, "\n");

        $this->_timerStop("Apply remote diff done");
        $this->debug("Peak memory: " . memory_get_peak_usage(true));
    }

    /*
     * Write file $filename with $stat and $contents to work copy
     * If $commit = true, then repository is updated as well
     */
    private function _writeFile($filename, $stat, $contents, $commit = false)
    {
        $old_stat = $this->_stat($filename);
        if ($stat === "dir") {
            if ($old_stat === "dir") return true;
            $this->_removeRecursive($filename);
            if (!mkdir($filename, 0777, true)) throw new UnrealsyncException("Cannot create dir $filename");
        } else if (strpos($stat, "symlink=") === 0) {
            if (strpos($old_stat, "symlink=") !== 0) $this->_removeRecursive($filename);
            list(, $lnk) = explode("=", $stat, 2);
            if (!symlink($lnk, $filename)) {
                throw new UnrealsyncException("Cannot create symlink $filename");
            }
        } else {
            if ($this->is_unix) {
                if (!is_dir(dirname($filename))) mkdir(dirname($filename), 0777, true);
                $tmp = self::REPO_TMP . "/" . basename($filename);
                $bytes_written = file_put_contents($tmp, $contents);
                if ($bytes_written === false || $bytes_written != mb_orig_strlen($contents)) {
                    throw new UnrealsyncException("Cannot write contents to $tmp");
                }
                foreach (explode("\n", $stat) as $ln) {
                    list($field, $value) = explode("=", $ln);
                    if ($field === "mode" && !chmod($tmp, $value)) {
                        throw new UnrealsyncException("Cannot chmod $filename");
                    } else if ($field === "mtime" && !touch($tmp, $value)) {
                        throw new UnrealsyncException("Cannot set mtime for $filename");
                    }
                }
                if (!rename($tmp, $filename)) {
                    fwrite(STDERR, "Cannot rename $tmp to $filename");
                    return false;
                }
            }
        }

        return $commit ? $this->_commit($filename, $stat) : true;
    }

    /*
     * Update repository entry for $filename
     */
    private function _commit($filename, $stat = null)
    {
        if ($filename === "." || !$filename) return true;

        if (!$stat) $stat = $this->_stat($filename);
        $repof = self::REPO_FILES . "/$filename";
        $rstat = $this->_rstat($filename);

        if ($stat === "dir") {
            if ($rstat && $rstat !== "dir" && !unlink($repof)) throw new UnrealsyncException("Cannot remove $repof");
            if ($rstat !== "dir" && !mkdir($repof, 0777, true)) throw new UnrealsyncException("Cannot create dir $repof");
            return true;
        }

        if ($rstat === "dir") $this->_removeRecursive($repof);
        if (!is_dir(dirname($repof))) mkdir(dirname($repof), 0777, true);

        $tmp = self::REPO_TMP . "/" . basename($filename);
        $bytes_written = file_put_contents($tmp, $stat);
        if ($bytes_written === false || $bytes_written != mb_orig_strlen($stat)) {
            throw new UnrealsyncException("Cannot write contents to $tmp");
        }
        if (!rename($tmp, $repof)) {
            fwrite(STDERR, "$this->hostname\$ Cannot rename $tmp to $repof\n");
            return false;
        }
        return true;
    }

    /*
     * Format:
     *
     * <10-byte file name length>
     * <file name>
     * <10-byte stat length>
     * <file stat>
     * [
     * (sent only if stat contains size (e.g. for regular files))
     * <10-byte file contents len>
     * <file contents>
     * ]
     */
    private function _cmdGetFiles($list)
    {
        $response = "";

        $offset = 0;
        while (true) {
            if (($end_pos = mb_orig_strpos($list, "\n", $offset)) === false) break;
            $file = mb_orig_substr($list, $offset, $end_pos - $offset);
            $offset = $end_pos + 1;
            $stat = $this->_stat($file);
            $response .= sprintf("%10u", mb_orig_strlen($file));
            $response .= $file;
            $response .= sprintf("%10u", mb_orig_strlen($stat));
            $response .= $stat;
            if ($this->_getSizeFromStat($stat)) {
                $contents = file_get_contents($file);
                $response .= sprintf("%10u", mb_orig_strlen($contents));
                $response .= $contents;
            }
        }

        return $response;
    }

    private function _dirIsEmpty($dir)
    {
        $dh = opendir($dir);
        if (!$dh) throw new UnrealsyncException("Cannot open directory $dir");
        $is_empty = true;
        while (false !== ($file = readdir($dh))) {
            if (isset($this->exclude[$file])) continue;
            $is_empty = false;
            break;
        }
        closedir($dh);
        return $is_empty;
    }

    private function _cmdDiff()
    {
        $empty = "";
        if ($this->_dirIsEmpty(".")) $empty .= self::EMPTY_LOCAL;
        if ($this->_dirIsEmpty(self::REPO_FILES)) $empty .= self::EMPTY_REPO;
        if ($empty) return self::EMPTY_HEADER . $empty;

        $diff = "";
        $this->_appendDiff(".");
        return $diff;
    }

    private function _appendContents($file, $stat)
    {
        if (mb_orig_strpos($stat, "symlink=") !== false) return;
        $contents = file_get_contents($file);
        if ($contents === false) throw new UnrealsyncFileException("Cannot read $file");
        $size = $this->_getSizeFromStat($stat);
        if (mb_orig_strlen($contents) != $size) throw new UnrealsyncFileException("Actual file size does not match stat");
        $this->diff .= sprintf("%10u", $size);
        $this->diff .= $contents;
    }

    /* Prevent from having too big diff that exceeds MAX_MEMORY
       If TRUE is returned then file is very big and it's processing must be skipped
    */
    private function _optimizeSendBuffers($diff_str, $file, $stat)
    {
        if (!mb_orig_strlen($stat)) $sz = 0;
        else $sz = $this->_getSizeFromStat($stat) + self::CONTENT_HEADER_LENGTH;

        if (mb_orig_strlen($diff_str) + $sz >= self::MAX_MEMORY) {
            $this->_sendBigFile($file);
            return true;
        } else if (mb_orig_strlen($diff_str) + mb_orig_strlen($this->diff) + $sz >= self::MAX_MEMORY) {
            $this->_sendAndCommitDiff();
        }
        return false;
    }

    private function _cmdBigInit($filename)
    {
        $this->chunk_filename = $filename;
        $this->chunk_tmp_filename = self::REPO_TMP . "/" . basename($filename);
        $this->chunk_fp = fopen($this->chunk_tmp_filename, "w");
        if (!$this->chunk_fp) throw new UnrealsyncException("Cannot open temporary file for chunk");
        return true;
    }

    private function _cmdChunkRcv($chunk)
    {
        if (fwrite($this->chunk_fp, $chunk) !== mb_orig_strlen($chunk)) {
            throw new UnrealsyncException("Could not write chunk");
        }

        return true;
    }

    private function _cmdBigCommit($stat)
    {
        if (!fclose($this->chunk_fp)) {
            throw new UnrealsyncException("Could not fclose() chunk file pointer");
        }
        if (!rename($this->chunk_tmp_filename, $this->chunk_filename)) {
            throw new UnrealsyncException("Could not move $this->chunk_filename");
        }

        return true;
    }

    private function _cmdBigAbort()
    {
        if (!fclose($this->chunk_fp)) {
            throw new UnrealsyncException("Could not fclose() chunk file pointer");
        }
        if (!unlink($this->chunk_tmp_filename)) {
            throw new UnrealsyncException("Could not unlink $this->chunk_filename");
        }
        return true;
    }

    private function _sendBigFile($file)
    {
        $stat = $this->_stat($file);
        if (!$stat) throw new UnrealsyncFileException("File vanished: $file");
        $rstat = $this->_rstat($file);
        if ($stat === $rstat) return;

        $sz = $this->_getSizeFromStat($stat);
        if (!$sz) throw new UnrealsyncException("Internal error: no size for big file");

        $fp = fopen($file, "rb");
        if (!$fp) throw new UnrealsyncFileException("Could not open big file $file for reading");
        fwrite(STDERR, "$this->hostname\$ Sending big file $file (" . round($sz / 1024) . " KiB)...");

        $this->_remoteExecuteAll(self::CMD_BIG_INIT, $file);
        while (mb_orig_strlen($chunk = fread($fp, self::MAX_MEMORY / 2))) {
            if ($stat !== $this->_stat($file)) {
                fwrite(STDERR, "file changed, aborting\n");
                $this->_remoteExecuteAll(self::CMD_BIG_ABORT);
                fclose($fp);
                return;
            }
            $this->_remoteExecuteAll(self::CMD_CHUNK_RCV, $chunk);
            fwrite(STDERR, ".");
        }

        fclose($fp);
        $this->_remoteExecuteAll(self::CMD_BIG_COMMIT, $stat);
        $this->_commit($file, $stat);

        fwrite(STDERR, "done\n");
    }

    private function _appendDiff($dir, $recursive = true)
    {
        // if we cannot open directory, it means that it vanished during diff computation. It is actually acceptable
        @$dh = opendir($dir);
        if (!$dh) throw new UnrealsyncFileException("Cannot opendir($dir)");
        try {
            $files = array();
            while (false !== ($rel_path = readdir($dh))) {
                if (isset($this->exclude[$rel_path])) continue;
                $file = "$dir/$rel_path";
                $stat = $this->_stat($file);
                if (!$stat) throw new UnrealsyncFileException("File vanished: $file");

                $files[$file] = true;
                $rstat = $this->_rstat($file);

                if (!$rstat) {
                    if ($stat === "dir") {
                        $this->_appendAddedFiles($file);
                    } else {
                        $str = "A $file\n$stat" . self::SEPARATOR;
                        if ($this->_optimizeSendBuffers($str, $file, $stat)) continue;
                        $this->diff .= $str;
                        $this->_appendContents($file, $stat);
                    }
                    continue;
                }

                if ($stat === $rstat) {
                    if ($stat === "dir" && $recursive) $this->_appendDiff($file, $recursive);
                    continue;
                }

                $str = "M $file\n$rstat\n\n$stat" . self::SEPARATOR;
                if ($this->_optimizeSendBuffers($str, $file, $stat)) continue;
                $this->diff .= $str;
                $this->_appendContents($file, $stat);
            }
        } catch (Exception $e) {
            closedir($dh);
            throw $e;
        }

        closedir($dh);

        // determine deletions by looking up files that are present in repository but not on disk
        // It is ok if we do not yet have any repository entry for directory because it needs to be commited first
        @$dh = opendir(self::REPO_FILES . "/$dir");
        if ($dh) {
            while (false !== ($rel_path = readdir($dh))) {
                if ($rel_path === "." || $rel_path === "..") continue;
                $file = "$dir/$rel_path";
                if (mb_orig_strlen($this->_stat($file)) > 0) continue;
                $rstat = $this->_rstat($file);
                $str = "D $file\n$rstat" . self::SEPARATOR;
                $this->_optimizeSendBuffers($str, $file, "");
                $this->diff .= $str;
            }
            closedir($dh);
        }
    }

    private function _appendAddedFiles($dir)
    {
        @$dh = opendir($dir);
        if (!$dh) {
            $this->debug("_appendAddedFiles: cannot opendir($dir)");
            return;
        }

        $str = "A $dir\ndir" . self::SEPARATOR;
        $this->_optimizeSendBuffers($str, $dir, "");
        $this->diff .= $str;

        while (false !== ($rel_path = readdir($dh))) {
            if (isset($this->exclude[$rel_path])) continue;
            $file = "$dir/$rel_path";
            $stat = $this->_stat($file);
            if (!$stat) {
                $this->debug("Cannot compute lstat for $file");
                continue;
            }

            if ($stat === "dir") {
                $this->_appendAddedFiles($file);
            } else {
                $str = "A $file\n$stat" . self::SEPARATOR;
                if ($this->_optimizeSendBuffers($str, $file, $stat)) continue;
                $this->diff .= $str;
                $this->_appendContents($file, $stat);
            }
        }
        closedir($dh);
    }

    private function _cmdPing()
    {
        return "pong";
    }

    private function _getSizeFromStat($stat)
    {
        if (mb_orig_strpos($stat, "symlink=") !== false) return 0;

        $offset = mb_orig_strpos($stat, "size=") + 5;
        return mb_orig_substr($stat, $offset, mb_orig_strpos($stat, "\n", $offset) - $offset);
    }

    /* get file stat that we have in repository (if any) */
    private function _rstat($short_filename)
    {
        clearstatcache();
        $filename = self::REPO_FILES . "/$short_filename";
        if (!file_exists($filename)) return "";
        if (is_dir($filename)) return "dir";
        return file_get_contents($filename);
    }

    /* get file stat, that is used to compare local and remote files */
    private function _stat($filename)
    {
        clearstatcache();
        $result = @lstat($filename);
        if ($result === false) return '';
        switch ($result['mode'] & 0170000) {
            case 0040000:
                return "dir";

            case 0120000:
                return "symlink=" . readlink($filename);

            case 0100000: // regular file
                $mode = $result['mode'] & 0777;
                if (!$this->is_unix) {
                    // we only support 777 and 666 (with default umask it is 755 and 644 accordingly) on windows
                    if ($mode & 0700 == 0700) $mode = 0777;
                    else $mode = 0666;
                }
                return sprintf("mode=%d\nsize=%d\nmtime=%d", $mode, $result['size'], $result['mtime']);
        }
        return '';
    }

    /*
     * Commit whole directory, recursively by default
     */
    private function _commitDir($dir = ".", $recursive = true)
    {
        $dh = opendir($dir);
        if (!$dh) {
            fwrite(STDERR, "Cannot open $dir for commiting changes\n");
            return false;
        }

        while (false !== ($rel_path = readdir($dh))) {
            if (isset($this->exclude[$rel_path])) continue;
            $file = "$dir/$rel_path";
            $rfile = self::REPO_FILES . "/$file";

            $stat = $this->_stat($file);
            $rstat = $this->_rstat($file);
            if ($stat === $rstat) {
                if ($stat === "dir" && $recursive) $this->_commitDir($file);
                continue;
            }

            if ($rstat && $stat === "dir") $this->_removeRecursive($rfile);
            $this->_commit($file, $stat);
            if ($stat === "dir") $this->_commitDir($file);
        }

        closedir($dh);

        /* looking for deleted entities */
        $dh = opendir($repo_dir = self::REPO_FILES . "/$dir");
        if (!$dh) {
            fwrite(STDERR, "Cannot open $repo_dir for commiting changes\n");
            return false;
        }

        while (false !== ($rel_path = readdir($dh))) {
            if (isset($this->exclude[$rel_path])) continue;
            $file = "$dir/$rel_path";
            $rfile = self::REPO_FILES . "/$file";

            $stat = $this->_stat($file);
            $rstat = $this->_rstat($file);

            if ($rstat && !$stat) $this->_removeRecursive($rfile);
        }
        closedir($dh);

        return true;
    }

    /* Commit changes that were sent in $diff */
    private function _commitDiff()
    {
        $offset = 0;

//        fwrite(STDERR, "$this->hostname\$ committing diff\n");

        while (true) {
            if (($end_pos = mb_orig_strpos($this->diff, self::SEPARATOR, $offset)) === false) break;
            $chunk = mb_orig_substr($this->diff, $offset, $end_pos - $offset);
            $offset = $end_pos + mb_orig_strlen(self::SEPARATOR);
            $op = $chunk[0];
            $first_line_pos = mb_orig_strpos($chunk, "\n");
            if ($first_line_pos === false) throw new UnrealsyncException("No new line in diff chunk: $chunk");
            $first_line = mb_orig_substr($chunk, 0, $first_line_pos);
            $file = mb_orig_substr($first_line, 2);
            if (!$file) throw new UnrealsyncException("No filename in diff chunk: $chunk");
            $rfile = self::REPO_FILES . "/$file";
            $chunk = mb_orig_substr($chunk, $first_line_pos + 1);

            if ($op === 'A' || $op === 'M') {
                if ($op === 'A') $diffstat = $chunk;
                else list ($oldstat, $diffstat) = explode("\n\n", $chunk);

                if ($diffstat !== "dir" && strpos($diffstat, "symlink=") === false) {
                    $length = intval(mb_orig_substr($this->diff, $offset, self::CONTENT_HEADER_LENGTH));
                    $offset += self::CONTENT_HEADER_LENGTH + $length;
                }

//                fwrite(STDERR, "$this->hostname\$ $op $file\n");
                $this->_commit($file, $diffstat);
            } else if ($op === 'D') {
//                fwrite(STDERR, "$this->hostname\$ Delete $rfile\n");
                $this->_removeRecursive($rfile);
            }
        }
    }

    private function _cmdCommit()
    {
        return $this->_commitDir();
    }

    private function _removeRecursive($path)
    {
        $stat = $this->_stat($path);
        if (!$stat) return true;
        if ($stat != "dir") return unlink($path);

        $dh = opendir($path);
        if (!$dh) return false;

        while (false !== ($rel_path = readdir($dh))) {
            if ($rel_path === "." || $rel_path === "..") continue;
            $this->_removeRecursive("$path/$rel_path");
        }

        closedir($dh);

        return rmdir($path);
    }

    private function _sendAndCommitDiff()
    {
        if ($this->is_server) throw new UnrealsyncException("Send and commit diff is not implemented on server");
        if (!$len = mb_orig_strlen($this->diff)) return;
        $this->had_changes = true;
        $this->log("diff size " . ($len > 1024 ? round($len / 1024) . " KiB" : $len . " bytes"));

        $this->_remoteExecuteAll(self::CMD_APPLY_DIFF, $this->diff);
        $this->_commitDiff();
        $this->diff = "";
    }

    private function _sendDirsDiff($dirs)
    {
        $this->had_changes = false;
        echo "\n";
        $this->log("Changed dirs: " . implode(" ", $dirs));
        foreach ($dirs as $dir) $this->_appendDiff($dir, false);

        if (mb_orig_strlen($this->diff) > 0) $this->_sendAndCommitDiff();
    }

    private function _cmdApplyDiff($diff)
    {
        $this->_applyRemoteDiff($diff);
    }

    /*
     * Filter dirs using exclude and transform absolute paths to relative
     */
    private function _getFilteredDirs($dirs)
    {
        $curdir = getcwd();

        foreach ($dirs as $idx => $dir) {
            if ($dir === $curdir) $dir = ".";
            if (mb_orig_strpos($dir, "$curdir/") === 0) $dir = mb_orig_substr($dir, mb_orig_strlen($curdir) + 1);

            $parts = explode("/", $dir);
            foreach ($parts as $p) {
                if ($p === ".") continue;
                if (isset($this->exclude[$p])) {
                    unset($dirs[$idx]);
                    continue(2);
                }
            }

            /* check if dir is still present, because event could be delivered when dir does not exist anymore */
            $stat = $this->_stat($dir);
            if ($stat !== 'dir') {
                unset($dirs[$idx]);
                continue;
            }
            $dirs[$idx] = $dir;
        }

        return $dirs;
    }

    function runServer()
    {
        while (true) {
            $cmd = trim($this->_fullRead(STDIN, self::CONTENT_HEADER_LENGTH));
            $len = intval($this->_fullRead(STDIN, self::CONTENT_HEADER_LENGTH));
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

        foreach ($this->servers as $srv => $srv_data) {
            $this->log("Preparing $srv");
            $this->_bootstrap($srv);
            $this->_sync($srv, self::SYNC_FROM_LOCAL);
        }

        $this->log("Commiting local changes...");
        if (!$this->_commitDir()) throw new UnrealsyncException("Cannot commit changes locally");

        $this->log("Starting local watcher...");
        $this->_startLocalWatcher();

        $this->log("Started successfully");

        $dir_hashes = array();

        $write = $except = array();
        while (false !== ($ln = fgets($this->watcher['pipe']))) {
            $ln = rtrim($ln);
            if ($ln === "-") {
                $read = array($this->watcher['pipe']);
                if (stream_select($read, $write, $except, 0)) continue; // there is more in pipe, send later
                while (true) {
                    $this->diff = "";
                    $have_errors = false;
                    $dirs = $this->_getFilteredDirs(array_keys($dir_hashes));
                    if (!$dirs) break;
                    try {
                        $this->_sendDirsDiff($dirs);
                    } catch (UnrealsyncFileException $e) {
                        $have_errors = true;
                        fwrite(STDERR, "Got an error during diff computation: " . $e->getMessage() . "\n");
                    }

                    if (!$have_errors) break;

                    fwrite(STDERR, "Got errors during diff computation. Waiting 1s to try again\n");
                    sleep(1);
                }

                if ($this->had_changes && $this->onsync) $this->_directSystem("$this->onsync &");
                $this->had_changes = false;
                $dir_hashes = array();

                continue;
            }
            /* turn changes in separate files into changes in directories for Mac OS X watcher compatibility */
            list(, $file) = explode(" ", $ln, 2);
            $dir_hashes[rtrim($file, "/")] = true;
        }

        return true;
    }
}
