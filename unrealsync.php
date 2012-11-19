<?php

try {
    $unrealsync = new Unrealsync();
    $unrealsync->run();
} catch (UnrealsyncException $e) {
    fwrite(STDERR, "unrealsync fatal: " . $e->getMessage() . "\n");
    if (getenv('UNREALSYNC_DEBUG')) fwrite(STDERR, "$e\n");
    exit(1);
}


class UnrealsyncException extends Exception {}

class Unrealsync
{
    const OS_WIN = 'windows';
    const OS_MAC = 'darwin';
    const OS_LIN = 'linux';

    const REPO = '.unrealsync';
    const REPO_CLIENT_CONFIG = '.unrealsync/client_config';
    const REPO_SERVER_CONFIG = '.unrealsync/server_config';
    const REPO_FILES = '.unrealsync/files';

    var $os, $is_unix = false, $isatty = true;
    var $user = 'user';
    var $hostname = 'localhost';

    /* options from config */
    var $servers = array();
    var $excludes = array();

    var $watcher = array(
        'pp'   => null, // proc_open handle
        'pipe' => null, // stdout pipe
    );

    function __construct()
    {
        $this->_setupOS();
        $this->_setupSettings();
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

    private function _setupSettings()
    {
        if (!@$this->_setupUnrealsyncDir()) $this->_configWizard();
        $this->_loadConfig();
    }

    private function _loadConfig()
    {
        $file = self::REPO_CLIENT_CONFIG;
        $config = parse_ini_file($file, true);
        if ($config === false) throw new UnrealsyncException("Cannot parse ini file $file");
        if (!isset($config['unrealsync_core'])) throw new UnrealsyncException("Section [unrealsync_core] not found in $file");
        $core_settings = $config['unrealsync_core'];
        unset($config['unrealsync_core']);
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
        $cmd = "ssh " . $this->_getSshOptions($options) . " " . escapeshellarg($hostname) . " " . escapeshellarg($remote_cmd);
        if (empty($options['proc_open'])) {
            exec($cmd, $out, $retval);
            if ($retval) return false;
            return implode("\n", $out);
        }

        throw new UnrealsyncException("SSH proc_open not implemented");
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
        $config  = "[unrealsync_core]\nexclude[] = .unrealsync\n\n";
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
        $devnull = array('file', '/dev/null', 'r');
        $pp = proc_open($binary, array($devnull, array('pipe', 'w'), $devnull), $pipes);
        if (!$pp) throw new UnrealsyncException("Cannot start local watcher ($binary)");

        $this->watcher = array(
            'pp' => $pp,
            'pipe' => $pipes[1],
        );
    }

    private function _bootstrap($srv)
    {
        $data = $this->servers[$srv];
        if (!$data) throw new UnrealsyncException("Internal error: no data for server '$srv'");
        if (!$host = $data['host']) throw new UnrealsyncException("No 'host' entry for '$srv'");
        if (!$dir  = $data['dir']) throw new UnrealsyncException("No 'dir' entry for '$srv'");
        $dir = rtrim($dir, '/');
        $dir_esc = escapeshellarg($dir);
        echo "Checking for .unrealsync directory\n";
        $result = $this->ssh($host, "if [ ! -d $dir_esc ]; then exec mkdir $dir_esc; fi; exit 0", $data);
        if ($result === false) throw new UnrealsyncException("Cannot create $dir for '$srv'");
        if (empty($data['os'])) {
            echo "Retreiving OS because it was not specified in config\n";
            $data['os'] = $this->ssh($host, "uname", $data);
            if ($data['os'] === false) throw new UnrealsyncException("Cannot get uname for '$srv");
        }
        if (!in_array($data['os'], array('Linux', 'Darwin'))) throw new UnrealsyncException("Unsupported remote os '$data[os]' for '$srv'");
        $watcher_path = __DIR__ . '/bin/' . strtolower($data['os']) . '/notify';
        $result = $this->scp($host, array(__FILE__, $watcher_path), $dir . '/');
        if ($result === false) throw new UnrealsyncException("Cannot scp unrealsync.php and watcher to '$srv'");

        echo "Init sync OK\n";
    }

    function run()
    {
        $this->_startLocalWatcher();
        foreach ($this->servers as $srv => $srv_data) {
            echo "Doing initial synchronization for $srv\n";
            $this->_bootstrap($srv);
        }
    }
}
