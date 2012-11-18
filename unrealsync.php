<?php

try {
    $unrealsync = new Unrealsync();
    $unrealsync->run();
} catch (UnrealsyncException $e) {
    fwrite(STDERR, "Fatal error occured: " . $e->getMessage() . "\n");
    exit(1);
}


class UnrealsyncException extends Exception {}

class Unrealsync
{
    const OS_WIN = 'windows';
    const OS_MAC = 'darwin';
    const OS_LIN = 'linux';

    const REPO = '.unrealsync';
    const REPO_CONFIG = '.unrealsync/config';
    const REPO_FILES = '.unrealsync/files';

    var $os, $is_unix = false;
    var $user = 'user';
    var $hostname = 'localhost';

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
        if (PHP_OS == 'Darwin') $this->os = self::OS_MAC;
        else if (PHP_OS == 'Linux') $this->os = self::OS_LIN;
        else throw new UnrealsyncException("Unsupported client OS: " . PHP_OS);

        if (isset($_SERVER['USER'])) $this->user = $_SERVER['USER'];
        $this->hostname = trim(`hostname -f`);
    }

    private function _setupSettings()
    {
        if (is_dir(self::REPO)) $this->_loadConfig();
        else $this->_configWizard();
    }

    private function _loadConfig()
    {
        throw new UnrealsyncException("Not yet implemented");
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
        $msg = $q;
        if ($default) $msg .= " [$default]";

        while (true) {
            echo $msg . ": ";
            $answer = rtrim(fgets(STDIN));
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

    function askYN($q, $default = 'Y')
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

    function ssh($hostname, $remote_cmd, $options = array())
    {
        $cmd = "ssh ";
        if (!empty($options['username'])) $cmd .= " -l " . escapeshellarg($options['username']);
        if (!empty($options['port']))     $cmd .= " -p " . intval($options['port']);
        $cmd .= escapeshellarg($hostname) . " " . escapeshellarg($remote_cmd);
        if (empty($options['proc_open'])) {
            exec($cmd, $out, $retval);
            if ($retval) return false;
            return implode("\n", $out);
        }

        throw new UnrealsyncException("SSH proc_open not implemented");
    }

    private function _configWizard()
    {
        echo "Welcome to unrealsync setup wizard\n";
        echo "Unrealsync is utility to do bidirectional sync between several computers\n\n";

        echo "It is highly recommended to have SSH keys set up for passwordless authentication\n";
        echo "Read more about it at http://mah.everybody.org/docs/ssh\n\n";

        echo "Connection string examples: '$this->hostname', '$this->user@$this->hostname', '$this->user@$this->hostname:2222'\n\n";

        $ssh_options = $this->_configWizardSSH();
        $remote_dir = $this->_configWizardRemoteDir($ssh_options);


    }

    private function _configWizardRemoteDir($ssh_options)
    {
        $self = $this;
        return $this->ask(
            "Remote directory to be synced",
            getcwd(),
            function ($str) use ($ssh_options, $self)
            {
                echo "Checking...\n";
                $cmd = "if [ -d " . escapeshellarg($str) . " ]; then echo Exists; fi";
                $result = $self->ssh($ssh_options['host'], $cmd, $ssh_options);
                if ($result !== "Exists") {
                    fwrite(STDERR, "Remote directory '$str' does not seem to exist\n");
                    return false;
                }
                echo "Remote directory OK\n";
                return true;
            }
        );
    }

    private function _configWizardSSH()
    {
        $username = $host = $port = '';
        $self = $this;

        $this->ask(
            "Remote server SSH connection string",
            "",
            function ($str) use (&$username, &$host, &$port, $self)
            {
                $username = $host = $port = '';
                if (strpos($str, ":") !== false) {
                    list($str, $port) = explode(":", $str, 2);
                    if (!ctype_digit($port)) {
                        fwrite(STDERR, "Port must be numeric\n");
                        return false;
                    }
                }

                if (strpos($str, "@") !== false) {
                    list($username, $host) = explode("@", $str, 2);
                }

                $host = $str;

                echo "Checking connection...\n";
                $result = $self->ssh($host, 'uname', array('username' => $username, 'port' => $port));
                if ($result === false) {
                    fwrite(STDERR, "Cannot connect\n");
                    return false;
                }

                echo "Connection is OK\n";

                if (!in_array($result, array('Linux', 'Darwin'))) {
                    fwrite(STDERR, "Remote OS '$result' is not supported, sorry\n");
                    return false;
                }

                return true;
            }
        );

        return array('username' => $username, 'host' => $host, 'port' => $port);
    }

    function run()
    {

    }
}
