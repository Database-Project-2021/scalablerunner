# from enum import Enum, auto
import io
import os
import traceback
from typing import Tuple, Union

import toml

from scalablerunner.util import BaseClass, UtilLogger, update
from scalablerunner.ssh import SSH

class DBRunner(BaseClass):
    """
    Run and configure the Auto-Bencher easily, fully with Python.
    """
    # SSH
    SSH_DEFAULT_RETRY_COUNT = 3
    SSH_DEFAULT_IS_RAISE_ERR = False
    SSH_DEFAULT_CMD_RETRY_COUNT = 2
    SSH_DEFAULT_TIMEOUT = 900

    # Auto-bencher
    AUTOBENCHER_NAME = 'auto-bencher'
    AUTOBENCHER_GITHUB = 'https://github.com/elasql/auto-bencher.git'

    # JARS
    JAR_FOLDER_NAME = 'jars'
    SERVER_JAR_NAME = 'server.jar'
    CLIENT_JAR_NAME = 'client.jar'

    # Some folders under DBRunner
    TEMP_DIR = 'temp'
    CPU_DIR = 'cpu'
    DISK_DIR = 'disk'
    LATENCY_DIR = 'latency'
    NETWORKIN_DIR = 'networkin'
    NETWORKOUT_DIR = 'networkout'
    
    # Name of database
    DB_NAME = 'hermes'

    # Name of configs
    BENCHER_CONFIG = 'bencher.toml'
    LOAD_CONFIG = 'load.toml'
    BENCH_CONFIG = 'bench.toml'

    # Directory of base-config 
    CONFIG_DIR = 'config'
    CURRENT_PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
    BASE_CONFIGS_DIR = os.path.join('data', CONFIG_DIR)

    # Path of base-config 
    BASE_BENCHER_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, BENCHER_CONFIG)
    BASE_LOAD_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, LOAD_CONFIG)
    BASE_BENCH_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, BENCH_CONFIG)

    # Get confiuration in format
    DICT = 'dict'
    TOML = 'toml'

    # Reports
    REPORTS_ON_HOST_DIR = 'reports'
    AUTOBENCHER_REPORTS_DIR = 'reports'
    NEW_NAME_OF_AUTOBENCHER_STATISTICS = 'stats'
    
    def __init__(self, workspace: str="db_runner_workspace") -> None:
        """
        :param str workspace: Customize the name of the workspace of the ``DBRunner``. If you need to run 
            multiple task including benchmark, load test-bed etc concurrently, you need to ensure that 
            ensure that each task executed concurrently has an individual workspace with different names.
        """
        self.is_config_bencher = False
        self.default_is_raise_err = self.SSH_DEFAULT_IS_RAISE_ERR
        self.default_retry_count = self.SSH_DEFAULT_RETRY_COUNT
        self.default_cmd_retry_count = self.SSH_DEFAULT_CMD_RETRY_COUNT
        self.default_timeout = self.SSH_DEFAULT_TIMEOUT
        self.host = None

        # Logger
        self.logger = self._set_UtilLogger(module='Runner', submodule='DBRunner', verbose=UtilLogger.INFO)

        # Set name of workspace
        self.__set_workspace(workspace=workspace)

    def __info(self, *args, **kwargs) -> None:
        """
        Log info via `UtilLogger.info`

        :param *args args: The positional arguments of method `UtilLogger.info`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.info`
        """
        super()._info(*args, **kwargs)

    def __warning(self, *args, **kwargs) -> None:
        """
        Log warning via ``UtilLogger.warning``

        :param *args args: The positional arguments of method `UtilLogger.warning`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.warning`
        """
        super()._warning(*args, **kwargs)
        
    def __error(self, *args, **kwargs) -> None:
        """
        Log error via ``UtilLogger.error``

        :param *args args: The positional arguments of method `UtilLogger.error`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.error`
        """
        super()._error(*args, **kwargs)

    def __type_check(self, *args, **kwargs) -> None:
        """
        Type check via function ``type_check`` in module ``Util``

        :param *args args: The positional arguments of function ``type_check`` in module ``Util``
        :param **kwargs kwargs: The keyword arguments of function ``type_check`` in module ``Util``
        """
        super()._type_check(*args, **kwargs)
    
    def __load_toml(self, toml_file: str) -> dict:
        """
        Load toml file to dictionary
        """
        return toml.load(toml_file)

    def __dump_toml(self, toml_dict: dict) -> str:
        """
        Dump dictionary to string in toml format
        """
        return io.StringIO(toml.dumps(toml_dict))

    def __process_is_raise_err(self, is_raise_err: bool) -> bool:
        """
        Determine to use the new value passed by the user or the default value.
        If the argument is ``None``, use default value instead.
        """
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        if is_raise_err is None:
            return self.default_is_raise_err
        else:
            return is_raise_err

    def __client_exec(self, fn_name: str, is_raise_err: bool=None, going_msg: str=None, finished_msg: str=None, error_msg: str=None, *args, **kwargs) -> Tuple:
        """
        Execute specific function given function name
        """
        self.__type_check(obj=fn_name, obj_type=str, obj_name='fn_name', is_allow_none=False)
        res = None
        try:
            self.__info(going_msg)
            res = getattr(self.host, fn_name)(*args, **kwargs)
            self.__info(finished_msg)
            return res
        except:
            self.__error(error_msg)
            traceback.print_exc()
            if self.__process_is_raise_err(is_raise_err=is_raise_err):
                raise BaseException(f"{error_msg}")
            return res

    def __ssh_exec_command(self, command: str, timeout: int=None, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> Tuple:
        """
        Execute command on remote host

        :param str command: The command would be executed on the remote host
        :param int timeout: Set command’s channel timeout
        :param str going_msg: The ongoing message
        :param str finished_msg: The finished message
        :param str error_msg: The error message
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        # Control 'default_cmd_retry_count' by this method
        res = self.__client_exec(fn_name='exec_command', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg, 
                                 command=command, cmd_retry_count=self.default_cmd_retry_count, get_pty=True, timeout=timeout)
        return res

    def __scp_put(self, files: str, remote_path: str, recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        self.__client_exec(fn_name='put', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg,
                           files=files, remote_path=remote_path, recursive=recursive)

    def __scp_putfo(self, fl: str, remote_path: str, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        self.__client_exec(fn_name='putfo', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg,
                           fl=fl, remote_path=remote_path)
    
    def __scp_get(self, remote_path: str, local_path: str='', recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        self.__client_exec(fn_name='get', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg,
                           remote_path=remote_path, local_path=local_path, recursive=recursive, mode=SSH.SCP)
    
    def __get_stable(self, remote_path: str, local_path: str='', recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        self.__client_exec(fn_name='get', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg,
                           remote_path=remote_path, local_path=local_path, recursive=recursive, mode=SSH.STABLE)

    def __large_put(self, files: str, remote_path: str, recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        self.__client_exec(fn_name='large_put', going_msg=going_msg, finished_msg=finished_msg, error_msg=error_msg,
                           files=files, remote_path=remote_path, recursive=recursive)
    
    def __set_workspace(self, workspace: str) -> None:
        """
        To handle customized workspace name, let the path of the configurations, temp folder, and auto-bencher become dynamic

        :param str workspace: The name of DBRunner's workspace
        """
        self.workspace = workspace
        self.dbrunner_autobencher_path = os.path.join(self.workspace, self.AUTOBENCHER_NAME)
        self.dbrunner_temp_path = os.path.join(self.workspace, self.TEMP_DIR)

        # Directory of configs on DB-Runner
        self.dbrunner_config_dir = os.path.join(self.workspace, self.AUTOBENCHER_NAME)
        self.dbrunner_bencher_config_path = os.path.join(self.dbrunner_config_dir, self.BENCHER_CONFIG)
        self.dbrunner_load_config_path = os.path.join(self.dbrunner_config_dir, self.LOAD_CONFIG)
        self.dbrunner_bench_config_path = os.path.join(self.dbrunner_config_dir, self.BENCH_CONFIG)

    def set_default_is_raise_err(self, default_is_raise_err: bool) -> 'DBRunner':
        """
        Set up default value of ``default_is_raise_err`` of this instance. If you've connect to the host before, 
        you need to reconnect or close and connect to the host again to apply the new settings.

        :param bool default_is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :return: The instance itself
        :rtype: DBRunner
        """
        self.__type_check(obj=default_is_raise_err, obj_type=bool, obj_name='default_is_raise_err', is_allow_none=False)

        self.default_is_raise_err = default_is_raise_err
        return self

    def set_default_retry_count(self, default_retry_count: bool) -> 'DBRunner':
        """
        Set up default value of ``retry_count`` of each operation. If you've connect to the host before, 
        you need to reconnect or close and connect to the host again to apply the new settings.

        :param int default_retry_count: Determine the default retry-count of the class SSH.
        :return: The instance itself
        :rtype: DBRunner
        """
        self.__type_check(obj=default_retry_count, obj_type=int, obj_name='default_retry_count', is_allow_none=False)

        self.default_retry_count = default_retry_count
        return self

    def set_default_cmd_retry_count(self, default_cmd_retry_count: int) -> 'DBRunner':
        """
        Set up default value of ``cmd_retry_count`` of SSH remote command execution.

        :param int default_cmd_retry_count: Determine the default command-retry count of the each SSH remote command execution.
        :return: The instance itself
        :rtype: DBRunner
        """
        self.__type_check(obj=default_cmd_retry_count, obj_type=int, obj_name='default_cmd_retry_count', is_allow_none=False)

        self.default_cmd_retry_count = default_cmd_retry_count
        return self

    def set_default_timeout(self, default_timeout: int) -> 'DBRunner':
        """
        Set up default value of ``timeout`` of SSH remote command execution. If you've connect to the host before, 
        you need to reconnect or close and connect to the host again to apply the new settings.

        :param int default_timeout: Determine the default timeout of the each SSH remote command execution.
        :return: The instance itself
        :rtype: DBRunner
        """
        self.__type_check(obj=default_timeout, obj_type=int, obj_name='default_timeout', is_allow_none=False)

        self.default_timeout = default_timeout
        return self

    def connect(self, hostname: str, username: str=None, password: str=None, port: int=22) -> None:
        """
        Connect to the host that ``Auto-Bencher`` locates

        :param str hostname: The host IP/URL
        :param str username: The username logins to
        :param str password: The password logins to
        :param int port: The SSH port to establish tunnel
        """
        self.hostname = str(hostname)
        self.username = str(username)
        self.password = str(password)
        self.port = int(port)

        self.host = SSH(hostname=self.hostname, username=self.username, password=self.password, port=self.port)
        # Let SSH module control the 'default_is_raise_err' and 'default_retry_count'
        self.host.set_default_is_raise_err(default_is_raise_err=self.default_is_raise_err)
        self.host.set_default_retry_count(default_retry_count=self.default_retry_count)
        self.host.set_default_timeout(default_timeout=self.default_timeout)
        
        self.__client_exec(fn_name='connect', going_msg=f"Connecting to remote host", finished_msg=f"Connected to remote host", error_msg=f"Failed to connect remote host")

    def reconnect(self) -> None:
        """
        Reconnect to the host that ``Auto-Bencher`` locates
        """
        if self.host is not None:
            self.close()
        self.connect(hostname=self.hostname, username=self.username, password=self.password, port=self.port)

    def is_active(self) -> bool:
        """
        Return true if this session is active (open).

        :return: Whether the session is active
        :rtype: bool
        """
        if self.host is not None:
            return self.host.is_active()
        else:
            return False
        
    def close(self) -> None:
        """
        Close the connection to ``Auto-Bencher`` host.
        """
        self.__client_exec(fn_name='close', going_msg=f"Closing the remote host", finished_msg=f"Closed to remote host", error_msg=f"Failed to close remote host")

    def config_bencher(self, sequencer: str=None, servers: list=None, clients: list=None, 
                       user_name: str=None, remote_work_dir: str=None, 
                       dir_name: str=None, package_path: str=None, base_config: str=None, alts: dict=None):
        """
        Config ``bencher.toml``, if the parameters is None, remain the settings in the base_config.

        :param str sequencer: The IP address of the sequencer.
        :param list servers: The list of IP addresses of the servers.
        :param list clients: The list of IP addresses of the clients.
        :param str user_name: The login username of the .
        :param str remote_work_dir: The working directory of the remote host.
        :param str dir_name: The directory of the JDK package.
        :param str package_path: The path of the JDK package.
        :param str base_config: The path of the base configuration of ``bencher.toml``.
        :param dict alts: The alternative options of configuration.
        """
        if base_config is None:
            self.bencher_config = self.__load_toml(self.BASE_BENCHER_CONFIG_PATH)
        else:
            self.bencher_config = self.__load_toml(base_config)

        # System setting
        if not (remote_work_dir is None):
            self.remote_work_dir = str(remote_work_dir)
        if not (user_name is None):
            self.bencher_config['system']['user_name'] = str(user_name)
        if not (remote_work_dir is None):
            self.bencher_config['system']['remote_work_dir'] = str(remote_work_dir)

        # JDK setting
        if not (dir_name is None):
            self.bencher_config['jdk']['dir_name'] = str(dir_name)
        if not (package_path is None):
            self.bencher_config['jdk']['package_path'] = str(package_path)

        # Machines setting
        if not (servers is None):
            self.servers = servers
            self.bencher_config['machines']['servers'] = servers
        else:
            self.servers = self.bencher_config['machines']['servers']
        if not (sequencer is None):
            self.sequencer = sequencer
            self.bencher_config['machines']['sequencer'] = sequencer
        if not (clients is None):
            self.bencher_config['machines']['clients'] = clients

        # Adapt alts
        if not (alts is None):
            self.bencher_config = update(self.bencher_config, alts)
        self.is_config_bencher = True
        # print (toml.dumps(self.bencher_config))
    
    def __verify_machine(self, server_count: int, server_client_ratio: float, max_server_per_machine: int, max_client_per_machine: int):
        self.__type_check(obj=server_count, obj_type=int, obj_name='server_count', is_allow_none=False)
        self.__type_check(obj=server_client_ratio, obj_type=float, obj_name='server_client_ratio', is_allow_none=False)
        self.__type_check(obj=max_server_per_machine, obj_type=int, obj_name='max_server_per_machine', is_allow_none=False)
        self.__type_check(obj=max_client_per_machine, obj_type=int, obj_name='max_client_per_machine', is_allow_none=False)

        # Check whether the number of server machine is enough or not
        # if not (server_count is None or max_server_per_machine is None):
        max_server_count = len(self.bencher_config['machines']['servers']) * max_server_per_machine
        if server_count > max_server_count:
            raise ValueError(f"Too much server, the number of server must <= {max_server_count}, but {server_count}")

        # Check whether the number of client machine is enough or not
        # if not (server_count is None or server_client_ratio is None or max_client_per_machine is None):
        client_count = server_count * server_client_ratio
        max_client_count = len(self.bencher_config['machines']['clients']) * max_client_per_machine
        if client_count > max_client_count:
            raise ValueError(f"Too much client, the number of client must <= {max_client_count}, but {client_count}")
    
    def config_cluster(self, jar_dir: str, server_count: int=None, server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None) -> None:
        """
        Config the section ``auto_bencher`` of the configuration ``load.toml``.

        :param str jar_dir: The directory of the .jar file of the database.
        :param int server_count: The number of the database server instances.
        :param float server_client_ratio: The ratio of the number of the client divided by the number of the server, 
            that is client / server.
        :param int max_server_per_machine: The maximum number of the server instance per machine.
        :param int max_client_per_machine: The maximum number of the client instance per machine.
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")

        self.__type_check(obj=jar_dir, obj_type=str, obj_name='jar_dir', is_allow_none=False)
        self.__type_check(obj=server_count, obj_type=int, obj_name='server_count', is_allow_none=True)
        self.__type_check(obj=server_client_ratio, obj_type=float, obj_name='server_client_ratio', is_allow_none=True)
        self.__type_check(obj=max_server_per_machine, obj_type=int, obj_name='max_server_per_machine', is_allow_none=True)
        self.__type_check(obj=max_client_per_machine, obj_type=int, obj_name='max_client_per_machine', is_allow_none=True)

        # Reset cluster settings
        self.auto_bencher_sec = {}

        # Set up diretory of JARs
        self.jar_folder = jar_dir
        self.auto_bencher_sec['jar_dir'] = str(jar_dir)
        self.jar_dir = os.path.join(self.workspace, self.AUTOBENCHER_NAME, self.JAR_FOLDER_NAME, self.jar_folder)

        if not (server_count is None):
            self.auto_bencher_sec['server_count'] = str(server_count)

        if not (server_client_ratio is None):
            self.auto_bencher_sec['server_client_ratio'] = str(server_client_ratio)
        
        if not (max_server_per_machine is None):
            self.auto_bencher_sec['max_server_per_machine'] = str(max_server_per_machine)
        
        if not (max_client_per_machine is None):
            self.auto_bencher_sec['max_client_per_machine'] = str(max_client_per_machine)
        self.is_config_cluster = True

    def upload_bencher_config(self):
        """
        Upload the ``self.bencher_config`` under the Auto-Bencher directory as ``bencher.toml`` on the host.
        The ``bencher.toml`` would be used by Auto-Bencher as the configuration of itself.
        """
        # Upload bencher.toml
        fl = self.__dump_toml(self.bencher_config)

        self.__scp_putfo(fl=fl, remote_path=self.dbrunner_bencher_config_path, 
                         going_msg=f"Uploading config 'bencher.toml'...",
                         finished_msg=f"Uploaded config 'bencher.toml'",
                         error_msg=f"Failed to upload config 'bencher.toml'")

    def upload_load_config(self):
        """
        Upload the ``self.load_config`` under the Auto-Bencher directory as ``load.toml`` on the host.
        The ``load.toml`` would be used by Auto-Bencher as the configuration of the load-test-bed.
        """
        # Upload load.toml
        fl = self.__dump_toml(self.load_config)
        self.__scp_putfo(fl=fl, remote_path=self.dbrunner_load_config_path,
                         going_msg=f"Uploading config 'load.toml'...",
                         finished_msg=f"Uploaded config 'load.toml'",
                         error_msg=f"Failed to upload config 'load.toml'")

    def upload_bench_config(self):
        """
        Upload the ``self.bench_config`` under the Auto-Bencher directory as ``bench.toml`` on the host. 
        The ``bench.toml`` would be used by Auto-Bencher as the configuration of the benchmark.
        """
        # Upload bench.toml
        fl = self.__dump_toml(self.bench_config)
        self.__scp_putfo(fl=fl, remote_path=self.dbrunner_bench_config_path, 
                         going_msg=f"Uploading config 'bench.toml'...", 
                         finished_msg=f"Uploaded config 'bench.toml'", 
                         error_msg=f"Failed to upload config 'bench.toml'")

    def get_bencher_config(self, format: str) -> Union[dict, str]:
        """
        Get the bencher.toml in dictionary or .toml format

        :param str format: Specify the format, there 2 options: ``DBRunner.DICT`` or ``DBRunner.TOML``
        :return: The contents of the bencher.toml in dictionary or .toml format
        :rtype: Union[dict, str]
        """
        self.__type_check(obj=format, obj_type=str, obj_name='format', is_allow_none=False)

        if format == self.DICT:
            return self.bencher_config
        elif format == self.TOML:
            return self.__dump_toml(self.bencher_config)

    def get_load_config(self, format: str) -> Union[dict, str]:
        """
        Get the load.toml in dictionary or .toml format

        :param str format: Specify the format, there 2 options: DBRunner.DICT or DBRunner.TOML
        :return: The contents of the load.toml in dictionary or .toml format
        :rtype: Union[dict, str]
        """
        self.__type_check(obj=format, obj_type=str, obj_name='format', is_allow_none=False)

        if format == self.DICT:
            return self.load_config
        elif format == self.TOML:
            return self.__dump_toml(self.load_config)

    def get_bench_config(self, format: str) -> Union[dict, str]:
        """
        Get the bench.toml in dictionary or .toml format

        :param str format: Specify the format, there 2 options: DBRunner.DICT or DBRunner.TOML
        :return: The contents of the bench.toml in dictionary or .toml format
        :rtype: Union[dict, str]
        """
        self.__type_check(obj=format, obj_type=str, obj_name='format', is_allow_none=False)

        if format == self.DICT:
            return self.bench_config
        elif format == self.TOML:
            return self.__dump_toml(self.bench_config)

    def init(self) -> Tuple:
        """
        Initialize ElaSQL and Auto-Bencher according to the settings that is set up with the method ``DBRunner.config_bencher``

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")

        # Install autobencher
        self.__ssh_exec_command(f"rm -rf {self.workspace}; mkdir {self.workspace}; cd {self.workspace}; git clone {self.AUTOBENCHER_GITHUB}; cd {self.AUTOBENCHER_NAME}; npm install",
                                going_msg=f"Installing Auto-Bencher...", 
                                finished_msg=f"Installed Auto-Bencher...", 
                                error_msg=f"Failed to install Auto-Bencher")

        # Create JAR directory and create TEMP directory for storing reports temporarily
        self.__ssh_exec_command(f"mkdir -p {self.dbrunner_temp_path}; mkdir -p {self.jar_dir}",
                                going_msg=f"Creating diretories: {self.dbrunner_temp_path} and {self.jar_dir}...", 
                                finished_msg=f"Created diretories: {self.dbrunner_temp_path} and {self.jar_dir}...", 
                                error_msg=f"Failed to create diretories: {self.dbrunner_temp_path} and {self.jar_dir}")

        self.upload_bencher_config()

        # Init Auto-bencher
        stdin, stdout, stderr, is_successed = self.__ssh_exec_command(f'cd {self.dbrunner_autobencher_path}; node src/main.js -c {self.BENCHER_CONFIG} init', 
                                                                      going_msg=f"Initializing database...", 
                                                                      finished_msg=f"Initialized database", 
                                                                      error_msg=f"Failed to initialize database")
        
        return stdin, stdout, stderr, is_successed

    def upload_jars(self, server_jar: str, client_jar: str, use_stable: bool=False) -> None:
        """
        Upload server.jar and client.jar.

        :param str server_jar: The path of the ``server.jar``
        :param str client_jar: The path of the ``client.jar``
        :param bool use_stable: Determine to use SCP or SFTP method to upload files. Generally, SCP method is faster but would occur EOF 
            error sometimes while SFTP method would open a file on remote host and write the contents in binary, which is more stable but slower.
        """
        if not self.is_config_cluster:
            raise BaseException(f"Please call method config_cluster() at first.")

        self.__type_check(obj=server_jar, obj_type=str, obj_name='server_jar', is_allow_none=False)
        self.__type_check(obj=client_jar, obj_type=str, obj_name='client_jar', is_allow_none=False)
        self.__type_check(obj=use_stable, obj_type=bool, obj_name='use_stable', is_allow_none=False)
        
        # Path of the .jar
        remote_server_jar = os.path.join(self.jar_dir, self.SERVER_JAR_NAME)
        remote_client_jar = os.path.join(self.jar_dir, self.CLIENT_JAR_NAME)

        # Message function
        def going_msg_fn(name):
            return f'Uploading {name}'
        def finished_msg_fn(name):
            return f'Uploaded {name}'
        def error_msg_fn(name):
            return f'Failed to upload {name}'

        # Switch between SCP and customized SFTP
        if use_stable:
            for jar, remote_jar, jar_name in zip([server_jar, client_jar], [remote_server_jar, remote_client_jar], [self.SERVER_JAR_NAME, self.CLIENT_JAR_NAME]):
                self.__large_put(files=jar, remote_path=remote_jar, going_msg=going_msg_fn(jar_name), 
                                 finished_msg=finished_msg_fn(jar_name), error_msg=error_msg_fn(jar_name))
        else:
            for jar, remote_jar, jar_name in zip([server_jar, client_jar], [remote_server_jar, remote_client_jar], [self.SERVER_JAR_NAME, self.CLIENT_JAR_NAME]):
                self.__scp_put(files=jar, remote_path=remote_jar, going_msg=going_msg_fn(jar_name), 
                               finished_msg=finished_msg_fn(jar_name), error_msg=error_msg_fn(jar_name))

    def upload_jdk(self, autobencher_jdk: str) -> Tuple:
        """
        Upload the JDK file on to the autobencher machine.

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        if not self.is_config_cluster:
            raise BaseException(f"Please call method config_cluster() at first.")

        self.__type_check(obj=autobencher_jdk, obj_type=str, obj_name='autobencher_jdk', is_allow_none=False)

        stdin, stdout, stderr, is_successed = self.__scp_put(files=autobencher_jdk, remote_path=self.jdk_dir, going_msg=f'Uploading JDK file', 
                                                             finished_msg=f'Uploaded JDK file', error_msg=f'Failed to upload JDK file')
        return stdin, stdout, stderr, is_successed

    def __update_cluster_config(self, config: dict):
        if not config.get('auto_bencher', None) is None:
            config['auto_bencher'].update(self.auto_bencher_sec)
        else:
            config['auto_bencher'] = self.auto_bencher_sec

        self.__verify_machine(server_count=int(config['auto_bencher']['server_count']), 
                              server_client_ratio=float(config['auto_bencher']['server_client_ratio']), 
                              max_server_per_machine=int(config['auto_bencher']['max_server_per_machine']), 
                              max_client_per_machine=int(config['auto_bencher']['max_client_per_machine']))
        return config

    def load(self, alts: dict=None, base_config: str=None, is_kill_java: bool=True) -> Tuple:
        """
        Load test bed

        :param dict alts: The modification would be applied to ``base_config``
        :param str base_config: The path of the load-config for Auto-Bencher and it would be modified by ``alts``
        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")
        # Load base-config
        if base_config is None:
            self.load_config = self.__load_toml(self.BASE_LOAD_CONFIG_PATH)
        else:
            self.load_config = self.__load_toml(base_config)
        # Apply adaptation
        if not (alts is None):
            self.load_config = update(self.load_config, alts)
        # Apply cluster settings
        self.load_config = self.__update_cluster_config(self.load_config)

        # Upload load.toml
        self.upload_load_config()

        # Kill JAVA processes
        self.kill_java()

        # Run load test bed
        stdin, stdout, stderr, is_successed = self.__ssh_exec_command(f'cd {self.dbrunner_autobencher_path}; node src/main.js -c {self.BENCHER_CONFIG} load -d {self.DB_NAME} -p {self.LOAD_CONFIG}', 
                                                                      going_msg=f"Loading test bed...", 
                                                                      finished_msg=f"Loaded test bed", 
                                                                      error_msg=f"Failed to load test bed")
        # Kill JAVA processes
        if is_kill_java:
            self.kill_java()

        return stdin, stdout, stderr, is_successed

    def __transfer_report(self, machine: str, file_name: str, res_dir: str, is_delete_reports: bool) -> None:
        self.__type_check(obj=machine, obj_type=str, obj_name='machine', is_allow_none=False)
        self.__type_check(obj=file_name, obj_type=str, obj_name='file_name', is_allow_none=False)
        self.__type_check(obj=res_dir, obj_type=str, obj_name='res_dir', is_allow_none=False)
        self.__type_check(obj=is_delete_reports, obj_type=bool, obj_name='is_delete_reports', is_allow_none=False)
        
        # Transfer reports to the remote host
        self.__ssh_exec_command(f"scp db-under@{machine}:{file_name} {os.path.join(res_dir, file_name)}", 
                                going_msg=f"Transfering report '{file_name}'' to remote host...", 
                                finished_msg=f"Transfered report '{file_name}' to remote host", 
                                error_msg=f"Failed to transfer report '{file_name}' to remote host")

        # Delete reports on the servers
        if is_delete_reports:
            self.__ssh_exec_command(f"ssh db-under@{machine} 'rm -f {file_name}'", 
                                    going_msg=f"Deleting report '{file_name}' on servers...", 
                                    finished_msg=f"Deleted seport '{file_name}' on servers", 
                                    error_msg=f"Failed to delete report '{file_name}' on servers")

    def collect_results(self, name: str, feature: str='transaction-features', 
                        dependency: str='transaction-dependencies',
                        cpu: str='transaction-cpu-time-server-', 
                        latency: str='transaction-latency-server-', 
                        diskio: str='transaction-diskio-count-server-', 
                        networkin: str='transaction-networkin-size-server-',
                        networkout: str='transaction-networkout-size-server-', is_delete_reports: bool=False) -> None:
        """
        Collect the reports on the servers and sequencer and transfer them to the host

        :param str name: The download path of the reports under the DBRunner workspace of the remote host
        :param str feature: The name of the transaction-features report
        :param str dependency: The name of the transaction-dependencies report
        :param str cpu: The name of the transaction-cpu-time reports
        :param str latency: The name of the transaction-latency reports
        :param str diskio: The name of the transaction-diskio-count reports
        :param str networkin: The name of the transaction-networkin-size reports
        :param str networkout: The name of the transaction-networkout-size reports
        :param bool is_delete_reports: Whether to delete the reports on the server and the sequencer
        """
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=feature, obj_type=str, obj_name='feature', is_allow_none=False)
        self.__type_check(obj=dependency, obj_type=str, obj_name='dependency', is_allow_none=False)
        self.__type_check(obj=cpu, obj_type=str, obj_name='cpu', is_allow_none=False)
        self.__type_check(obj=latency, obj_type=str, obj_name='latency', is_allow_none=False)
        self.__type_check(obj=diskio, obj_type=str, obj_name='diskio', is_allow_none=False)
        self.__type_check(obj=networkin, obj_type=str, obj_name='networkin', is_allow_none=False)
        self.__type_check(obj=networkout, obj_type=str, obj_name='networkout', is_allow_none=False)
        self.__type_check(obj=is_delete_reports, obj_type=bool, obj_name='is_delete_reports', is_allow_none=False)

        # Create directory
        res_dir = os.path.join(self.workspace, self.TEMP_DIR, name)
        self.__ssh_exec_command(f"mkdir -p {res_dir}")

        # For feature reports
        file_name = f"{feature}.csv"
        self.__transfer_report(machine=self.sequencer, file_name=file_name, res_dir=res_dir, is_delete_reports=is_delete_reports)

        # For dependency reports
        file_name = f"{dependency}.txt"
        self.__transfer_report(machine=self.sequencer, file_name=file_name, res_dir=res_dir, is_delete_reports=is_delete_reports)

        # For each type of reports
        for file_dir, file_type in zip([self.CPU_DIR, self.LATENCY_DIR, self.DISK_DIR, self.NETWORKIN_DIR, self.NETWORKOUT_DIR], [cpu, latency, diskio, networkin, networkout]):
            # For each server machine
            for id, server in enumerate(self.servers):
                file_name = f"{file_type}{id}.csv"
                # Transfer reports to the remote host
                self.__transfer_report(machine=server, file_name=file_name, res_dir=res_dir, is_delete_reports=is_delete_reports)

    def move_stats(self, name: str, is_delete_reports: bool=False) -> None:
        """
        Move jobs-0 folder and files job-0-timeline.csv, throughput.csv... etc to the report's path of the DBRunner.

        :param str name: The directory name of the reports
        :param bool is_delete_reports: Whether to delete the reports on the remote host
        """
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=is_delete_reports, obj_type=bool, obj_name='is_delete_reports', is_allow_none=False)

        autobener_reports_dir = os.path.join(self.dbrunner_autobencher_path, self.AUTOBENCHER_REPORTS_DIR)
        reports_dir = os.path.join(self.dbrunner_temp_path, name)
        stats_reports_dir = os.path.join(reports_dir, self.NEW_NAME_OF_AUTOBENCHER_STATISTICS)

        if is_delete_reports:
            # Move statistics reports generatted by autobencher to stats_reports_dir
            operation = "mv $path/$latest_date/$latest_time $new_name;"
        else:
            # Copy statistics reports generatted by autobencher
            operation = "cp -r $path/$latest_date/$latest_time $target; mv $target/$latest_time $new_name;"

        cmd = "path='" + autobener_reports_dir + "'; \
              target='" + reports_dir + "'; \
              new_name='" + stats_reports_dir + "'; \
              latest_date=`ls -t ${path} | head -1`; \
              latest_time=`ls -t ${path}/${latest_date} | head -1`; \
              mkdir -p $target; " + operation + "echo -e \"Move stats from ${latest_date}/${latest_time}\";"
        
        self.__ssh_exec_command(cmd, 
                                going_msg=f"Moving stats '{reports_dir}' on host...", 
                                finished_msg=f"Moved stats '{reports_dir}' on host", 
                                error_msg=f"Failed to move stats '{reports_dir}' on host")


    def pull_reports_to_local(self, name: str, path: str, is_delete_reports: bool=False, use_stable=False) -> None:
        """
        Download the reports on the host to the local

        :param str name: The directory name of the reports
        :param str path: The download path on the local host
        :param bool is_delete_reports: Whether to delete the reports on the remote host
        :param bool use_stable: Determibe whether to use ``SSH.STABLE`` or not, the default value is ``False``, using ``SSH.SP``. 
            But it's under construction.
        """
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=path, obj_type=str, obj_name='path', is_allow_none=False)
        self.__type_check(obj=is_delete_reports, obj_type=bool, obj_name='is_delete_reports', is_allow_none=False)
        self.__type_check(obj=use_stable, obj_type=bool, obj_name='use_stable', is_allow_none=False)

        reports_dir = os.path.join(self.dbrunner_temp_path, name)

        if use_stable:
            self.__warning(f"Stable version is under construction, use default way instead.")
            # self.__get_stable(remote_path=reports_dir, local_path=path, recursive=True, 
            #                   going_msg=f"Pulling reports '{name}' to local '{path}'...", 
            #                   finished_msg=f"Pulled reports '{name}' to local '{path}'", 
            #                   error_msg=f"Failed to pull reports '{name}' to local '{path}'")
        # else:
        self.__scp_get(remote_path=reports_dir, local_path=path, recursive=True, 
                       going_msg=f"Pulling reports '{name}' to local '{path}'...", 
                       finished_msg=f"Pulled reports '{name}' to local '{path}'", 
                       error_msg=f"Failed to pull reports '{name}' to local '{path}'")

        if is_delete_reports:
            self.__ssh_exec_command(f"rm -rf {reports_dir}", 
                                    going_msg=f"Deleting reports '{reports_dir}' on host...", 
                                    finished_msg=f"Deleted reports '{reports_dir}' on host", 
                                    error_msg=f"Failed to delete reports '{reports_dir}' on host")

    def bench(self, reports_path: str, alts: dict=None, base_config: str=None, is_pull_reports: bool=True, is_delete_reports: bool=True, is_kill_java: bool=True, use_stable=False) -> Tuple:
        """
        Run Benchmark

        :param str reports_path: The download path of the reports on the local host
        :param dict alts: The modification would be applied to ``base_config``
        :param str base_config: The path of the load-config for Auto-Bencher and it would be modified by ``alts``
        :param bool is_delete_reports: Whether to delete the reports on the server, sequencer, and the remote host
        :param bool use_stable: Determibe whether to use ``SSH.STABLE`` or not, the default value is ``False``, using ``SSH.SP``
        :return: A tupel contains the standard input/output/error stream after executing the command. 
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")
        # Load base-config
        if base_config is None:
            self.bench_config = self.__load_toml(self.BASE_BENCH_CONFIG_PATH)
        else:
            self.bench_config = self.__load_toml(base_config)
        # Apply adaptation
        if not (alts is None):
            self.bench_config = update(self.bench_config, alts)
        # Apply cluster settings
        self.bench_config = self.__update_cluster_config(self.bench_config)

        # Upload load.toml
        self.upload_bench_config()

        # Kill JAVA processes
        self.kill_java()

        # Run benchmark
        stdin, stdout, stderr, is_successed = self.__ssh_exec_command(f'cd {self.dbrunner_autobencher_path}; node src/main.js -c {self.BENCHER_CONFIG} benchmark -d {self.DB_NAME} -p {self.BENCH_CONFIG}', 
                                                                      going_msg=f"Benchmarking...", 
                                                                      finished_msg=f"Benchmarked", 
                                                                      error_msg=f"Failed to benchmark")

        # Collect reports
        if is_pull_reports:
            self.collect_results(name=self.REPORTS_ON_HOST_DIR, is_delete_reports=is_delete_reports)
            self.move_stats(name=self.REPORTS_ON_HOST_DIR, is_delete_reports=is_delete_reports)
            self.pull_reports_to_local(name=self.REPORTS_ON_HOST_DIR, path=reports_path, is_delete_reports=is_delete_reports, use_stable=use_stable)
            
        # Kill JAVA processes
        if is_kill_java:
            self.kill_java()

        return stdin, stdout, stderr, is_successed

    def execute(self, command: str) -> Tuple:
        """
        Execute the command on the sequencers, servers and clients

        :param str command: The command would be executed on all sequencers, servers, and clients
        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        stdin, stdout, stderr, is_successed = self.__ssh_exec_command(f"cd {self.dbrunner_autobencher_path}; node src/main.js -c {self.BENCHER_CONFIG} exec --command '{command}'", 
                                                        going_msg=f"Executing command {command}...", 
                                                        finished_msg=f"Executed command {command}", 
                                                        error_msg=f"Failed to execute command {command}")
        return stdin, stdout, stderr, is_successed

    def kill_java(self) -> Tuple:
        """
        Kill the Java processes on the sequencers, servers and clients

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        stdin, stdout, stderr, is_successed = self.execute(command="pkill -f java")
        return stdin, stdout, stderr, is_successed