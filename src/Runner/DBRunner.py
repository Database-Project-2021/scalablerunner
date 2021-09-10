from enum import Enum, auto
import io
import os
import sys
from time import sleep
import types
import traceback
from typing import Callable, Collection, Tuple, Type, TypeVar, Union
from pygments.console import colorize

import paramiko
from scp import SCPClient
import toml

from util import delay, info, warning, error, progress, progress4, type_check, update
    
# def delay():
#     sleep(1)

# def info(msg: str) -> None:
#     return colorize('green', msg)

# def warning(msg: str) -> None:
#     return colorize('yellow', msg)
    
# def error(msg: str) -> None:
#     return colorize('red', msg)

# def progress(filename, size, sent):
#     sys.stdout.write("%s's progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100))
#     if float(sent)/float(size) >= 1:
#         sys.stdout.write("\n")

# def progress4(filename, size, sent, peername):
#     sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (peername[0], peername[1], filename, float(sent)/float(size)*100) )
#     if float(sent)/float(size) >= 1:
#         sys.stdout.write("\n")

# def type_check(obj: object, obj_type: Type, obj_name: str, is_allow_none: bool) -> None:
#     if obj_type is callable:
#         if not callable(obj):
#             if not is_allow_none:
#                 raise TypeError(f"Parameter '{obj_name}' should be a callable, but a {type(obj)}")
#             else:
#                 if not (obj == None):
#                     raise TypeError(f"Parameter '{obj_name}' should be a callable or None, but a {type(obj)}")
#     else:
#         if not isinstance(obj, obj_type):
#             if not is_allow_none:
#                 raise TypeError(f"Parameter '{obj_name}' should be {obj_type}, but a {type(obj)}")
#             else:
#                 if not (obj == None):
#                     raise TypeError(f"Parameter '{obj_name}' should be {obj_type} or None, but a {type(obj)}")

class RemoteType(Enum):
        SSH = auto()
        SCP = auto()
        SFTP = auto()
class SSH():
    RECONNECT_TIME_OUT = 20
    RECONNECT_COUNT = 1
    RECONNECT_WAITING = 2

    def __init__(self, hostname: str, username: str, password: str=None, port: int=22) -> None:
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.scpClient = None

    def __info(self, *args, **kwargs) -> None:
        print(f"[SSH] Info: {info(*args, **kwargs)}")

    def __warning(self, *args, **kwargs) -> None:
        print(f"[SSH] Warning: {warning(*args, **kwargs)}")
        
    def __error(self, *args, **kwargs) -> None:
        print(f"[SSH] Error: {error(*args, **kwargs)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)
    
    def __retrying_execution(self, remote_type: RemoteType, fn_name: str, name: str, retry_count: int, *args, **kargs):
        self.__type_check(obj=remote_type, obj_type=RemoteType, obj_name='remote_type', is_allow_none=False)
        self.__type_check(obj=fn_name, obj_type=str, obj_name='fn_name', is_allow_none=False)
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)
        
        try_counter = retry_count + 1
        is_successed = False
        res = None
        try:
            for i in range(try_counter):
                try:
                    if self.client.get_transport() is not None:
                        while not self.client.get_transport().is_active():
                            pass
                    if remote_type is RemoteType.SSH:
                        res = getattr(self.client, fn_name)(*args, **kargs)
                    elif remote_type is RemoteType.SCP:
                        res = getattr(self.scpClient, fn_name)(*args, **kargs)
                    else:
                        raise ValueError(f"No such type of operation in 'RemoteType'")
                    is_successed = True
                    break
                except:
                    is_successed = False
                    if i == 0 and try_counter > 1:
                        traceback.print_exc()
                        self.__warning(f"{name} failed, re-trying.")
                        self.reconnect()
                    elif i > 0:
                        traceback.print_exc()
                        self.__warning(f"{i}-th re-try failed.")
                        self.reconnect()
        finally:
            if is_successed:
                self.__info(f"SUCCESSED: {name}")
            else:
                self.__error(f"FAILED: {name}")
            return res

    def connect(self, timeout: int=20, retry_count: int=3):
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='connect', name="SSH connection", retry_count=retry_count, 
                                  hostname=self.hostname, port=self.port, username=self.username, password=self.password, timeout=timeout)
        # self.scpClient = SCPClient(self.client.get_transport(), progress=progress)
        self.scpClient = SCPClient(self.client.get_transport(), progress4=progress4)

    def reconnect(self):
        self.__warning(f"Reconnecting... Waiting for {self.RECONNECT_WAITING}s")
        self.client.close()
        sleep(self.RECONNECT_WAITING)
        self.connect(timeout=self.RECONNECT_TIME_OUT, retry_count=self.RECONNECT_COUNT)

    def exec_command(self, command: str, is_show_result: bool=True, retry_count: int=3):
        self.__type_check(obj=is_show_result, obj_type=bool, obj_name='is_show_result', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        stdin, stdout, stderr = self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='exec_command', name=f"SSH command: {command}", retry_count=retry_count, command=command)

        if is_show_result:
            output = ""
            for line in stdout:
                output = output + line
            if output != "":
                print(output)

            output = ""
            for line in stderr:
                output = output + line
            if output != "":
                self.__error(f"An error occured while executing SSH remote command.")
                print(output)

        return stdin, stdout, stderr

    def put(self, files: str, remote_path: str, recursive: bool=False, preserve_times: bool=False, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='put', name='SCP put', retry_count=retry_count, files=files, 
                                      remote_path=remote_path, recursive=recursive, preserve_times=preserve_times)

    def putfo(self, fl, remote_path: str, mode: str='0644', size: int=None, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")

        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='putfo', name='SCP put byte', retry_count=retry_count, fl=fl, 
                                      remote_path=remote_path, mode=mode, size=size)

    def get(self, remote_path: str, local_path: str='', recursive: bool=False, preserve_times: bool=False, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='get', name='SCP get', retry_count=retry_count, remote_path=remote_path, 
                                      local_path=local_path, recursive=recursive, preserve_times=preserve_times)

# def update(d: dict, u: dict) -> dict:
#     for k, v in u.items():
#         if isinstance(v, Collection.abc.Mapping):
#             d[k] = update(d.get(k, {}), v)
#         else:
#             d[k] = v
#     return d

class DBRunner():
    # SSH
    RETRY_COUNT = 3
    # Auto-bencher
    AUTOBENCHER_NAME = 'auto-bencher'
    AUTOBENCHER_GITHUB = 'https://github.com/elasql/auto-bencher.git'

    # JARS
    JAR_FOLDER_NAME = 'jars'
    SERVER_JAR_NAME = 'server.jar'
    CLIENT_JAR_NAME = 'client.jar'

    WORKSPACE = 'db_runner_workspace'
    DBRUNNER_AUTOBENHER_PATH = os.path.join(WORKSPACE, AUTOBENCHER_NAME)
    TEMP_DIR = 'temp'
    CPU_DIR = 'cpu'
    DISK_DIR = 'disk'
    LATENCY_DIR = 'latency'
    DBRUNNER_TEMP_PATH = os.path.join(WORKSPACE, TEMP_DIR)
    
    # Name of database
    DB_NAME = 'hermes'

    # Name of configs
    BENCHER_CONFIG = 'bencher.toml'
    LOAD_CONFIG = 'load.toml'
    BENCH_CONFIG = 'bench.toml'

    # Directory of base-config 
    CONFIG_DIR = 'config'
    CURRENT_PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
    BASE_CONFIGS_DIR = os.path.join(CURRENT_PYTHON_DIR, CONFIG_DIR)

    # Path of base-config 
    BASE_BENCHER_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, BENCHER_CONFIG)
    BASE_LOAD_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, LOAD_CONFIG)
    BASE_BENCH_CONFIG_PATH = os.path.join(BASE_CONFIGS_DIR, BENCH_CONFIG)

    # Directory of configs on DB-Runner
    DBRUNNER_CONFIG_DIR = os.path.join(WORKSPACE, AUTOBENCHER_NAME)
    DBRUNNER_BENCHER_CONFIG_PATH = os.path.join(DBRUNNER_CONFIG_DIR, BENCHER_CONFIG)
    DBRUNNER_LOAD_CONFIG_PATH = os.path.join(DBRUNNER_CONFIG_DIR, LOAD_CONFIG)
    DBRUNNER_BENCH_CONFIG_PATH = os.path.join(DBRUNNER_CONFIG_DIR, BENCH_CONFIG)

    # Reports
    REPORTS_ON_HOST_DIR = 'reports'
    
    def __init__(self) -> None:
        self.is_config_bencher = False

    def __info(self, msg: str) -> None:
        if msg is not None:
            print(f"[DB Runner] Info: {info(msg=msg)}")

    def __warning(self, msg: str) -> None:
        if msg is not None:
            print(f"[DB Runner] Warning: {warning(msg=msg)}")
        
    def __error(self, msg: str) -> None:
        if msg is not None:
            print(f"[DB Runner] Error: {error(msg=msg)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)
    
    def __load_toml(self, toml_file: str) -> dict:
        """
        Load toml file to dictionary
        """
        return toml.load(toml_file)

    def __dump_toml(self, toml_dict) -> str:
        """
        Dump dictionary to string in toml format
        """
        return io.StringIO(toml.dumps(toml_dict))

    def __ssh_exec_command(self, command: str, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> Tuple:
        res = None
        try:
            self.__info(going_msg)
            res = self.host.exec_command(command=command, retry_count=self.RETRY_COUNT)
            self.__info(finished_msg)
            return res
        except:
            self.__error(error_msg)
            return res

    def __scp_put(self, files: str, remote_path: str, recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        try:
            self.__info(going_msg)
            self.host.put(files=files, remote_path=remote_path, recursive=recursive, retry_count=self.RETRY_COUNT)
            self.__info(finished_msg)
        except:
            self.__error(error_msg)

    def __scp_putfo(self, fl: str, remote_path: str, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        try:
            self.__info(going_msg)
            self.host.putfo(fl=fl, remote_path=remote_path, retry_count=self.RETRY_COUNT)
            self.__info(finished_msg)
        except:
            self.__error(error_msg)
    
    def __scp_get(self, remote_path: str, local_path: str='', recursive: bool=False, going_msg: str=None, finished_msg: str=None, error_msg: str=None) -> None:
        try:
            self.__info(going_msg)
            self.host.get(remote_path=remote_path, local_path=local_path, recursive=recursive, retry_count=self.RETRY_COUNT)
            self.__info(finished_msg)
        except:
            self.__error(error_msg)

    def connect(self, hostname: str, username: str, password: str):
        self.hostname = str(hostname)
        self.username = str(username)
        self.password = str(password)

        self.host = SSH(hostname=self.hostname, username=self.username, password=self.password)
        try:
            self.__info(f"Connecting to remote host.")
            self.host.connect(retry_count=self.RETRY_COUNT)
            self.__info(f"Connected to remote host.")
        except:
            self.__error(f"Failed to remote host.")

    def config_bencher(self, sequencer: str=None, servers: list=None, clients: list=None, 
                       user_name: str=None, remote_work_dir: str=None, 
                       dir_name: str=None, package_path: str=None, base_config: str=None, alts: dict=None):
        """
        Config bencher.toml, if the parameters is None, remain the settings in the base_config.
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
    
    def config_cluster(self, jar_dir: str, server_count: int=None, server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None):
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")

        # Reset cluster settings
        self.auto_bencher_sec = {}

        # Set up diretory of JARs
        self.__type_check(obj=jar_dir, obj_type=str, obj_name='jar_dir', is_allow_none=False)
        self.jar_folder = jar_dir
        self.auto_bencher_sec['jar_dir'] = str(jar_dir)
        self.jar_dir = os.path.join(self.WORKSPACE, self.AUTOBENCHER_NAME, self.JAR_FOLDER_NAME, self.jar_folder)

        if not (server_count is None):
            self.auto_bencher_sec['server_count'] = str(server_count)

        if not (server_client_ratio is None):
            self.auto_bencher_sec['server_client_ratio'] = str(server_client_ratio)
        
        if not (max_server_per_machine is None):
            self.auto_bencher_sec['max_server_per_machine'] = str(max_server_per_machine)
        
        if not (max_client_per_machine is None):
            self.auto_bencher_sec['max_client_per_machine'] = str(max_client_per_machine)

    def upload_bencher_config(self):
        # Upload bencher.toml
        fl = self.__dump_toml(self.bencher_config)

        self.__scp_putfo(fl=fl, remote_path=self.DBRUNNER_BENCHER_CONFIG_PATH, 
                         going_msg=f"Uploading config 'bencher.toml'...",
                         finished_msg=f"Uploaded config 'bencher.toml'",
                         error_msg=f"Failed to upload config 'bencher.toml'")

    def upload_load_config(self):
        # Upload load.toml
        fl = self.__dump_toml(self.load_config)

        self.__scp_putfo(fl=fl, remote_path=self.DBRUNNER_LOAD_CONFIG_PATH,
                         going_msg=f"Uploading config 'load.toml'...",
                         finished_msg=f"Uploaded config 'load.toml'",
                         error_msg=f"Failed to upload config 'load.toml'")

    def upload_bench_config(self):
        # Upload bench.toml
        fl = self.__dump_toml(self.bench_config)
        self.__scp_putfo(fl=fl, remote_path=self.DBRUNNER_BENCH_CONFIG_PATH, 
                         going_msg=f"Uploading config 'bench.toml'...", 
                         finished_msg=f"Uploaded config 'bench.toml'", 
                         error_msg=f"Failed to upload config 'bench.toml'")

    def init(self):
        """
        First use
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")

        # Install autobencher
        self.__ssh_exec_command(f"rm -rf {self.WORKSPACE}; mkdir {self.WORKSPACE}; cd {self.WORKSPACE}; git clone {self.AUTOBENCHER_GITHUB}; cd {self.AUTOBENCHER_NAME}; npm install")

        # Create JAR directory and create TEMP directory for storing reports temporarily
        self.__ssh_exec_command(f"mkdir -p {self.DBRUNNER_TEMP_PATH}; mkdir -p {self.jar_dir}")

        self.upload_bencher_config()

        # Init Auto-bencher
        self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} init', 
                                going_msg=f"Initializing database...", 
                                finished_msg=f"Initialized database", 
                                error_msg=f"Failed to initialize database")

    def upload_jars(self, server_jar: str, client_jar: str):
        try:
            self.__info(f"Uploading JARs...")
            self.__scp_put(files=server_jar, remote_path=self.jar_dir)
            self.__scp_put(files=client_jar, remote_path=self.jar_dir)
            self.__info(f"Uploaded JARs...")
        except:
            self.__info(f"Failed to upload JARs")

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

    def load(self, alts: dict=None, base_config: str=None):
        """
        Load test bed
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
            self.bench_config = update(self.bench_config, alts)
        # Apply cluster settings
        self.load_config = self.__update_cluster_config(self.load_config)

        # Upload load.toml
        self.upload_load_config()

        # Run load test bed
        self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} load -d {self.DB_NAME} -p {self.LOAD_CONFIG}', 
                                going_msg=f"Loading test bed...", 
                                finished_msg=f"Loaded test bed", 
                                error_msg=f"Failed to load test bed")

    def collect_results(self, name: str, cpu: str='transaction-cpu-time-server-', 
                        latency: str='transaction-latency-server-', 
                        diskio: str='transaction-diskio-count-server-', format: str='csv', is_delete_reports: bool=False):
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)

        # For each type of reports
        for file_dir, file_type in zip([self.CPU_DIR, self.LATENCY_DIR, self.DISK_DIR], [cpu, latency, diskio]):
            res_dir = os.path.join(self.WORKSPACE, self.TEMP_DIR, name, file_dir)
            self.__ssh_exec_command(f"mkdir -p {res_dir}")
            # For each machine
            for id, server in enumerate(self.servers):
                file_name = f"{file_type}{id}.{format}"
                # Transfer reports to the remote host
                self.__ssh_exec_command(f"scp db-under@{server}:{file_name} {os.path.join(res_dir, file_name)}", 
                                        going_msg=f"Transfering report '{file_name}'' to remote host...", 
                                        finished_msg=f"Transfered report '{file_name}' to remote host", 
                                        error_msg=f"Failed to transfer report '{file_name}' to remote host")
                # Delete reports on the servers
                if is_delete_reports:
                    self.__ssh_exec_command(f"ssh db-under@{server} 'rm -f {file_name}'", 
                                            going_msg=f"Deleting report '{file_name}' on servers...", 
                                            finished_msg=f"Deleted seport '{file_name}' on servers", 
                                            error_msg=f"Failed to delete report '{file_name}' on servers")

    def pull_reports_to_local(self, name: str, path: str, is_delete_reports: bool=False):
        reports_dir = os.path.join(self.DBRUNNER_TEMP_PATH, name)

        self.__scp_get(remote_path=reports_dir, local_path=path, recursive=True, 
                       going_msg=f"Pulling reports '{name}' to local '{path}'...", 
                       finished_msg=f"Pulled reports '{name}' to local '{path}'", 
                       error_msg=f"Failed to pull reports '{name}' to local '{path}'")

        if is_delete_reports:
            self.__ssh_exec_command(f"ssh db-under@{self.hostname} 'rm -rf {reports_dir}'", 
                                    going_msg=f"Deleting reports '{reports_dir}' on host...", 
                                    finished_msg=f"Deleted reports '{reports_dir}' on host", 
                                    error_msg=f"Failed to delete reports '{reports_dir}' on host")

    def bench(self, reports_path: str, alts: dict=None, base_config: str=None, is_delete_reports: bool=True):
        """
        Run Benchmark
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

        # Run benchmark
        self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} benchmark -d {self.DB_NAME} -p {self.BENCH_CONFIG}', 
                                going_msg=f"Benchmarking...", 
                                finished_msg=f"Benchmarked", 
                                error_msg=f"Failed to benchmark")

        # Collect reports
        self.collect_results(name=self.REPORTS_ON_HOST_DIR, is_delete_reports=is_delete_reports)
        self.pull_reports_to_local(name=self.REPORTS_ON_HOST_DIR, path=reports_path, is_delete_reports=is_delete_reports)

    def execute(self, command: str):
        self.__ssh_exec_command(f"cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} exec --command {command}", 
                                going_msg=f"Executing command {command}...", 
                                finished_msg=f"Executed command {command}", 
                                error_msg=f"Failed to execute command {command}")

    def kill_java(self):
        self.execute(command="pkill -f java")

def run(fn):
    fn(files='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', remote_path='/home/db-under/db_runner_workspace/auto-bencher/jars/latest/')

def testing():
    ip = "140.114.85.15"
    username = "db-under"
    password = "db-under"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=ip, username=username, password=password)
    scpClient = SCPClient(client.get_transport(), progress4=progress4)
    fn = scpClient.put
    for i in range(10):
        try:
            # scpClient.put(files='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', remote_path='/home/db-under/db_runner_workspace/auto-bencher/jars/latest/')
            run(fn)
            print(f"Testing Success: {info('Upload Success')}")
        except:
            print(f"Testing Error: {error('Upload Failed')}")
            client.close()
            client.connect(hostname=ip, username=username, password=password)
            scpClient = SCPClient(client.get_transport(), progress4=progress4)
            print(f"Testing Warning: {warning('Reconnected')}")

if __name__ == '__main__':
    ip = "140.114.85.15"
    username = "db-under"
    password = "db-under"

    # testing()

    # client = SSH(hostname=ip, username=username, password=password)
    # client.connect()
    # stdin, stdout, stderr = client.exec_command(command='cd /home/db-under/sychou/autobench; ls -l')
    # client.get(remote_path='/home/db-under/sychou/autobench/share.sh')
    # client.put(files='/home/weidagogo/sychou/db/ouModels.ipynb', remote_path='/home/db-under/sychou/autobench/ouModels.ipynb')

    dr = DBRunner()
    dr.connect(hostname=ip, username=username, password=password)
    dr.config_bencher(sequencer="192.168.1.32", 
                      servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                      clients=["192.168.1.9", "192.168.1.8"], package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    dr.config_cluster(server_count=4, jar_dir='latest')
    dr.init()
    dr.upload_jars(server_jar='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', 
                    client_jar='/home/weidagogo/sychou/db/DBRunner/jars/client.jar')
    dr.load()
    dr.bench(reports_path='/home/weidagogo/sychou/db/results', is_delete_reports=False)
    dr.kill_java()