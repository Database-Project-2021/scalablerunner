# from enum import Enum, auto
import io
import os
import traceback
from typing import Tuple

import toml

from runner.util import BaseClass, UtilLogger, info, warning, error, type_check, update
from runner.ssh import SSH

class DBRunner(BaseClass):
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
    BASE_CONFIGS_DIR = os.path.join('data', CONFIG_DIR)

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

        # Logger
        self.logger = self._set_UtilLogger(module='Runner', submodule='DBRunner', verbose=UtilLogger.INFO)

    def __info(self, *args, **kwargs) -> None:
        # if msg is not None:
        #     print(f"[DB Runner] Info: {info(msg=msg)}")
        super()._info(*args, **kwargs)

    def __warning(self, *args, **kwargs) -> None:
        # if msg is not None:
        #     print(f"[DB Runner] Warning: {warning(msg=msg)}")
        super()._warning(*args, **kwargs)
        
    def __error(self, *args, **kwargs) -> None:
        # if msg is not None:
        #     print(f"[DB Runner] Error: {error(msg=msg)}")
        super()._error(*args, **kwargs)

    def __type_check(self, *args, **kwargs) -> None:
        # type_check(*args, **kwargs)
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

    def connect(self, hostname: str, username: str=None, password: str=None) -> None:
        """
        Connect to the host that ``Auto-Bencher`` locates

        :param str hostname: The host IP/URL
        :param str username: The username logins to
        :param str password: The password logins to
        """
        self.hostname = str(hostname)
        self.username = str(username)
        self.password = str(password)

        self.host = SSH(hostname=self.hostname, username=self.username, password=self.password)
        try:
            self.__info(f"Connecting to remote host")
            self.host.connect(retry_count=self.RETRY_COUNT)
            self.__info(f"Connected to remote host")
        except:
            self.__error(f"Failed to connect remote host")
        
    def close(self) -> None:
        """
        Close the connection to ``Auto-Bencher`` host.
        """
        try:
            self.__info(f"Closing the remote host")
            self.host.close()
            self.__info(f"Closed to remote host")
        except:
            self.__error(f"Failed to close remote host")

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
        """
        
        """
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
        self.is_config_cluster = True

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

    def init(self) -> Tuple:
        """
        Initialize ElaSQL and Auto-Bencher according to the settings that is set up with the method ``DBRunner.config_bencher``
        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: list, list, list
        """
        if not self.is_config_bencher:
            raise BaseException(f"Please call method config_bencher() to config bencher.toml at first.")

        # Install autobencher
        self.__ssh_exec_command(f"rm -rf {self.WORKSPACE}; mkdir {self.WORKSPACE}; cd {self.WORKSPACE}; git clone {self.AUTOBENCHER_GITHUB}; cd {self.AUTOBENCHER_NAME}; npm install")

        # Create JAR directory and create TEMP directory for storing reports temporarily
        self.__ssh_exec_command(f"mkdir -p {self.DBRUNNER_TEMP_PATH}; mkdir -p {self.jar_dir}")

        self.upload_bencher_config()

        # Init Auto-bencher
        stdin, stdout, stderr = self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} init', 
                                                        going_msg=f"Initializing database...", 
                                                        finished_msg=f"Initialized database", 
                                                        error_msg=f"Failed to initialize database")
        
        return stdin, stdout, stderr

    def upload_jars(self, server_jar: str, client_jar: str):
        if not self.is_config_cluster:
            raise BaseException(f"Please call method config_cluster() at first.")

        try:
            self.__info(f"Uploading JARs...")
            self.__scp_put(files=server_jar, remote_path=self.jar_dir)
            self.__scp_put(files=client_jar, remote_path=self.jar_dir)
            self.__info(f"Uploaded JARs...")
        except:
            # traceback.print_exc()
            self.__error(f"Failed to upload JARs")

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
        stdin, stdout, stderr = self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} load -d {self.DB_NAME} -p {self.LOAD_CONFIG}', 
                                                        going_msg=f"Loading test bed...", 
                                                        finished_msg=f"Loaded test bed", 
                                                        error_msg=f"Failed to load test bed")
        return stdin, stdout, stderr

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
        stdin, stdout, stderr = self.__ssh_exec_command(f'cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} benchmark -d {self.DB_NAME} -p {self.BENCH_CONFIG}', 
                                                        going_msg=f"Benchmarking...", 
                                                        finished_msg=f"Benchmarked", 
                                                        error_msg=f"Failed to benchmark")

        # Collect reports
        self.collect_results(name=self.REPORTS_ON_HOST_DIR, is_delete_reports=is_delete_reports)
        self.pull_reports_to_local(name=self.REPORTS_ON_HOST_DIR, path=reports_path, is_delete_reports=is_delete_reports)

        return stdin, stdout, stderr

    def execute(self, command: str):
        stdin, stdout, stderr = self.__ssh_exec_command(f"cd {self.DBRUNNER_AUTOBENHER_PATH}; node src/main.js -c {self.BENCHER_CONFIG} exec --command {command}", 
                                                        going_msg=f"Executing command {command}...", 
                                                        finished_msg=f"Executed command {command}", 
                                                        error_msg=f"Failed to execute command {command}")
        return stdin, stdout, stderr

    def kill_java(self):
        stdin, stdout, stderr = self.execute(command="pkill -f java")
        return stdin, stdout, stderr