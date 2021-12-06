import logging
import os
import io
from typing import List, Tuple
import unittest
import traceback
from time import sleep

import toml

from scalablerunner.util import info, warning, error, type_check, UtilLogger
from scalablerunner.ssh import SSH
from scalablerunner.dbrunner import DBRunner
from scalablerunner.taskrunner import TaskRunner
from scalablerunner.adapter import DBRunnerAdapter

# logging.basicConfig(filename='temp/test.log',
#                     filemode='a',
#                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.DEBUG)

# Global variables
temp_dir = 'temp'
host_infos_file = 'host_infos.toml'
test_log = 'test.log'

def get_temp_dir() -> str:
    # Create 'temp' directory
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir

def get_host_infos() -> Tuple[str, str, str, str]:
    config = toml.load(host_infos_file)

    hostname = str(config['hostname'])
    username = str(config['username'])
    password = str(config['password'])
    port = int(config['port'])
    print(f"[Test] Hostname: {info(hostname)} | Username: {(info(username))} | Password: {info(password)} | Port: {info(str(port))}")

    return hostname, username, password, port

def get_server_infos() -> Tuple[str, List[str], List[str], str]:
    config = toml.load(host_infos_file)

    sequencer = str(config['sequencer'])
    servers = config['servers']
    clients = config['clients']
    package_path = config['package_path']
    print(f"[Test] Sequencer: {info(sequencer)} | Servers: {(info(', '.join(servers)))} | Clients: {info(', '.join(clients))} | Package Path: {info(package_path)}")

    return sequencer, servers, clients, package_path

def get_rpyc_server_infos() -> Tuple[str, int]:
    config = toml.load(host_infos_file)

    rpyc_ip = str(config['rpyc_ip'])
    rpyc_port = int(config['rpyc_port'])
    print(f"[Test] RPYC IP: {info(rpyc_ip)} | RPYC Port: {info(str(rpyc_port))}")

    return rpyc_ip, rpyc_port

def get_log_path() -> str:
    return os.path.join(get_temp_dir(), test_log)

class TestUtil(unittest.TestCase):
    def test_info(self):
        msg = info(f"Unit test on info()")
        assert isinstance(msg, str)

    def test_warning(self):
        msg = info(f"Unit test on warning()")
        assert isinstance(msg, str)

    def test_error(self):
        msg = info(f"Unit test on error()")
        assert isinstance(msg, str)
    
    def test_Logger(self):
        UtilLogger

class TestSSH(unittest.TestCase):
    TEST_LOOP_COUNT = 10
    TIMEOUT = 20
    RETRY_COUNT = 3
    CMD_RETRY_COUNT = 2

    # Default values
    DEFAULT_IS_RAISE_ERR = True
    DEFAULT_RETRY_COUNT = 3
    DEFAULT_TIMEOUT = 20

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

        # Get host infos
        self.HOSTNAME, self.USERNAME, self.PASSWORD, self.PORT = get_host_infos()
        self.client = SSH(hostname=self.HOSTNAME, username=self.USERNAME, password=self.PASSWORD, port=self.PORT)
        self.client.output_log(file_name=get_log_path())
        # self.client.set_default_is_raise_err(default_is_raise_err=self.DEFAULT_IS_RAISE_ERR)

    def __info(self, *args, **kwargs) -> None:
        print(f"[Test SSH] Info: {info(*args, **kwargs)}")

    def __warning(self, *args, **kwargs) -> None:
        print(f"[Test SSH] Warning: {warning(*args, **kwargs)}")
        
    def __error(self, *args, **kwargs) -> None:
        print(f"[Test SSH] Error: {error(*args, **kwargs)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)

    def setUp(self):
        self.client.set_default_is_raise_err(default_is_raise_err=self.DEFAULT_IS_RAISE_ERR)
        self.client.set_default_retry_count(default_retry_count=self.DEFAULT_RETRY_COUNT)
        self.client.set_default_timeout(default_timeout=self.DEFAULT_TIMEOUT)
        self.client.connect(retry_count=self.RETRY_COUNT)

    def tearDown(self):
        self.client.close()

    def test_connect(self):
        for i in range(self.TEST_LOOP_COUNT):
            self.client.reconnect()
        
    def test_set_default_is_raise_err(self):
        is_passed = False
        # Turn off is_raise_err, shouldn't raise error
        self.client.set_default_is_raise_err(default_is_raise_err=False)
        self.client.exec_command(command='rm new_dir_test', retry_count=self.RETRY_COUNT, cmd_retry_count=True)

        # Turn on is_raise_err, should raise error
        self.client.set_default_is_raise_err(default_is_raise_err=True)
        try:
            self.client.exec_command(command='rm new_dir_test', retry_count=self.RETRY_COUNT, cmd_retry_count=True)
            is_passed = False
        except:
            is_passed = True

        if not is_passed:
            self.__error(f"Failed to pass test_set_default_is_raise_err()")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_set_default_is_raise_err()")

    def test_exec_command(self):
        for i in range(self.TEST_LOOP_COUNT):
            stdin, stdout, stderr, is_successed = self.client.exec_command(command='ls -la; mkdir new_dir_test; rm -rf new_dir_test', 
                                                                           bufsize=-1, get_pty=False, environment=None, 
                                                                           retry_count=self.RETRY_COUNT, cmd_retry_count=self.CMD_RETRY_COUNT)
        assert is_successed is True
        stdin, stdout, stderr, is_successed = self.client.exec_command(command='rm new_dir_test1', retry_count=self.RETRY_COUNT, 
                                                                       cmd_retry_count=self.CMD_RETRY_COUNT, is_raise_err=False)
        assert is_successed is False

        is_passed = True
        # Test on turning on both is_raise_err and is_show_result 
        try:
            self.client.exec_command(command='rm new_dir_test2', retry_count=self.RETRY_COUNT, cmd_retry_count=1, is_raise_err=True)
            is_passed = False
        except:
            traceback.print_exc()
        # Test on turning on is_raise_err and turning off is_show_result 
        try:
            self.client.exec_command(command='rm new_dir_test3', retry_count=self.RETRY_COUNT, cmd_retry_count=1, is_show_result=False, is_raise_err=True)
            is_passed = False
        except:
            traceback.print_exc()

        # Test on timeout
        try:
            self.client.exec_command(command='ls -lh; sleep 50', timeout=1, retry_count=self.RETRY_COUNT, cmd_retry_count=1, is_show_result=True, is_raise_err=True)
            is_passed = False
        except:
            traceback.print_exc()
            is_passed = True

        # Test on default_timeout
        try:
            self.client.set_default_timeout(default_timeout=1)
            self.client.reconnect()
            self.client.exec_command(command='ls -lh; sleep 60', cmd_retry_count=1, is_show_result=True, is_raise_err=False)
        except:
            traceback.print_exc()
            is_passed = False

        # Check whether passed the test
        if not is_passed:
            self.__error(f"Failed to pass test_exec_command()")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_exec_command()")

    def test_put(self):
        for i in range(self.TEST_LOOP_COUNT):
            # server.jar
            self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3)

            # bench.toml
            # self.client.put(files='data/config/bench.toml', remote_path='./', recursive=False, preserve_times=False, retry_count=3)

            # jars.zip
            # self.client.put(files='data/jars/jars.zip', remote_path='./', recursive=False, preserve_times=False, retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            # server.jar
            # self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            # self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

            # bench.toml
            self.client.put(files='data/config/bench.toml', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            self.client.put(files='data/config/bench.toml', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

            # jars.zip
            # self.client.put(files='data/jars/jars.zip', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            # self.client.put(files='data/jars/jars.zip', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

    def test_putfo(self):
        # Test file
        test_file = io.StringIO('HI Test\n' * 100000)
        for i in range(self.TEST_LOOP_COUNT):
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3, is_raise_err=False)
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3, is_raise_err=True)

    def test_large_put(self):
        for i in range(self.TEST_LOOP_COUNT):
            # server.jar
            self.client.large_put(files='data/jars/server.jar', remote_path='./server.jar', recursive=False, retry_count=3)

            # bench.toml
            # self.client.put(files='data/config/bench.toml', remote_path='./bench.toml', recursive=False, retry_count=3)

            # jars.zip
            # self.client.put(files='data/jars/jars.zip', remote_path='./jars.zip', recursive=False, retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            # server.jar
            # self.client.put(files='data/jars/server.jar', remote_path='./server.jar', recursive=False, retry_count=3, is_raise_err=False)
            # self.client.put(files='data/jars/server.jar', remote_path='./server.jar', recursive=False, retry_count=3, is_raise_err=True)

            # bench.toml
            self.client.put(files='data/config/bench.toml', remote_path='./bench.toml', recursive=False, retry_count=3, is_raise_err=False)
            self.client.put(files='data/config/bench.toml', remote_path='./bench.toml', recursive=False, retry_count=3, is_raise_err=True)

            # jars.zip
            # self.client.put(files='data/jars/jars.zip', remote_path='./jars.zip', recursive=False, retry_count=3, is_raise_err=False)
            # self.client.put(files='data/jars/jars.zip', remote_path='./jars.zip', recursive=False, retry_count=3, is_raise_err=True)

    def test_get(self):
        for i in range(self.TEST_LOOP_COUNT):
            self.client.get(local_path='/opt/shared-disk2/sychou/', remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3)

            self.client.get(local_path='/opt/shared-disk2/sychou', remote_path='./db_runner_workspace_cw/auto-bencher/src', recursive=True, mode=SSH.STABLE)
            self.client.get(local_path='/opt/shared-disk2/sychou', remote_path='./db_runner_workspace_cw/auto-bencher/src', recursive=True, mode=SSH.SFTP)

            # self.client.get(local_path='/opt/shared-disk2/sychou/jars.zip', remote_path='./jars.zip', recursive=False, mode=SSH.STABLE)
            # self.client.get(local_path='/opt/shared-disk2/sychou/transaction-features.csv', remote_path='./db_runner_workspace_test/temp/reports/transaction-features.csv', recursive=False, mode=SSH.STABLE)

        for i in range(self.TEST_LOOP_COUNT // 2):
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

def test_task(epoch :int, decay: str, machine: int, gpu: int, dataset_size: int):
    import os
    # import jax.numpy as np
    import numpy as np
    from time import sleep
    os.environ["CUDA_VISIBLE_DEVICES"] = f'{gpu}'
    print(f"Epoch: {epoch}, Decay: {decay}, Dataset Size: {dataset_size}, Machine: {machine}, GPU: {gpu}")
    sleep(5)

def test_err(flag: str, gpu: int):
    raise BaseException(f"Something wrong with the flag {flag} on gpu {gpu}")

class TestTaskRunner(unittest.TestCase):
    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

    def test_run(self):
        rpyc_ip, rpyc_port = get_rpyc_server_infos()
        config = {
            'section-1': { # Each section would be executed sequentially.
                'group-1': { # The groups under the same section would be executed concurrently
                    'Call': test_task, # Call can be either a function call or a command in string
                    'Param': { # The TaskRunner would list all kinds of combination of the parameters and execute them once
                        'decay': ['exp', 'anneal', 'static'],
                        'epoch': [100, 1000, 10000],
                        'dataset_size': [1000, 2000, 3000]
                    },
                    'Async': { # The task under the same group would be schedule to the resources by TaskRunner during runtime.
                        'machine': [0, 1],
                        'gpu': [0, 1]
                    },
                },    
                'group-2':{ # 'group-2' can be seem as another resource group that handle different task from 'group-1' during 'section-1'
                    'Call': 'ls',
                    'Param': {
                        '': ['-l', '-a', '-la']  
                    },
                    'Async': {
                        '': []
                    }   
                },
                'group-3':{ # 'group-2' can be seem as another resource group that handle different task from 'group-1' during 'section-1'
                    'Call': 'ls',
                    'Param': {
                        '': ['-l', '-a', '-la']  
                    }  
                },
                'group-error': {
                    'Call': test_err,
                    'Param': {
                        'flag': ['-a', '-l', '-la']
                    },
                    'Async': {
                        'gpu': [0, 1, 2]
                    } 
                },
                'group-bug': {
                    'Call': [],
                    'Param': {
                        'flag': ['-a', '-l', '-la']
                    },
                    'Async': {
                        'gpu': [0, 1, 2]
                    } 
                }
            },
            'section-error': {
                'group-1': [],
                'group-2': []
            },
            'section-2': {
                'group-1': {
                    'Call': 'ls',
                    'Param': {
                        '': ['-a']
                    }
                },
                'group-wrong-cmd': {
                    'Call': 'lsa',
                    'Param': {
                        '': ['-a', '-l', '-la']
                    },
                    'Async': {
                        '': [0, 1, 2]
                    } 
                },
            },
            'section-remote': {
                'group-remote 18180': {
                    'Call': test_task,
                    'Param': {
                        'decay': ['exp', 'anneal', 'static'],
                        'epoch': [100, 1000, 10000],
                        'dataset_size': [1000, 2000, 3000]
                    },
                    'Async': {
                        'machine': [0, 1],
                        'gpu': [0, 1]
                    },
                    'Remote': rpyc_ip, # RPYC server IP
                    'Port': rpyc_port # RPYC server port
                },
                'group-remote default': {
                    'Call': test_task,
                    'Param': {
                        'decay': ['exp', 'anneal', 'static'],
                        'epoch': [100, 1000, 10000],
                        'dataset_size': [1000, 2000, 3000]
                    },
                    'Async': {
                        'machine': [0, 1],
                        'gpu': [0, 1]
                    },
                    'Remote': rpyc_ip, # RPYC server IP
                },
            }
        }
        
        tr = TaskRunner(config=config, delay=0.5)
        tr.output_log(file_name=get_log_path())
        tr.run()

def config_db_runner(db_runner: DBRunner) -> DBRunner:
    sequencer, servers, clients, package_path = get_server_infos()
    db_runner.config_bencher(sequencer=sequencer, 
                             servers=servers, 
                             clients=clients, 
                             package_path=package_path)
    db_runner.config_cluster(server_count=4, jar_dir='latest')
    return db_runner

def get_workspace_name():
    return 'db_runner_workspace_test'

class TestDBRunner(unittest.TestCase):
    # SSH default value
    SSH_DEFAULT_RETRY_COUT = 3
    SSH_DEFAULT_IS_RAISE_ERR = True

    # Configurations
    VANILLABENCH_NAME = "vanillabench"
    ELASQL_NAME = "elasql"
    ELASQLBENCH_NAME = "elasqlbench"
    BENCHMARK_INTERVAL_NAME = "org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL"
    INIT_RECORD_PER_PART_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART"
    RW_TX_RATE_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE"
    ENABLE_COLLECTING_DATA_NAME = "org.elasql.perf.tpart.TPartPerformanceManager.ENABLE_COLLECTING_DATA"

    BENCHMARK_INTERVAL = "120000"
    INIT_RECORD_PER_PART = "100000"
    ENABLE_COLLECTING_DATA = "true"
    RW_TX_RATE = "1"

    ARGS_LOAD = {
                    ELASQLBENCH_NAME: {
                        INIT_RECORD_PER_PART_NAME: INIT_RECORD_PER_PART
                    }
                }

    ARGS_BENCH = {
                    VANILLABENCH_NAME: {
                        BENCHMARK_INTERVAL_NAME: BENCHMARK_INTERVAL
                    },
                    ELASQL_NAME: {
                        ENABLE_COLLECTING_DATA_NAME: ENABLE_COLLECTING_DATA
                    },
                    ELASQLBENCH_NAME: {
                        INIT_RECORD_PER_PART_NAME: INIT_RECORD_PER_PART,
                        RW_TX_RATE_NAME: RW_TX_RATE
                    }
                }

    def __info(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Info: {info(*args, **kwargs)}")

    def __warning(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Warning: {warning(*args, **kwargs)}")
        
    def __error(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Error: {error(*args, **kwargs)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

        # Get host infos
        self.HOSTNAME, self.USERNAME, self.PASSWORD, self.PORT = get_host_infos()

        self.dr = DBRunner(workspace=get_workspace_name())
        self.dr.output_log(file_name=get_log_path())
    
    @classmethod
    def setUpClass(cls):
        # Get host infos
        HOSTNAME, USERNAME, PASSWORD, PORT = get_host_infos()

        dr = DBRunner(workspace=get_workspace_name())
        dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
        dr = config_db_runner(dr)
        dr.init()

    def setUp(self):
        self.dr.set_default_is_raise_err(default_is_raise_err=self.SSH_DEFAULT_IS_RAISE_ERR)
        self.dr.set_default_retry_count(default_retry_count=self.SSH_DEFAULT_RETRY_COUT)
        self.dr.connect(hostname=self.HOSTNAME, username=self.USERNAME, password=self.USERNAME)
        self.dr = config_db_runner(self.dr)

    def tearDown(self):
        self.dr.close()

    def test_connect(self):
        try:
            for i in range(10):
                self.dr.close()
            self.dr.connect(hostname=self.HOSTNAME, username=self.USERNAME, password=self.USERNAME)
        except:
            self.__error(f"Fail to pass test_connect")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_connect()")

    def test_upload_jars(self):
        try:
            for i in range(10):
                self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
                self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar', use_stable=True)
        except:
            self.__error(f"Fail to pass test_upload_jars")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_upload_jars()")

    def test_load(self):
        try:
            self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
            for i in range(3):
                self.dr.load(alts=self.ARGS_LOAD, is_kill_java=True)

            # Check configuration load.toml
            assert self.dr.get_load_config(format=DBRunner.DICT)[self.ELASQLBENCH_NAME][self.INIT_RECORD_PER_PART_NAME] == self.INIT_RECORD_PER_PART
        except:
            self.__error(f"Fail to pass test_load()")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_load()")

    # def test_pull_reports_to_local(self):
    #     try:
    #         self.dr.pull_reports_to_local(name="db_runner_workspace_test/auto-bencher/parameters", path=get_temp_dir(), is_delete_reports=False)
    #     except:
    #         self.__error(f"Fail to pass test_pull_reports_to_local()")
    #         traceback.print_exc()
    #         raise BaseException(f"Failed to pass test_pull_reports_to_local()")

    # def test_move_stats(self):
    #     try:
    #         self.dr.move_stats(name=DBRunner.REPORTS_ON_HOST_DIR)
    #     except:
    #         self.__error(f"Fail to pass test_move_stats()")
    #         traceback.print_exc()
    #         raise BaseException(f"Failed to pass test_move_stats()")

    def test_bench(self):
        try:
            self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
            self.dr.load(alts=self.ARGS_LOAD, is_kill_java=True)
            for i in range(1):
                # self.dr.bench(reports_path=get_temp_dir(), alts=self.ARGS_BENCH, is_pull_reports=True, is_delete_reports=True, 
                #               is_kill_java=True, use_stable=False)
                self.dr.bench(reports_path=get_temp_dir(), alts=self.ARGS_BENCH, base_config='data/config/bench.toml', is_pull_reports=True, is_delete_reports=True, 
                              is_kill_java=True, use_stable=True)

            # self.dr.bench(reports_path=get_temp_dir(), alts=self.ARGS_BENCH, is_pull_reports=False, is_delete_reports=False, 
            #               is_kill_java=True)

            # Check configuration bench.toml
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQLBENCH_NAME][self.INIT_RECORD_PER_PART_NAME] == self.INIT_RECORD_PER_PART
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQLBENCH_NAME][self.RW_TX_RATE_NAME] == self.RW_TX_RATE
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQL_NAME][self.ENABLE_COLLECTING_DATA_NAME] == self.ENABLE_COLLECTING_DATA

        except:
            self.__error(f"Fail to pass test_bench()")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_bench()")

def config_db_runner_adapter(db_runner_adapter: DBRunnerAdapter) -> DBRunnerAdapter:
    sequencer, servers, clients, package_path = get_server_infos()
    db_runner_adapter.config(server_count=4, sequencer=sequencer, servers=servers, clients=clients, package_path=package_path, jar_dir='latest')
    return db_runner_adapter

def name_fn(reports_path: str, alts: dict):
    # rw = alts['vanillabench']['org.vanilladb.bench.BenchmarkerParameters.NUM_RTES']
    rw = alts['elasqlbench']['org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE']
    rw_dir = os.path.join(reports_path, f'rw-{rw}')
    if not os.path.isdir(rw_dir):
        os.makedirs(rw_dir) 
    return rw_dir

# def name_fn(reports_path: str, alts: dict):
#     rw = alts['vanillabench']['org.vanilladb.bench.BenchmarkerParameters.NUM_RTES']
#     rw_dir = os.path.join(reports_path, f'rte-{rw}')
#     if not os.path.isdir(rw_dir):
#         os.makedirs(rw_dir) 
#     return rw_dir

class TestDBRunnerAdapater(unittest.TestCase):
    # SSH default value
    SSH_DEFAULT_RETRY_COUT = 3
    SSH_DEFAULT_IS_RAISE_ERR = True

    SERVER_NUM = 4
    BASE_PATH = 'temp/testing'

    # Jars
    SERVER_JAR = 'data/jars/server.jar'
    CLIENT_JAR = 'data/jars/client.jar'

    # Configurations
    LOAD_CONFIG = 'data/config/load.toml'
    BENCH_CONFIG = 'data/config/bench.toml'

    VANILLABENCH_NAME = "vanillabench"
    ELASQL_NAME = "elasql"
    ELASQLBENCH_NAME = "elasqlbench"
    BENCHMARK_INTERVAL_NAME = "org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL"
    INIT_RECORD_PER_PART_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART"
    RW_TX_RATE_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE"
    ENABLE_COLLECTING_DATA_NAME = "org.elasql.perf.tpart.TPartPerformanceManager.ENABLE_COLLECTING_DATA"
    NUM_RTES_NAME = "org.vanilladb.bench.BenchmarkerParameters.NUM_RTES"

    BENCHMARK_INTERVAL = "120000"
    INIT_RECORD_PER_PART = "100000"
    ENABLE_COLLECTING_DATA = "true"
    RW_TX_RATE = "1"

    ARGS_LOAD = {
                    ELASQLBENCH_NAME: {
                        INIT_RECORD_PER_PART_NAME: INIT_RECORD_PER_PART
                    }
                }

    ARGS_BENCH = {
                    VANILLABENCH_NAME: {
                        BENCHMARK_INTERVAL_NAME: BENCHMARK_INTERVAL
                    },
                    ELASQL_NAME: {
                        ENABLE_COLLECTING_DATA_NAME: ENABLE_COLLECTING_DATA
                    },
                    ELASQLBENCH_NAME: {
                        INIT_RECORD_PER_PART_NAME: INIT_RECORD_PER_PART,
                        RW_TX_RATE_NAME: RW_TX_RATE
                    }
                }

    # PARAMS = [[VANILLABENCH_NAME, NUM_RTES_NAME, "200"],
    #           [VANILLABENCH_NAME, NUM_RTES_NAME, "100"]]
    PARAMS = [[ELASQLBENCH_NAME, RW_TX_RATE_NAME, "1"],
              [ELASQLBENCH_NAME, RW_TX_RATE_NAME, "0"]]

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

        # Get host infos
        self.hostname, self.username, self.password, self.port = get_host_infos()

    def __info(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Info: {info(*args, **kwargs)}")

    def __warning(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Warning: {warning(*args, **kwargs)}")
        
    def __error(self, *args, **kwargs) -> None:
        print(f"[Test DB Runner] Error: {error(*args, **kwargs)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)
    
    @classmethod
    def setUpClass(cls):
        dra = DBRunnerAdapter(reports_path=TestDBRunnerAdapater.BASE_PATH, workspace=get_workspace_name())
        dra.output_log(file_name=get_log_path())

        config_db_runner_adapter(db_runner_adapter=dra)
        
        hostname, username, password, port = get_host_infos()
        dra.connect(hostname=hostname, username=username, password=password, port=port)
        dra.init_autobencher(server_jar=TestDBRunnerAdapater.SERVER_JAR, client_jar=TestDBRunnerAdapater.CLIENT_JAR)
        dra.close()

    def setUp(self):
        self.dra = DBRunnerAdapter(reports_path=self.BASE_PATH, workspace=get_workspace_name())
        self.dra.output_log(file_name=get_log_path())
        self.dra.set_default_is_raise_err(default_is_raise_err=self.SSH_DEFAULT_IS_RAISE_ERR)
        self.dra.set_default_retry_count(default_retry_count=self.SSH_DEFAULT_RETRY_COUT)

        sequencer, servers, clients, package_path = get_server_infos()
        self.dra.config(server_count=self.SERVER_NUM, sequencer=sequencer, servers=servers, clients=clients, package_path=package_path)
        
        hostname, username, password, port = get_host_infos()
        self.dra.connect(hostname=hostname, username=username, password=password, port=port)

    def tearDown(self):
        self.dra.close()

    def test_config(self):
        try:
            sequencer, servers, clients, package_path = get_server_infos()
            self.dra.config(server_count=self.SERVER_NUM, sequencer=sequencer, servers=servers, clients=clients, package_path=package_path)
        except:
            self.__error(f"Fail to pass test_config")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_config()")

    def test_init_autobencher(self):
        try:
            self.dra.init_autobencher(server_jar=self.SERVER_JAR, client_jar=self.CLIENT_JAR)
        except:
            self.__error(f"Fail to pass test_init_autobencher")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_init_autobencher()")

    def test_load_test_bed(self):
        try:
            self.dra.load_test_bed(alts=self.ARGS_LOAD, rte_param=self.RTE_PARAM[0])
        except:
            self.__error(f"Fail to pass test_load_test_bed")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_load_test_bed()")

    def test_init_autobencher_load_test_bed(self):
        try:
            self.dra.init_autobencher_load_test_bed(server_jar=self.SERVER_JAR, client_jar=self.CLIENT_JAR, alts=self.ARGS_LOAD, rte_param=self.RTE_PARAM[0])
        except:
            self.__error(f"Fail to pass test_init_autobencher_load_test_bed")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_init_autobencher_load_test_bed()")

    def test_bench(self):
        config_test = {
            f'Section Initialize | {TestDBRunnerAdapater.SERVER_NUM} server | TestDBRunnerAdapater': {
                'Group Test': {
                    'Call': self.dra.init_autobencher_load_test_bed,
                    'Param': {
                        'server_jar': [self.SERVER_JAR], 
                        'client_jar': [self.CLIENT_JAR],
                        'alts': [self.ARGS_LOAD],
                        'base_config': [self.LOAD_CONFIG],
                        'custom_param1': self.PARAMS,
                    }
                },
            },
            f'Section Benchmark | {TestDBRunnerAdapater.SERVER_NUM} server | TestDBRunnerAdapater': {
                'Group Test': {
                    'Call': self.dra.benchmark,
                    'Param': {
                        'name_fn': [name_fn],
                        'alts': [self.ARGS_BENCH],
                        'base_config': [self.BENCH_CONFIG],
                        'custom_param1': self.PARAMS,
                    }
                },
            }
        }
        try:
            tr = TaskRunner(config=config_test)
            tr.run()
        except:
            self.__error(f"Fail to pass test_bench")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_bench()")

def suite():
    suite = unittest.TestSuite()

    # Test Util
    suite.addTest(TestUtil('test_info'))
    suite.addTest(TestUtil('test_warning'))
    suite.addTest(TestUtil('test_error'))
    suite.addTest(TestUtil('test_Logger'))
    
    # Test SSH
    suite.addTest(TestSSH('test_connect'))
    suite.addTest(TestSSH('test_set_default_is_raise_err'))
    suite.addTest(TestSSH('test_exec_command'))
    suite.addTest(TestSSH('test_put'))
    suite.addTest(TestSSH('test_putfo'))
    suite.addTest(TestSSH('test_large_put'))
    suite.addTest(TestSSH('test_get'))

    # Test TaskRunner
    suite.addTest(TestTaskRunner('test_run'))

    # Test DBRunner
    suite.addTest(TestDBRunner('test_connect'))
    suite.addTest(TestDBRunner('test_upload_jars'))
    suite.addTest(TestDBRunner('test_load'))
    suite.addTest(TestDBRunner('test_bench'))

    # Test DBRunnerAdapater
    suite.addTest(TestDBRunnerAdapater('test_config'))
    suite.addTest(TestDBRunnerAdapater('test_init_autobencher'))
    suite.addTest(TestDBRunnerAdapater('test_load_test_bed'))
    suite.addTest(TestDBRunnerAdapater('test_init_autobencher_load_test_bed'))
    suite.addTest(TestDBRunnerAdapater('test_bench'))
    return suite

if __name__ == '__main__':
    # python -m unittest tests.test_all.TestDBRunner.test_upload_jars
    test_runner = unittest.TextTestRunner(failfast=True)
    test_runner.run(suite())