import os
import io
import unittest
import traceback
from time import sleep

import toml

from scalablerunner.util import info, warning, error, type_check, UtilLogger
from scalablerunner.ssh import SSH
from scalablerunner.dbrunner import DBRunner
from scalablerunner.taskrunner import TaskRunner

# Global variables
host_infos_file = 'host_infos.toml'

def get_temp_dir():
    # Create 'temp' directory
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir

def get_host_infos():
    config = toml.load(host_infos_file)

    hostname = str(config['hostname'])
    username = str(config['username'])
    password = str(config['password'])
    port = int(config['port'])
    print(f"[Test] Hostname: {info(hostname)} | Username: {(info(username))} | Password: {info(password)} | Port: {info(str(port))}")

    return hostname, username, password, port

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
    TIME_OUT = 20
    RETRY_COUNT = 3
    CMD_RETRY_COUNT = 2

    # Default values
    DEFAULT_IS_RAISE_ERR = True
    DEFAULT_RETRY_COUNT = 3

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

        # Get host infos
        self.HOSTNAME, self.USERNAME, self.PASSWORD, self.PORT = get_host_infos()
        self.client = SSH(hostname=self.HOSTNAME, username=self.USERNAME, password=self.PASSWORD, port=self.PORT)
        self.client.set_default_is_raise_err(default_is_raise_err=self.DEFAULT_IS_RAISE_ERR)

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
        self.client.connect(timeout=self.TIME_OUT, retry_count=self.RETRY_COUNT)

    def tearDown(self):
        self.client.close()

    def test_connect(self):
        for i in range(self.TEST_LOOP_COUNT):
            self.client.reconnect()
        
    def test_set_default_is_raise_err(self):
        is_passed = False
        # Turn off is_raise_err, shouldn't raise error
        self.client.set_default_is_raise_err(default_is_raise_err=False)
        self.client.exec_command(command='rm new_dir_test', timeout=self.TIME_OUT, retry_count=self.RETRY_COUNT, cmd_retry_count=True)

        # Turn on is_raise_err, should raise error
        self.client.set_default_is_raise_err(default_is_raise_err=True)
        try:
            self.client.exec_command(command='rm new_dir_test', timeout=self.TIME_OUT, retry_count=self.RETRY_COUNT, cmd_retry_count=True)
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
                                                                           bufsize=-1, timeout=self.TIME_OUT, get_pty=False, environment=None, 
                                                                           retry_count=self.RETRY_COUNT, cmd_retry_count=self.CMD_RETRY_COUNT)
        assert is_successed is True
        stdin, stdout, stderr, is_successed = self.client.exec_command(command='rm new_dir_test', timeout=self.TIME_OUT, 
                                                                       retry_count=self.RETRY_COUNT, cmd_retry_count=self.CMD_RETRY_COUNT, is_raise_err=False)
        assert is_successed is False

        is_passed = True
        try:
            self.client.exec_command(command='rm new_dir_test', timeout=self.TIME_OUT, retry_count=self.RETRY_COUNT, cmd_retry_count=True, is_raise_err=True)
            is_passed = False
        except:
            # self.__info(f"Passed test_exec_command()")
            is_passed = True

        try:
            self.client.exec_command(command='rm new_dir_test', timeout=self.TIME_OUT, retry_count=self.RETRY_COUNT, cmd_retry_count=True, is_show_result=False, is_raise_err=True)
            is_passed = False
        except:
            is_passed = True

        if not is_passed:
            self.__error(f"Failed to pass test_exec_command()")
            traceback.print_exc()
            raise BaseException(f"Failed to pass test_exec_command()")

    def test_put(self):
        for i in range(self.TEST_LOOP_COUNT):
            self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

    def test_putfo(self):
        # Test file
        test_file = io.StringIO('HI Test\n' * 100000)
        for i in range(self.TEST_LOOP_COUNT):
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3, is_raise_err=False)
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3, is_raise_err=True)

    def test_get(self):
        for i in range(self.TEST_LOOP_COUNT):
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3)

        for i in range(self.TEST_LOOP_COUNT // 2):
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3, is_raise_err=False)
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3, is_raise_err=True)

def test_run(epoch :int, decay: str, machine: int, gpu: int, dataset_size: int):
    import os
    import jax.numpy as np
    os.environ["CUDA_VISIBLE_DEVICES"] = f'{gpu}'
    print(f"Epoch: {epoch}, Decay: {decay}, Dataset Size: {dataset_size}, Machine: {machine}, GPU: {gpu}")
    sleep(5)

def test_err(flag: str, gpu: int):
    raise BaseException(f"Something wrong with the flag {flag} on gpu {gpu}")

class TestTaskRunner(unittest.TestCase):
    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)

    def test_run(self):
        config = {
            'section-1': { # Each section would be executed sequentially.
                'group-1': { # The groups under the same section would be executed concurrently
                    'Call': test_run, # Call can be either a function call or a command in string
                    'Param': { # The TaskRunner would list all kinds of combination of the parameters and execute them once
                        'decay': ['exp', 'anneal', 'static'],
                        'epoch': [100, 1000, 10000],
                        'dataset_size': [1000, 2000, 3000]
                    },
                    'Async': { # The task under the same group would be schedule to the resources by TaskRunner during runtime.
                        'machine': [0, 1],
                        'gpu': [0, 1]
                    }
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
            }
        }
        
        tr = TaskRunner(config=config, delay=0.5)
        tr.run()

def config_db_runner(db_runner: DBRunner) -> DBRunner:
    db_runner.config_bencher(sequencer="192.168.1.32", 
                             servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                             clients=["192.168.1.9", "192.168.1.8"], 
                             package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    db_runner.config_cluster(server_count=4, jar_dir='latest')
    return db_runner

def get_workspace_name():
    return 'db_runner_workspace_test'

class TestDBRunner(unittest.TestCase):
    # SSH default value
    SSH_DEFAULT_RETRY_COUT = 3
    SSH_DEFAULT_IS_RAISE_ERR = True

    # Configurations
    ELASQL_NAME = "elasql"
    ELASQLBENCH_NAME = "elasqlbench"
    INIT_RECORD_PER_PART_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART"
    RW_TX_RATE_NAME = "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE"
    ENABLE_COLLECTING_DATA_NAME = "org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA"

    INIT_RECORD_PER_PART = "100000"
    ENABLE_COLLECTING_DATA = "true"
    RW_TX_RATE = "1"

    ARGS_LOAD = {
                    ELASQLBENCH_NAME: {
                        INIT_RECORD_PER_PART_NAME: INIT_RECORD_PER_PART
                    }
                }

    ARGS_BENCH = {
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

    def test_bench(self):
        try:
            self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
            self.dr.load(alts=self.ARGS_LOAD, is_kill_java=True)
            for i in range(1):
                self.dr.bench(reports_path=get_temp_dir(), alts=self.ARGS_BENCH, is_pull_reports=True, is_delete_reports=True, 
                              is_kill_java=True)

            # Check configuration bench.toml
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQLBENCH_NAME][self.INIT_RECORD_PER_PART_NAME] == self.INIT_RECORD_PER_PART
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQLBENCH_NAME][self.RW_TX_RATE_NAME] == self.RW_TX_RATE
            assert self.dr.get_bench_config(format=DBRunner.DICT)[self.ELASQL_NAME][self.ENABLE_COLLECTING_DATA_NAME] == self.ENABLE_COLLECTING_DATA

        except:
            self.__error(f"Fail to pass test_bench()")
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
    suite.addTest(TestSSH('test_exec_command'))
    suite.addTest(TestSSH('test_put'))
    suite.addTest(TestSSH('test_putfo'))
    suite.addTest(TestSSH('test_get'))

    # Test TaskRunner
    suite.addTest(TestTaskRunner('test_run'))

    # Test DBRunner
    suite.addTest(TestDBRunner('test_connect'))
    suite.addTest(TestDBRunner('test_upload_jars'))
    suite.addTest(TestDBRunner('test_load'))
    suite.addTest(TestDBRunner('test_bench'))
    return suite

if __name__ == '__main__':
    # unittest.main()
    # python -m unittest tests.test_all.TestDBRunner.test_upload_jars
    test_runner = unittest.TextTestRunner(failfast=True)
    test_runner.run(suite())