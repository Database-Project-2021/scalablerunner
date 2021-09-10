import os
import io
import unittest
import traceback

import paramiko
from scp import SCPClient

from src.runner.util import info, warning, error, type_check
from src.runner.ssh import SSH
from src.runner.db_runner import DBRunner
# from src.runner.db_runner import DBRunner

# def run(fn):
#     fn(files='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', remote_path='/home/db-under/db_runner_workspace/auto-bencher/jars/latest/')

# def testing():
#     ip = "140.114.85.15"
#     username = "db-under"
#     password = "db-under"

#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     client.connect(hostname=ip, username=username, password=password)
#     scpClient = SCPClient(client.get_transport(), progress4=progress4)
#     fn = scpClient.put
#     for i in range(10):
#         try:
#             # scpClient.put(files='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', remote_path='/home/db-under/db_runner_workspace/auto-bencher/jars/latest/')
#             run(fn)
#             print(f"Testing Success: {info('Upload Success')}")
#         except:
#             print(f"Testing Error: {error('Upload Failed')}")
#             client.close()
#             client.connect(hostname=ip, username=username, password=password)
#             scpClient = SCPClient(client.get_transport(), progress4=progress4)
#             print(f"Testing Warning: {warning('Reconnected')}")

def get_temp_dir():
    # Create 'temp' directory
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir

class TestSSH(unittest.TestCase):
    IP = "140.114.85.15"
    USERNAME = "db-under"
    PASSWORD = "db-under"
    PORT = 22

    TEST_LOOP_COUNT = 10

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)
        self.client = SSH(hostname=self.IP, username=self.USERNAME, password=self.PASSWORD, port=self.PORT)

    def test_connect(self):
        self.client.connect(timeout=20, retry_count=3)
        for i in range(self.TEST_LOOP_COUNT):
            self.client.reconnect()
        self.client.close()

    def test_put(self):
        self.client.connect(timeout=20, retry_count=3)
        for i in range(self.TEST_LOOP_COUNT):
            self.client.put(files='data/jars/server.jar', remote_path='./', recursive=False, preserve_times=False, retry_count=3)

        self.client.close()

    def test_putfo(self):
        self.client.connect(timeout=20, retry_count=3)

        # Test file
        test_file = io.StringIO('HI Test\n' * 100000)
        for i in range(self.TEST_LOOP_COUNT):
            self.client.putfo(fl=test_file, remote_path='./test.txt', retry_count=3)

        self.client.close()

    def test_get(self):
        self.client.connect(timeout=20, retry_count=3)

        for i in range(self.TEST_LOOP_COUNT):
            self.client.get(local_path=get_temp_dir(), remote_path='./server.jar', recursive=False, preserve_times=False, retry_count=3)

        self.client.close()


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
    
    # def test_Logger(self):
    #     Logger

class TestDBRunner(unittest.TestCase):
    IP = "140.114.85.15"
    USERNAME = "db-under"
    PASSWORD = "db-under"

    TEST_LOOP_COUNT = 1

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
        self.dr = DBRunner()
        self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
        self.dr.config_bencher(sequencer="192.168.1.32", 
                    servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                    clients=["192.168.1.9", "192.168.1.8"], package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
        self.dr.config_cluster(server_count=4, jar_dir='latest')
        self.dr.init()
    
    def test_connect(self):
        try:
            self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
            for i in range(self.TEST_LOOP_COUNT):
                self.dr.close()
            self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
        except:
            traceback.print_exc()
            self.__error(f"Fail to pass test_connect")
        finally:
            self.dr.close()

    def test_upload_jars(self):
        try:
            self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
            for i in range(self.TEST_LOOP_COUNT):
                self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
        except:
            traceback.print_exc()
            self.__error(f"Fail to pass test_upload_jars")
        finally:
            self.dr.close()

    def test_load(self):
        try:
            self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
            for i in range(self.TEST_LOOP_COUNT):
                self.dr.load()
        except:
            traceback.print_exc()
            self.__error(f"Fail to pass test_load()")
        finally:
            self.dr.close()

    def test_bench(self):
        try:
            self.dr.connect(hostname=self.IP, username=self.USERNAME, password=self.USERNAME)
            self.dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
            for i in range(self.TEST_LOOP_COUNT):
                self.dr.bench(reports_path=get_temp_dir())
        except:
            traceback.print_exc()
            self.__error(f"Fail to pass test_bench()")
        finally:
            self.dr.close()

    # def test_module(self):
    #     # pass
    #     dr.config_bencher(sequencer="192.168.1.32", 
    #                     servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
    #                     clients=["192.168.1.9", "192.168.1.8"], package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    #     dr.config_cluster(server_count=4, jar_dir='latest')
    #     dr.init()
    #     dr.upload_jars(server_jar='/home/weidagogo/sychou/db/DBRunner/jars/server.jar', 
    #                     client_jar='/home/weidagogo/sychou/db/DBRunner/jars/client.jar')
    #     dr.load()
    #     dr.bench(reports_path='/home/weidagogo/sychou/db/results', is_delete_reports=False)
    #     dr.kill_java()

if __name__ == '__main__':
    unittest.main()