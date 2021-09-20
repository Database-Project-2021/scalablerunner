import os
from typing import Tuple

import toml

from scalablerunner.ssh import SSH
from scalablerunner.dbrunner import DBRunner
from scalablerunner.util import info
from cost_estimator import Loader, Preprocessor, RegressionModels, Task

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

def config_db_runner(db_runner: DBRunner) -> DBRunner:
    db_runner.config_bencher(sequencer="192.168.1.32", 
                             servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                             clients=["192.168.1.9", "192.168.1.8"], 
                             package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    db_runner.config_cluster(server_count=4, jar_dir='latest')
    return db_runner

def get_workspace_name():
    return 'db_runner_workspace_test'

class PopCat():
    MICRO = '1'
    TPCC = '2'
    TPCE = '3'
    YCSB = '4'

    def __init__(self, reports_path: str, bench_type: str):
        self.reports_path = reports_path
        self.bench_type = bench_type

    def init(self, sequencer: str, servers: list, clients: list, server_jar: str, client_jar: str,
             package_path: str='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz'):
        HOSTNAME, USERNAME, PASSWORD, PORT = get_host_infos()

        self.dr = DBRunner(workspace=get_workspace_name())
        self.dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
        # self.dr = config_db_runner(self.dr)
        self.dr.config_bencher(sequencer=sequencer, servers=servers, clients=clients, package_path=package_path)
        self.dr.init()
        self.dr.upload_jars(server_jar=server_jar, client_jar=client_jar)

    def load(self, server_count: int, alts: dict, server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None, jar_dir: str='latest'):
        self.dr.config_cluster(server_count=server_count, server_client_ratio=server_client_ratio, max_server_per_machine=max_server_per_machine, max_client_per_machine=max_client_per_machine, jar_dir=jar_dir)
        self.dr.load(is_kill_java=True, alts=alts)

    def benchmark(self, alts: dict):
        self.dr.bench(reports_path=self.reports_path, alts=alts)

    def train_model(self):
        pass

    def call_costestimator(self, estimator_py: str) -> Tuple:
        """
        Call the cost estimator on local client.

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        # self.dr.__type_check(obj=estimator_py, obj_type=str, obj_name='estimator_py', is_allow_none=False)

        stdin, stdout, stderr, is_successed = self.dr.__client_exec(self.dr, fn_name = 'python', going_msg = 'Calling CostEstimator', finished_msg = 'Called CostEstimator', error_msg = 'Failed calling CostEstimator', py_file = estimator_py)

if __name__ == '__main__':
    print("OK")

    IP = "140.114.85.15"
    USERNAME = "db-under"
    PASSWORD = "db-under"
    PORT = 22
    client = SSH(hostname=IP, username=USERNAME, password=PASSWORD, port=PORT)
    client.connect()

    pc = PopCat()
    pc.init()