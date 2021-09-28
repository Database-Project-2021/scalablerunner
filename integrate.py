import os
import rpyc
from typing import List, Sequence, Tuple

import toml

from scalablerunner.ssh import SSH
from scalablerunner.dbrunner import DBRunner
from scalablerunner.util import info, BaseClass
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

def name_fn(reports_path: str, alts: dict):
    rte = alts['vanillabench']['org.vanilladb.bench.BenchmarkerParameters.NUM_RTES']
    return os.path.join(reports_path, f'google_rte-{rte}')

# def config_db_runner(db_runner: DBRunner) -> DBRunner:
#     db_runner.config_bencher(sequencer="192.168.1.32", 
#                              servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
#                              clients=["192.168.1.9", "192.168.1.8"], 
#                              package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
#     db_runner.config_cluster(server_count=4, jar_dir='latest')
#     return db_runner

class PopCat(BaseClass):
    YCSB = 0
    def __init__(self, reports_path: str, bench_type: str, workspace: str):
        self.reports_path = reports_path
        self.bench_type = bench_type
        self.dr = DBRunner(workspace)

    def connect(self):
        HOSTNAME, USERNAME, PASSWORD, PORT = get_host_infos()
        self.dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
    
    def close(self):
        self.dr.close()

    def config(self, server_count: int=None, sequencer: str=None, servers: list=None, clients: list=None, package_path: str=None, 
               server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None, jar_dir: str='latest'):

        self.__config(server_count=server_count, sequencer=sequencer, servers=servers, clients=clients, package_path=package_path,
                      server_client_ratio=server_client_ratio, max_server_per_machine=max_server_per_machine, max_client_per_machine=max_client_per_machine, jar_dir=jar_dir)

    def __config(self, server_count: int, sequencer: str, servers: list, clients: list,package_path: str, 
               server_client_ratio: float, max_server_per_machine: int, max_client_per_machine: int, jar_dir: str):
             
        self.dr.config_bencher(sequencer=sequencer, servers=servers, clients=clients, package_path=package_path)
        self.dr.config_cluster(server_count=server_count, server_client_ratio=server_client_ratio, max_server_per_machine=max_server_per_machine, max_client_per_machine=max_client_per_machine, jar_dir=jar_dir)

    def init(self, server_jar: str, client_jar: str):
        self.connect()
        # self.dr = config_db_runner(self.dr)
        self.dr.init()
        self.dr.upload_jars(server_jar=server_jar, client_jar=client_jar, use_stable=True)
        self.close()

    def load(self, alts: dict=None, base_config: str=None):
        self.connect()
        self.dr.load(is_kill_java=True, alts=alts, base_config=base_config)
        self.close()

    def init_load(self, server_jar: str, client_jar: str, base_config: str=None, alts: dict=None):
        self.init( server_jar=server_jar, client_jar=client_jar)
        self.load(alts=alts, base_config=base_config)

    def benchmark(self, name_fn: callable, alts: dict=None, base_config: str=None, event: str=None):
        self.connect()
        rp_path = name_fn(reports_path=self.reports_path, alts=alts)
        self.dr.bench(is_kill_java=True, reports_path=rp_path, alts=alts, base_config=base_config)
        self.close()

    def __remote_estimator(self):
        """
        Call the cost estimator on local client.

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        # self.dr.__type_check(obj=estimator_py, obj_type=str, obj_name='estimator_py', is_allow_none=False)
        stdin, stdout, stderr, is_successed = self.dr.__client_exec(self.dr, fn_name = 'python', going_msg = 'Calling CostEstimator', finished_msg = 'Called CostEstimator', error_msg = 'Failed calling CostEstimator', py_file = self.ESTIMATOR_PY)

    def __local_estimator(self):
        pass

    def local_estimator(self, **kwargs) -> None:
        self.__local_estimator(**kwargs)

    def remote_estimator(self, hostname: str, username: str, passward: str, port: int, event: str, **kwargs) -> None:
        for server in kwargs['remote_servers']:
            conn = rpyc.classic.connect(server)
            fn = conn.teleport(self.__remote_estimator)
            fn()
        # ssh.exec_command(command=f'source /home/{username}/.bashrc; conda activate jax-ntk; python estimator.py')

class Socket():
    def __init__(self):
        pass

    def raise_event(self):
        pass

    def wait_event(self):
        pass

cw = {
    'workspace': 'db_runner_workspace_cw',
    'package_path': '/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz',
    # New
    # 'sequencer': "192.168.1.6",
    # 'servers': ["192.168.1.15", "192.168.1.16", "192.168.1.17", "192.168.1.27"],
    # 'clients': ["192.168.1.30", "192.168.1.31"],
    # SY
    'sequencer': "192.168.1.32", 
    'servers': ["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
    'clients': ["192.168.1.9", "192.168.1.8"], 
    'load_alts': [None],
    'load_base_config': ['temp/smg/load.smg.toml'],
    'bench_alts': [
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "50",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "75",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "100",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "125",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "150",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "175",
            },
        },
        {
            'vanillabench':{
                'org.vanilladb.bench.BenchmarkerParameters.NUM_RTES': "200",
            },
        }
    ],
    'bench_base_config': ['temp/smg/bench.smg.toml']
#     'benc_alts': {
#                     'vanillabench': {
#                         'org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL': "30000"
#                     },
#                     'elasql': {
#                         'org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA': "true"
#                     },
#                     'elasqlbench': {
#                         'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000",
#                         'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE': "0"
#                     }
#                 }
}

# sy = {
#     'sequencer': "192.168.1.32", 
#     'servers': ["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
#     'clients': ["192.168.1.9", "192.168.1.8"], 
#     'workspace': 'db_runner_workspace_sy',
#     'benc_alts': {
#                     'vanillabench': {
#                         'org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL': "30000"
#                     },
#                     'elasql': {
#                         'org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA': "true"
#                     },
#                     'elasqlbench': {
#                         'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000",
#                         'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE': "1"
#                     }
#                 }
# }

package_path = '/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz'
server_jar = 'data/jars/server.jar'
client_jar = 'data/jars/client.jar'
server_count = 4
# load_alts = {
#                 'elasqlbench': {
#                     'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000"
#                 }
#             }

if __name__ == '__main__':
    pc = PopCat(reports_path='temp/integrate', bench_type=PopCat.YCSB, workspace=cw['workspace'])
    pc.config(server_count=1, sequencer=cw['sequencer'], servers=cw['servers'], clients=cw['clients'])

    # pc.init_load(server_jar=server_jar, client_jar=server_jar, alts=cw['load_alts'][0], base_config=cw['load_base_config'][0])
    # pc.benchmark(name_fn=name_fn, alts=cw['bench_alts'][0], base_config=cw['bench_base_config'][0])

    config = {
        'Section Initialize | 1 server': {
            'Group CW': {
                'Call': pc.init_load,
                'Param': {
                    'server_jar': [server_jar], 
                    'client_jar': [client_jar],
                    'alts': cw['load_alts'],
                    'base_config': cw['load_base_config'],
                }
            },
        },
        'Section Benchmark | 1 server': {
            'Group CW': {
                'Call': pc.benchmark,
                'Param': {
                    'name_fn': [name_fn],
                    'alts': cw['bench_alts'],
                    'base_config': cw['bench_base_config']
                }
            },
        }
    }

    tr = TaskRunner(config=config)
    tr.run()