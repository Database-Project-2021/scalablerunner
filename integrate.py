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

# def config_db_runner(db_runner: DBRunner) -> DBRunner:
#     db_runner.config_bencher(sequencer="192.168.1.32", 
#                              servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
#                              clients=["192.168.1.9", "192.168.1.8"], 
#                              package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
#     db_runner.config_cluster(server_count=4, jar_dir='latest')
#     return db_runner

# def get_workspace_name():
#     return 'db_runner_workspace_test'

class PopCat(BaseClass):
    # MICRO = '1'
    # TPCC = '2'
    # TPCE = '3'
    # YCSB = '4'

    # ESTIMATOR_PY = 'cost_est.py'

    # LOCAL = 'local'
    # REMOTE = 'remote'

    def __init__(self, reports_path: str, bench_type: str):
        self.reports_path = reports_path
        self.bench_type = bench_type

    def init(self, server_count: int, sequencer: str, servers: list, clients: list, server_jar: str, client_jar: str, workspace: str,
             package_path: str='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz', 
             server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None, jar_dir: str='latest'):
        HOSTNAME, USERNAME, PASSWORD, PORT = get_host_infos()

        self.dr = DBRunner(workspace)
        self.dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
        # self.dr = config_db_runner(self.dr)
        self.dr.config_bencher(sequencer=sequencer, servers=servers, clients=clients, package_path=package_path)
        self.dr.config_cluster(server_count=server_count, server_client_ratio=server_client_ratio, max_server_per_machine=max_server_per_machine, max_client_per_machine=max_client_per_machine, jar_dir=jar_dir)
        self.dr.init()
        self.dr.upload_jars(server_jar=server_jar, client_jar=client_jar, use_stable=True)

    def load(self, alts: dict):
        self.dr.load(is_kill_java=True, alts=alts)

    def init_load(self, server_count: int, sequencer: str, servers: list, clients: list, server_jar: str, client_jar: str, workspace: str, alts: dict):
        self.init(server_count=server_count, sequencer=sequencer, servers=servers, clients=clients, server_jar=server_jar, client_jar=client_jar, workspace=workspace)
        self.load(alts=alts)

    def benchmark(self, name: str, alts: dict, event: str):
        self.dr.bench(reports_path=self.reports_path, alts=alts)

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
        # self.__remote_estimator(*args, **kwargs)
        # ssh = SSH(hostname=hostname, username=username, passward=passward, port=port)
        # ssh.connect()
        # ssh.put(files='cost_est.py', remote_path='estimator.py')
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
    'sequencer': "192.168.1.20",
    'servers': ["192.168.1.16", "192.168.1.17", "192.168.1.23", "192.168.1.19"],
    'clients': ["192.168.1.3", "192.168.1.4"],
    'workspace': 'db_runner_workspace_cw',
    'benc_alts': {
                    'vanillabench': {
                        'org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL': "30000"
                    },
                    'elasql': {
                        'org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA': "true"
                    },
                    'elasqlbench': {
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000",
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE': "0"
                    }
                }
}

sy = {
    'sequencer': "192.168.1.32", 
    'servers': ["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
    'clients': ["192.168.1.9", "192.168.1.8"], 
    'workspace': 'db_runner_workspace_sy',
    'benc_alts': {
                    'vanillabench': {
                        'org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL': "30000"
                    },
                    'elasql': {
                        'org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA': "true"
                    },
                    'elasqlbench': {
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000",
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE': "1"
                    }
                }
}

wy = {
    'sequencer': "192.168.1.32", 
    'servers': ["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
    'clients': ["192.168.1.9", "192.168.1.8"], 
    'workspace': 'db_runner_workspace_wy',
    'benc_alts': {
                    'vanillabench': {
                        'org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL': "30000"
                    },
                    'elasql': {
                        'org.elasql.perf.tpart.ai.Estimator.ENABLE_COLLECTING_DATA': "true"
                    },
                    'elasqlbench': {
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000",
                        'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE': "0.5"
                    }
                }
}

package_path = '/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz'
server_jar = 'data/jars/server.jar'
client_jar = 'data/jars/client.jar'
server_count = 4
load_alts = {
                'elasqlbench': {
                    'org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART': "100000"
                }
            }

if __name__ == '__main__':
    pc = PopCat(reports_path='temp/integrate', bench_type=PopCat.YCSB)
    # pc.init_load(server_count=server_count, sequencer=sy['sequencer'], servers=sy['servers'], clients=sy['clients'], 
    #              workspace=sy['workspace'], server_jar=server_jar, client_jar=server_jar, alts=load_alts)
    # pc.benchmark(alts=sy['benc_alts'])

    config = {
        'Section Initialize': {
            'Group CW': {
                'Call': pc.init_load,
                'Param': {
                    'server_count': [server_count],
                    'sequencer': [cw['sequencer']],
                    'servers': [cw['servers']],
                    'clients': [cw['clients']],
                    'workspace': [cw['workspace']],
                    'server_jar': [server_jar],
                    'client_jar': [server_jar],
                    'alts': [load_alts]
                }
            },
            'Group SY': {
                'Call': pc.init_load,
                'Param': {
                    'server_count': [server_count],
                    'sequencer': [sy['sequencer']],
                    'servers': [sy['servers']],
                    'clients': [sy['clients']],
                    'workspace': [sy['workspace']],
                    'server_jar': [server_jar],
                    'client_jar': [server_jar],
                    'alts': [load_alts]
                }
            }
        },
        'Section Benchmark': {
            'Group CW': {
                'Call': pc.benchmark,
                'Param': {
                    'alts': [cw['benc_alts']]
                }
            },
            'Group SY': {
                'Call': pc.benchmark,
                'Param': {
                    'alts': [sy['benc_alts']]
                }
            }
        }
    }

    tr = TaskRunner(config=config)
    tr.run()