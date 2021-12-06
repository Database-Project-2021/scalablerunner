import os

from scalablerunner.taskrunner import TaskRunner
from scalablerunner.adapter import DBRunnerAdapter

def get_temp_dir():
    # Create 'temp' directory
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir

def config_db_runner_adapter(db_runner_adapter: DBRunnerAdapter) -> DBRunnerAdapter:
    db_runner_adapter.config(server_count=4, jar_dir='latest', sequencer="192.168.1.32", 
                             servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                             clients=["192.168.1.9", "192.168.1.8"], 
                             package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    return db_runner_adapter

def name_fn(reports_path: str, alts: dict):
    rw = alts['elasqlbench']['org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE']
    rw_dir = os.path.join(reports_path, f'rw-{rw}')
    if not os.path.isdir(rw_dir):
        os.makedirs(rw_dir) 
    return rw_dir

if __name__ == '__main__':
    HOSTNAME = "your_host_ip"
    USERNAME = "your_username"
    PASSWORD = "your_password"

    PORT = 22
    SSH_DEFAULT_RETRY_COUT = 3
    SSH_DEFAULT_CMD_RETRY_COUT = 2
    SSH_DEFAULT_IS_RAISE_ERR = False

    dra = DBRunnerAdapter(reports_path=get_temp_dir())
    # Log file name
    dra.output_log(file_name='temp/example_dra.log')
    # Connect to the remote host, where Auto-Bencher loactes
    dra.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
    dra = config_db_runner_adapter(dra)

    # Setting behaviors of the DBRunnerAdapter
    # Whether raise exception or not while error occur
    dra.set_default_is_raise_err(default_is_raise_err=SSH_DEFAULT_IS_RAISE_ERR)
    # The retrying count while the SSH connection fails
    dra.set_default_retry_count(default_retry_count=SSH_DEFAULT_RETRY_COUT)
    # The redoing count while the SSH command failed
    dra.set_default_cmd_retry_count(default_cmd_retry_count=SSH_DEFAULT_CMD_RETRY_COUT)


    ARGS_LOAD = {
                    "elasqlbench": {
                        "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART": "100000"
                    }
                }

    ARGS_BENCH = {
                    "vanillabench": {
                        "org.vanilladb.bench.BenchmarkerParameters.BENCHMARK_INTERVAL": "120000",
                    },
                    "elasql": {
                        "org.elasql.perf.tpart.TPartPerformanceManager.ENABLE_COLLECTING_DATA": "true"
                    },
                    "elasqlbench": {
                        "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART": "100000",
                        "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE": "1"
                    }
                }

    # Custom parameters
    # [Class, Parameter name, Value]
    PARAMS = [["elasqlbench", "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE", "1"],
              ["elasqlbench", "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE", "0"]]

    # Base configurations
    LOAD_CONFIG = 'data/config/load.toml'
    BENCH_CONFIG = 'data/config/bench.toml'
    
    config_test = {
            f'Section Initialize': {
                'Group Test': {
                    'Call': dra.init_autobencher_load_test_bed,
                    'Param': {
                        'server_jar': ['data/jars/server.jar'], 
                        'client_jar': ['data/jars/client.jar'],
                        'alts': [ARGS_LOAD],
                        'base_config': [LOAD_CONFIG],
                        # An arbitrary name for the parameter that want to modify. You can give multiple custom parameters.
                        'custom_param1': PARAMS,
                    }
                },
            },
            f'Section Benchmark': {
                'Group Test': {
                    'Call': dra.benchmark,
                    'Param': {
                        'name_fn': [name_fn],
                        'alts': [ARGS_BENCH],
                        'base_config': [BENCH_CONFIG],
                        # An arbitrary name for the parameter that want to modify. You can give multiple custom parameters.
                        'custom_param1': PARAMS,
                    }
                },
            }
        }
    tr = TaskRunner(config=config_test)
    tr.run()