import os

from scalablerunner.dbrunner import DBRunner

def get_temp_dir():
    # Create 'temp' directory
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    return temp_dir

def config_db_runner(db_runner: DBRunner) -> DBRunner:
    # Set up configurations of Auto-Bencher
    db_runner.config_bencher(sequencer="192.168.1.32", 
                             servers=["192.168.1.31", "192.168.1.30", "192.168.1.27", "192.168.1.26"], 
                             clients=["192.168.1.9", "192.168.1.8"], 
                             package_path='/home/db-under/sychou/autobench/package/jdk-8u211-linux-x64.tar.gz')
    # Set up configurations of cluster
    db_runner.config_cluster(server_count=4, jar_dir='latest')
    return db_runner

if __name__ == '__main__':
    HOSTNAME = "your_host_ip"
    USERNAME = "your_username"
    PASSWORD = "your_password"
    
    PORT = 22
    SSH_DEFAULT_RETRY_COUT = 3
    SSH_DEFAULT_CMD_RETRY_COUT = 2
    SSH_DEFAULT_IS_RAISE_ERR = False

    dr = DBRunner()
    # Log file name
    dr.output_log(file_name='temp/example_dr.log')
    # Connect to the remote host, where Auto-Bencher loactes
    dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
    dr = config_db_runner(dr)
    # Init Auto-Bencher
    dr.init()

    # Setting behaviors of the DBRunner
    # Whether raise exception or not while error occur
    dr.set_default_is_raise_err(default_is_raise_err=SSH_DEFAULT_IS_RAISE_ERR)
    # The retrying count while the SSH connection fails
    dr.set_default_retry_count(default_retry_count=SSH_DEFAULT_RETRY_COUT)
    # The redoing count while the SSH command failed
    dr.set_default_cmd_retry_count(default_cmd_retry_count=SSH_DEFAULT_CMD_RETRY_COUT)

    # Upload .jar files
    dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')

    # Load test bed
    BASE_CONFIG_LOAD = 'data/config/load.toml'
    ARGS_LOAD = {
                    "elasqlbench": {
                        "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART": "100000"
                    }
                }
    dr.load(base_config=BASE_CONFIG_LOAD, alts=ARGS_LOAD, is_kill_java=True)

    # Benchmark
    BASE_CONFIG_BENCH = 'data/config/bench.toml'
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
    dr.bench(reports_path=get_temp_dir(), base_config=BASE_CONFIG_BENCH, alts=ARGS_BENCH, is_pull_reports=True, is_delete_reports=True, is_kill_java=True)