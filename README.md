# Scalable Runner

The scalable-runner integrate several pacakages that can execute the Python function/shell command remotely/locally. It can also schedule tasks to specific resources(ex: GPUs) automatically.

`Runner` consist of 3 modules:

- [TaskRnner](#TaskRunner): A scalable task-runner can schedule tasks to different groups of resources/machines. All you need to do is writting down the config file.
- [SSH](#SSH): A warpper of [paramiko](https://github.com/paramiko/paramiko) and we've implemented **auto-retrying** feature to guarantee the task can always be done.
- [DBRunner](#DBRunner): It can benchmark ElaSQL DBMS automatically with [`Auto-Bencher`](https://github.com/elasql/auto-bencher)

## Installation

Install with pip.

```bash
pip install git+https://github.com/Database-Project-2021/scalablerunner.git
```

## Test

Create a ``host_infos.toml`` file under the root diretory of this package like

```
- scalablerunner
    - scalablerunner
    - README.md
    - host_infos.toml
```

Then, fill the information of the host IP/URL, login username, login password and the SSH tunnel port. The ``unittest`` would be done on this machine.

```toml
# File name: host_infos.toml
hostname = "your_host_ip"
username = "your_username"
password = "your_password"
port = 22
sequencer = "sequencer_IP"
servers = ["server_1_IP", "server_2_IP", "server_3_IP", "server_4_IP"]
clients = ["client_1_IP", "client_2_IP"]
package_path = 'JDK/package/path'
```

Finally, run the ``unittest`` command. It would run all testcases.

```bash
python -m unittest tests.test_all
```

If you only want to test some specific module, ex: ``scalablerunner.SSH`` module, you can run like this.

```bash
python -m unittest tests.test_all.TestSSH
```

Or even only test on specific function, like ``scalablerunner.SSH.put``, you can run

```bash
python -m unittest tests.test_all.TestSSH.test_put
```

## TaskRunner

### Remote Execution

``TaskRunner`` now supports **Remote Execution** and it can execute a group of task on the another machine. It is implemented with ``rpyc`` package and you can refer to this [page](https://github.com/tomerfiliba-org/rpyc). If you want to execute a task on the target machine, you need to launch a classic RPYC server on the target machine at first. Then, fill the public IP and the service port listened by RPYC server of the target machine to the field ``Remote`` and ``Port``. The ``TaskRunner`` will run the group of task on the target mahine. If you don't specify the port of RPYC server, it would use default port. 

To launch the RPYC server in the backgound on Linux like system, you can use our script ``scripts/install_rpyc.sh``.

```bash
bash scripts/install_rpyc.sh
```

Then the RPYC server would be launch, listening on default port. You can also specify custom port like 18180

```bash
bash scripts/install_rpyc.sh 18180
```

### Example Code

An example config, please refer to the [example_taskrunner.py](./examples/example_taskrunner.py)

```python
from scalablerunner.taskrunner import TaskRunner
from time import sleep

def test_run(epoch :int, decay: str, machine: int, gpu: int, dataset_size: int):
    """
    Example task
    """
    import os
    import jax.numpy as np
    os.environ["CUDA_VISIBLE_DEVICES"] = f'{gpu}'
    print(f"Epoch: {epoch}, Decay: {decay}, Dataset Size: {dataset_size}, Machine: {machine}, GPU: {gpu}")
    sleep(5)
       
if __name__ == '__main__':
    config = {
        'Section: Search for Decay, Epoch, and Dataset Size': { # Each section would be executed sequentially.
            'GTX 2080': { # The groups under the same section would be executed concurrently
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
            'CPU':{ # 'group-2' can be seem as another resource group that handle different task from 'group-1' during 'section-1'
                'Call': 'ls',
                'Param': {
                    '': ['-l', '-a', '-la']  
                },
                'Async': {
                    '': []
                },
                'Remote': 'localhost', # RPYC server IP, it would assign the 'CPU' group to execute on the specify RPYC server
                'Port': 18812 # RPYC server port
            }    
        },
        'Another Section': {
            'A dummy group': {
                'Call': 'ls',
                'Param': {
                    '': ['-a']
                }
            }
        }
    }
    
    tr = TaskRunner(config=config)
    tr.run()
```
## SSH
### Example Code

Please refer to the [example_ssh.py](./examples/example_ssh.py)

```python
from scalablerunner.ssh import SSH

if __name__ == '__main__':
    IP = "your_host_ip"
    USERNAME = "your_username"
    PASSWORD = "your_password"
    PORT = 22
    # A dummy file you want to upload/download
    dummy_file = 'hi.txt'
    download_dest = './'

    client = SSH(hostname=IP, username=USERNAME, password=PASSWORD, port=PORT)
    
    # 'is_raise_err' is true means SSH would raise an exception and stop the program while an error occur
    # Set default value of 'is_raise_err'
    client.set_default_is_raise_err(default_is_raise_err=True)
    # Set default value of retry-count as 3
    client.set_default_retry_count(default_retry_count=3)

    # With 'retry_count' = 3, it would try to establish a connection at nost 3 times while an error ocurrs to SSH session
    # 'timeout' means timeout of the TCP session.
    # 'is_raise_err' means whether raise an exception and top while an error occurs
    client.connect(timeout=20, retry_count=3, is_raise_err=False)

    # Upload a file, parameter 'remote_path' means the destination of the upload
    client.put(files=dummy_file, remote_path=download_dest, recursive=False, preserve_times=False, retry_count=3)
    # Upload a folder
    client.put(files=dummy_file, remote_path=download_dest, recursive=True, preserve_times=False, retry_count=3)

    # Download a file, allowable retry-count is 3
    # As the feature of the parameter 'preserve_times', please refer to paramiko
    # The parameter 'local_path' means the download destination
    client.get(remote_path=dummy_file, local_path=download_dest, recursive=False, preserve_times=False, retry_count=3)
    # Download a folder, allowable retry-count is 3
    client.get(remote_path=dummy_file, local_path=download_dest, recursive=True, preserve_times=False, retry_count=3)

    # Upload a file
    client.putfo(fl='HI \n'*20, remote_path='./test.txt', retry_count=3)

    
    # Make a new directory on the remote host. 'cmd_retry_count' indicates the number of retrying while exeuting the command
    stdin, stdout, stderr, is_successed = client.exec_command(command='ls -la; mkdir new_dir_test; rm -rf new_dir_test', 
                                                              retry_count=3, cmd_retry_count=2)
    
    # Setting 'get_pty' to True would get a interactive session which can type. 'environment' sets the environmen variable to the command.
    stdin, stdout, stderr, is_successed = client.exec_command(command='python', 
                                                              timeout=None, get_pty=True, environment=None, 
                                                              retry_count=3, cmd_retry_count=2)    
```

## DBRunner

Please refer to the [example_dbrunner.py](./examples/example_dbrunner.py)

```python
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
```

## DBRunnerAdapter

``DBRunnerAdapter`` is an interface to integrate ``DBRunner`` and ``TaskRunner``. 

Please refer to the [example_dbrunneradapter.py](./examples/example_dbrunneradapter.py)

```python
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
```