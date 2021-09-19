# Scalable Runner

The scalable runner integrate several pacakages that can execute the Python function/shell command remotely/locally. It can also schedule tasks to specific resources(ex: GPUs) automatically.

`Runner` consist of 3 modules:

- [TaskRnner](#TaskRunner): A scalable task runner can schedule tasks to different groups of resources/machines. All you need to do is writting down the config file.
- [SSH](#SSH): A warpper of [paramiko](https://github.com/paramiko/paramiko) and we've implemented **auto-retrying** feature to guarantee the task can always be done.
- [DBRunner](#DBRunner): It can benchmark ElaSQL DBMS automatically with [`Auto-Bencher`](https://github.com/elasql/auto-bencher)

## Installation

Install with pip.

```bash
pip install git+https://github.com/Database-Project-2021/runner.git
```

## Test

Create a ``host_infos.toml`` file under the root diretory of this package like

```
- Runner
    - runner
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
```

Finally, run the ``unittest`` command. It would run all testcases.

```bash
python -m unittest tests.test_all
```

If you only want to test some specific module, ex: ``runner.SSH`` module, you can run like this.

```bash
python -m unittest tests.test_all.TestSSH
```

Or even only test on specific function, like ``runner.SSH.put``, you can run

```bash
python -m unittest tests.test_all.TestSSH.test_put
```

## TaskRunner

### Example Code

An example config, please refer to the [example_task_runner.py](./examples/example_task_runner.py)

```python
from runner.task_runner import TaskRunner
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
                }   
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
from runner.ssh import SSH

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

Please refer to the [example_db_runner.py](./examples/example_db_runner.py)

```python
import os

from runner.db_runner import DBRunner

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

    dr = DBRunner(workspace='db_runner_workspace_test')
    # Connect to the remote host, where Auto-Bencher loactes
    dr.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
    dr = config_db_runner(dr)
    # Init Auto-Bencher
    dr.init()

    dr.set_default_is_raise_err(default_is_raise_err=SSH_DEFAULT_IS_RAISE_ERR)
    dr.set_default_retry_count(default_retry_count=SSH_DEFAULT_RETRY_COUT)
    dr.set_default_cmd_retry_count(default_cmd_retry_count=SSH_DEFAULT_CMD_RETRY_COUT)

    # Upload .jar files
    dr.upload_jars(server_jar='data/jars/server.jar', client_jar='data/jars/client.jar')
    # Load test bed
    dr.load(is_kill_java=True)
    # Benchmark
    dr.bench(reports_path=get_temp_dir(), is_pull_reports=True, is_delete_reports=True, is_kill_java=True)
```