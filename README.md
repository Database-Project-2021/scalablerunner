# Scalable Runner

The scalable runner integrate several pacakages that can execute the Python function/shell command remotely/locally. It can also schedule tasks to specific resources(ex: GPUs) automatically.

`Runner` consist of 3 modules:

- [TaskRnner](#TaskRunner): A scalable task runner can schedule tasks to different groups of resources/machines. All you need to do is writting down the config file.
- [SSH](#SSH): A warpper of [paramiko](https://github.com/paramiko/paramiko) and we've implemented **auto-retrying** feature to guarantee the task can always be done.
- [DBRunner](#DBRunner): 

## TaskRunner

### Usage

An example config: 

```python
from runner.task_runner import TaskRunner

def test_run(epoch :int, decay: str, machine: int, gpu: int, dataset_size: int):
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

## DBRunner