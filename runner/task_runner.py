import traceback
import os
from time import sleep
import types
from typing import Callable, Tuple, Type, TypeVar, Union
from pygments.console import colorize
from multiprocessing import Process, Queue

from runner.util import UtilLogger, BaseClass

def info(msg: str) -> None:
    print(colorize('green', msg))

def warning(msg: str) -> None:
    print(colorize('yellow', msg))
    
def error(msg: str) -> None:
    print(colorize('red', msg))
    
def delay():
    sleep(1)

class ProcessTask():
    """
    An abstract class defines the task that can be executed by ProcessPool
    """
    def run(self):
        pass

class Task(ProcessTask):
    def __init__(self, call: Union[str, Callable]) -> None:
        self.call = call
        self.param_list = []
        self.param_list_str = ''
        self.param_list_dict = {}
        
        if isinstance(self.call, str):
           self.is_str = True
        elif isinstance(self.call, types.FunctionType):
            self.is_str = False
        else:
            raise TypeError(f"Parameter 'call' must be either string or callable")
    
    def __convert_param_list_to_str(self) -> str:
        params = []
        for param in self.param_list:
            if param[0] == '':
                params.append(''.join([str(param[0]), str(param[1])]))
            else:
                params.append(' '.join([str(param[0]), str(param[1])]))
        return ' '.join(params)
        
    def __convert_param_list_to_dict(self) -> dict:
        params = {}
        for param in self.param_list:
            params[param[0]] = param[1]
        return params
    
    def __update_task(self):
        self.param_list_str = self.__convert_param_list_to_str()
        self.param_list_dict = self.__convert_param_list_to_dict()
    
    def __union_dict(self, d_1, d_2):
        return dict(list(d_1.items()) + list(d_2.items()))
    
    def add_options(self, options: list) -> None:
        if isinstance(options, list):
            if len(options) > 0:
                self.param_list += options
                self.__update_task()
        elif options != None:
            raise TypeError(f"Parameter 'options' should be a list or None.")

    def get(self) -> Union[Tuple[Callable, dict], Tuple[str, str]]:
        if self.is_str:
            param = self.__convert_param_list_to_str()
        else:
            param = self.__convert_param_list_to_dict()
        return self.call, param
    
    def run(self) -> None:
        call, param = self.get()
        if self.is_str:
            # print([call, param])
            if isinstance(param, str) and len(param) > 0:
                try:
                    # subprocess.run([call, param])
                    os.system(f"{call} {param}")
                except:
                    traceback.print_exc()
                    warning(f"Warning: Cannot execute the task: {call} {param}")
            else:   
                try: 
                    # subprocess.run([call])
                    os.system(f"{call} {param}")
                except:
                    traceback.print_exc()
                    warning(f"Warning: Cannot execute the task: {call}")
        else:
            try: 
                call(**param)
            except:
                traceback.print_exc()
                warning(f"Warning: Cannot execute the task: {call.__name__}({param})")
    
    def __str__(self) -> str:
        params = []
        for param in self.param_list:
            if param[0] == '':
                params.append(''.join([str(param[0]), str(param[1])]))
            else:
                params.append(' '.join([str(param[0]), str(param[1])]))
        param_str = ' '.join(params)
        
        if self.is_str:
            string = f'{self.call} {param_str}'
        else:
            string = f'{self.call.__name__}(): {param_str}'
        return string
    
    def __repr__(self) -> str:
        return self.__str__()

class ProcessArg():
    def __init__(self, id: int, msg_in_queue: Queue, msg_out_queue: Queue):
        self.id = id
        self.msg_in_queue = msg_in_queue
        self.msg_out_queue = msg_out_queue
        
class ProcessPool():
    def __init__(self, num_process: int):
        self.num_process = num_process
        self.msg_name = 'msg'
        self.task_name = 'task'
        self.res_name = 'result'
        
        # Status msg
        self.running_msg = 'ongoing'
        self.done_msg = 'done'
        
    def init(self, get_task_fn: Callable[[int], TypeVar("T")], on_recv_fn: Callable[[TypeVar("T")], None]) -> None:
        if not isinstance(get_task_fn, types.FunctionType):
            raise TypeError(f"The parameter 'get_task_fn' should be a callable function.")
        
        if not isinstance(on_recv_fn, types.FunctionType):
            raise TypeError(f"The parameter 'on_recv_fn' should be a callable function.")
        
        self.get_task_fn = get_task_fn
        self.on_recv_fn = on_recv_fn
        
    def __pack_to_slave_msg(self, msg: str, task: ProcessTask) -> dict:
        return {self.msg_name: msg, self.task_name: task}
    
    def __unpack_to_slave_msg(self, queue_msg: dict) -> Tuple[str, ProcessTask]:
        msg = queue_msg.get(self.msg_name, None)
        task = queue_msg.get(self.task_name, None)
        # print(f"Msg: {msg}, Task: {task}")
        
        if not isinstance(msg, str):
            raise TypeError(f"The value of the key '{self.msg_name}' in the queue message isn't a integer, but {type(msg)}.")
        if not isinstance(task, ProcessTask):
            raise TypeError(f"The value of the key '{self.task_name}' in the queue message isn't a ProcessTask, but {type(task)}.")
        return msg, task
    
    def __send_to_slave(self, id: int, msg: str, task: Callable) -> None:
        # print(f"Send to Slave {id}, Msg: {msg}")
        self.send_msg_to_slave_queue_list[id].put(self.__pack_to_slave_msg(msg=msg, task=task))
        
    def __recv_from_slave(self) -> Tuple[str, object]:
        new_res = self.recv_msg_form_slave_queue.get()
        msg, res = self.__unpack_to_master_msg(queue_msg=new_res)
        # print(f"Recv from Slave {msg}, Msg: {msg}, Result: {res}")
        return msg, res
        
    def __pack_to_master_msg(self, msg: str, result: object) -> dict:
        return {self.msg_name: msg, self.res_name: result}
    
    def __unpack_to_master_msg(self, queue_msg: dict) -> Tuple[int, Union[object, None]]:
        msg = queue_msg.get(self.msg_name, None)
        res = queue_msg.get(self.res_name, None)
        
        if not isinstance(msg, int):
            raise TypeError(f"The value of the key '{self.msg_name}' in the queue message isn't a integer, but {type(msg)}.")
        return msg, res
        
    def __acquire_task(self, arg: ProcessArg) -> Tuple[str, ProcessTask]:
        queue_msg = arg.msg_in_queue.get()
        return self.__unpack_to_slave_msg(queue_msg=queue_msg)
    
    def __send_res(self, arg: ProcessArg, res: object) -> None:
        arg.msg_out_queue.put(self.__pack_to_master_msg(msg=arg.id, result=res))
        
    def __slave_fn(self, arg: ProcessArg) -> None:
        """
        msg = {
            'msg': string message
            'task': Callable function to execute
        }
        """
        
        # Acquire new task
        msg, task = self.__acquire_task(arg=arg)
        while(msg != self.done_msg):
            # Execute task
            res = task.run()
            # Send back results
            self.__send_res(arg=arg, res=res)
            # Acquire new task
            msg, task = self.__acquire_task(arg=arg)
    
    def __create_processes(self) -> None:
        self.send_msg_to_slave_queue_list = [Queue() for i in range(self.num_process)]
        self.recv_msg_form_slave_queue = Queue()
        
        self.proc_list = [Process(target=self.__slave_fn, 
                                  args=(ProcessArg(id=i, 
                                                  msg_in_queue=self.send_msg_to_slave_queue_list[i], 
                                                  msg_out_queue=self.recv_msg_form_slave_queue), )) for i in range(self.num_process)]
    
    def run(self) -> None:
        # Phase 1
        self.__create_processes()
        slave_counter = 0
        new_task = self.get_task_fn(id=slave_counter)
        
        while(new_task != None):
            if not isinstance(new_task, ProcessTask):
                raise TypeError(f"The task that acquired from 'self.get_task_fn' should inherit from the class 'ProcessTask'.")
            self.__send_to_slave(id=slave_counter, msg=self.running_msg, task=new_task)
            
            slave_counter += 1
            if slave_counter >= self.num_process:
                break
            new_task = self.get_task_fn(id=slave_counter)
            
        for proc in self.proc_list:
            proc.start()
            delay()
        
        # Phase 2
        while(True):
            # Receive results
            recv_msg, recv_res = self.__recv_from_slave()
            self.on_recv_fn(recv_msg, recv_res)
            
            # Send task to slaves
            new_task = self.get_task_fn(id=recv_msg)
            if new_task == None:
                break
            self.__send_to_slave(id=recv_msg, msg=self.running_msg, task=new_task)
            
        # Phase 3
        for id in range(self.num_process):
            self.__send_to_slave(id=id, msg=self.done_msg, task=ProcessTask())
            
        for proc in self.proc_list:
            proc.join()
        
        while(not self.recv_msg_form_slave_queue.empty()):
            recv_msg, recv_res = self.__recv_from_slave()
            self.on_recv_fn(recv_msg, recv_res)
        
class ResourceManager():
    def __init__(self, resources: list) -> None:
        self.resources = resources
        self.resource_occupation_list = []
        for i in range(len(resources)):
            # If no process occupation
            self.resource_occupation_list.append(None)
            
        if len(resources) <= 0:
            self.is_multi_resources = False
        else:
            self.is_multi_resources = True
    
    def release_resource(self, id: int) -> None:
        if self.is_multi_resources:
            for idx, occup_id in enumerate(self.resource_occupation_list):
                if id == occup_id:
                    self.resource_occupation_list[idx] = None
                    return
    
    def acquire_resource(self, id: int, is_blocking: bool=True):
        first_loop = True
        if self.is_multi_resources:
            while(is_blocking or first_loop):
                for idx, occup_id in enumerate(self.resource_occupation_list):
                    # If no process occupation
                    if occup_id == None:
                        self.resource_occupation_list[idx] = id
                        return idx, self.resources[idx]
                first_loop = False
        return None, None

class GroupController():
    def __init__(self, section_name: str, group_name: str, tasks: dict) -> None:
        if not isinstance(tasks, dict):
            raise TypeError(f"Parameter 'tasks' of section-group '{section_name} - {group_name}' is not a dictionary")
        
        if (tasks.keys() == None) or (len(tasks.keys()) == 0):
            raise ValueError(f"Parameter 'tasks' of section-group '{section_name} - {group_name}' shouldn't be empty")
        
        self.section_name = section_name
        self.group_name = group_name
        self.tasks = tasks
        
        # Entry keywords
        self.call_entry_name = 'Call'
        self.param_entry_name = 'Param'
        self.async_entry_name = 'Async'
        self.callback_entry_name = 'Callback'
        
    def __build_combination_list(self, param_list: list, entry_idx: int=0, param_obj: list=[]) -> list:
        if not isinstance(param_list, list):
            raise TypeError(f"Parameter 'param_list' is not a list")
        
        if param_list == []:
            return []
        
        # print(f"param_list[entry_idx][0]: {param_list[entry_idx][0]}")
        # print(f"param_list[entry_idx][1]: {param_list[entry_idx][1]}")
        # print(f"entry_idx: {entry_idx}")
        
        entry_name = param_list[entry_idx][0]
        vals = param_list[entry_idx][1]
        
        if not isinstance(vals, list):
            raise TypeError(f"The value of the entry of the parameters should be a list")
        
        param_entry_num = len(param_list)
        if param_entry_num == 0:
            return None
        
        param_combination_list = []
        for val in vals:
            new_param = param_obj.copy()
            new_param.append((entry_name, val))
            # print(f"new_param: {new_param}")
            
            next_entry_idx = entry_idx + 1
            if next_entry_idx < param_entry_num:
                res = self.__build_combination_list(entry_idx=next_entry_idx, param_obj=new_param, param_list=param_list)
                if res != None:
                    param_combination_list += res
            else:
                param_combination_list.append(new_param)
            # print(f"param_combination_list: {param_combination_list}")
        return param_combination_list
    
    def __build_list(self, params: dict) -> list:
        if isinstance(params, dict):
            return [(k, params[k]) for k in params.keys()]
        else:
            return []
    
    def __init_tasks(self) -> None:
        self.call = self.tasks.get(self.call_entry_name, None)
        if not (isinstance(self.call, str) or isinstance(self.call, types.FunctionType)):
            raise TypeError(f"Value of entry 'Call' of section-group '{self.section_name} - {self.group_name}' is neither a function nor string")
        
        # Convert parameter config to list
        self.param_list = self.__build_list(params=self.tasks.get(self.param_entry_name, None))
        self.async_list = self.__build_list(params=self.tasks.get(self.async_entry_name, None))
        self.param_counter = 0
        self.async_counter = 0
        self.task_counter = 0
        
        # List all kind of combination of parameter settings
        self.param_combination_list = self.__build_combination_list(param_list=self.param_list)
        self.async_combination_list = self.__build_combination_list(param_list=self.async_list)
        self.resourceMgr = ResourceManager(resources=self.async_combination_list)
        
        # If there is only one combination of parameters
        if len(self.param_combination_list) <= 1:
            self.is_single_task = True
        else:
            self.is_single_task = False
        
        # If there is only one asynchronous resource
        if len(self.async_combination_list) == 0:
            self.process_pool = ProcessPool(num_process=1)
            self.is_multi_resources = False
        else:    
            self.process_pool = ProcessPool(num_process=len(self.async_combination_list))
            self.is_multi_resources = True
            
        self.results_list = []
        self.is_init_tasks = True
        # print(f"self.param_combination_list: {self.param_combination_list}")
        # print(f"self.async_combination_list: {self.async_combination_list}")
        
    def __next_param(self) -> Tuple[int, dict]:
        if not self.is_init_tasks:
            raise BaseException(f"Please call method 'self.__init_tasks()' before using __next_param()")
        if self.param_counter < len(self.param_combination_list):
            next_idx = self.param_counter
            next_param = self.param_combination_list[self.param_counter]
        else:
            next_idx = None
            next_param = None
        self.param_counter += 1
        
        return next_idx, next_param
    
    def __next_async(self, is_wait: bool=True):
        if not self.is_init_tasks:
            raise BaseException(f"Please call method 'self.__init_tasks()' before using __next_async()")
        # 
        next_idx = self.async_counter
        # print(f"Async next_idx: {next_idx}")
        
        # Choose 
        async_num = len(self.async_combination_list)
        if async_num > 0:
            next_async = self.async_combination_list[self.async_counter]
            self.async_counter = (self.async_counter + 1) % async_num
        else:
            next_async = None
            next_idx = None
        
        return next_idx, next_async
    
    def __make_task(self, call: Union[str, Callable], param: list, async_opt: list) -> Task:
        task = Task(call=call)
        task.add_options(options=param)
        task.add_options(options=async_opt)
        
        return task
        
    def __next_task(self, id: int, is_blocking=True) -> Task:
        if not self.is_init_tasks:
            raise BaseException(f"Please call method 'self.__init_tasks()' before using __next_async()")
        
        # Except for the group has only one task, the next_param may be None(Only have call but no param). Otherwise, the next_param shouldn't be None.
        next_param_idx, next_param = self.__next_param()
        if self.task_counter > 0 and self.task_counter >= len(self.param_combination_list):
            return None
        
        # Acquire resource during runtime
        next_async_idx, next_async = self.resourceMgr.acquire_resource(id=id, is_blocking=is_blocking)
        
        # print(f"Next Param: {next_param}, Type: {type(next_param)}, None: {type(None)}")
        # print(f"Next Async: {next_async}, Type: {type(next_async)}, None: {type(None)}")
        
        # Make task and incease self.task_counter
        next_task = self.__make_task(call=self.call, param=next_param, async_opt=next_async)
        self.task_counter += 1
        return next_task

    def __run_task(self, id: int, task: Task) -> None:
        info(f"-->Run Task: {self.section_name} | {self.group_name} | {task}")
        if isinstance(task, Task):
            task.run()
            self.resourceMgr.release_resource(id=id)
        else:
            ValueError(f"Parameter 'task' requires a Task object.")
            
    def run(self):
        self.__init_tasks()
        if self.is_single_task:
            # print(f"Getting next task")
            next_task = self.__next_task(id=0, is_blocking=False)
            # print(f"Running task")
            self.__run_task(id=0, task=next_task)
        else:
            # The callback function would be executed while the ProcessPool need to fetch the new task
            def get_task_fn(id):
                # print(f"Getting next task by __next_task()")
                next_task = self.__next_task(id=id)
                if not next_task == None:
                    info(f"-->Run Task: {self.section_name} | {self.group_name} | {next_task}")
                
                return next_task
            
            # The callback function would be executed when receive the return value of the completed task
            def on_recv_fn(id, result):
                self.results_list.append(result)
                self.resourceMgr.release_resource(id=id)
            
            self.process_pool.init(get_task_fn=get_task_fn, on_recv_fn=on_recv_fn)
            self.process_pool.run()
        
    # def run(self):
    #     self.__init_tasks()
    #     next_param_idx, next_param = self.__next_param()
        
    #     # Single task
    #     if next_param == None:
    #         # Make task
    #         task = self.__make_task(call=self.call, param=None, async_opt=None)
    #         # Run task
    #         self.__run_task(id=0, task=task)
        
    #     # Multiple tasks
    #     while(next_param != None):
    #         # Runtime decision
    #         next_async_idx, next_async = self.__next_async()
    #         # print(f"Next Task: {next_param}, Next Async: {next_async}")
            
    #         # Make task
    #         task = self.__make_task(call=self.call, param=next_param, async_opt=next_async)
    #         # Run task
    #         self.__run_task(id=0, task=task)
    #         # Get next task
    #         next_param_idx, next_param = self.__next_param()

class TaskRunner(BaseClass):
    """
    A config is like:
    section-1: # Sequential execution
        group-1: # Asynchronous / Synchronous execution
            Call: python GAN.py # A string command | run_fn # A function callable 
            Param: {
                '-epoch': [100, 10000, 100000]
                '-decay': ['exp', 'anneal', 'static']
            }
            'Async': {
                '-gpu': [0, 1, 2, 3]
            }
        group-2:
            Call: 'python GAN2.py'
            Param: {
                '-epoch': [1002, 100002, 1000002]
                '-decay': ['exp', 'anneal', 'static']
            }
            'Async': {
                '-gpu': [0, 1, 2, 3]
            }
    section-2:

    """
    def __init__(self, config: dict) -> None:
        self.__type_check(obj=config, obj_type=dict, obj_name='config', is_allow_none=False)

        self.config = config

        # Logger
        self.logger = self._set_UtilLogger(module='Runner', submodule='TaskRunner', verbose=UtilLogger.INFO)

    def __info(self, *args, **kwargs) -> None:
        """
        Log info via `UtilLogger.info`

        :param *args args: The positional arguments of method `UtilLogger.info`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.info`
        """
        super()._info(*args, **kwargs)

    def __warning(self, *args, **kwargs) -> None:
        """
        Log warning via ``UtilLogger.warning``

        :param *args args: The positional arguments of method `UtilLogger.warning`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.warning`
        """
        super()._warning(*args, **kwargs)
        
    def __error(self, *args, **kwargs) -> None:
        """
        Log error via ``UtilLogger.error``

        :param *args args: The positional arguments of method `UtilLogger.error`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.error`
        """
        super()._error(*args, **kwargs)

    def __type_check(self, *args, **kwargs) -> None:
        """
        Type check via function ``type_check`` in module ``Util``

        :param *args args: The positional arguments of function ``type_check`` in module ``Util``
        :param **kwargs kwargs: The keyword arguments of function ``type_check`` in module ``Util``
        """
        super()._type_check(*args, **kwargs)

    def __run_tasks(self, section_name: str, group_name: str, tasks: dict) -> Process:        
        def run_group_controller(section_name: str, group_name: str, tasks: dict):
            gc = GroupController(section_name=section_name, group_name=group_name, tasks=tasks)
            gc.run()
            
        proc = Process(target=run_group_controller, args=(section_name, group_name, tasks))
        proc.start()
        
        return proc
        
    def __run_groups(self, section_name: str, groups: dict) -> None:
        if not isinstance(groups, dict):
            raise TypeError(f"Parameter 'groups' of section '{section_name}' is not a dictionary")
        
        if (groups.keys() == None) or (len(groups.keys()) == 0):
            raise ValueError(f"The parameter 'groups' of section '{section_name}' shouldn't be empty")
        
        # Run groups concurrently
        group_proc_list = []
        for group_name in groups.keys():
            print(colorize("green", f"->Start Group: {section_name} | {group_name}"))
            tasks = groups.get(group_name, None)
            new_group_proc = self.__run_tasks(section_name=section_name, group_name=group_name,tasks=tasks)
            group_proc_list.append(new_group_proc)
            delay()
            
        # Block until all processes finish
        for group_proc in group_proc_list:
            group_proc.join()
        
    def run(self) -> None:
        # if not isinstance(self.config, dict):
        #     raise TypeError(f"Config is not a dictionary")
        
        # if (self.config.keys() == None) or (len(self.config.keys()) == 0):
        #     raise ValueError(f"The config shouldn't be empty")
        
        for section_name in self.config.keys():
            print(colorize("green", f"Start Section: {section_name}"))
            groups = self.config.get(section_name, None)
            
            self.__run_groups(section_name=section_name, groups=groups)
     
# def test_run(epoch :int, decay: str, dataset_size: int, gpu: int):
#     print(f"Epoch: {epoch}, Decay: {decay}, Dataset Size: {dataset_size}, GPU: {gpu}")
    
def test_run(epoch :int, decay: str, machine: int, gpu: int, dataset_size: int):
    import os
    import jax.numpy as np
    os.environ["CUDA_VISIBLE_DEVICES"] = f'{gpu}'
    print(f"Epoch: {epoch}, Decay: {decay}, Dataset Size: {dataset_size}, Machine: {machine}, GPU: {gpu}")
    sleep(5)
       
if __name__ == '__main__':
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
            }    
        },
        'section-2': {
            'group-1': {
                'Call': 'ls',
                'Param': {
                    '': ['-a']
                }
            }
        }
    }
    
    tr = TaskRunner(config=config)
    tr.run()