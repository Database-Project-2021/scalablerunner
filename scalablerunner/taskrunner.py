import traceback
import os
from time import sleep
import types
from typing import Callable, List, Tuple, TypeVar, Union
import threading
from multiprocessing import Process, Queue

import rpyc

from scalablerunner.util import UtilLogger, BaseClass

class ProcessTask(BaseClass):
    """
    An abstract class defines the task that can be executed by ``ProcessPool``
    """
    def run(self):
        pass

class Task(ProcessTask):
    """
    An implementation of the interface ``ProcessTask`` for the ``GroupController``
    """
    def __init__(self, call: Union[str, Callable], remote_ip: str=None, remote_port: int=None) -> None:
        """
        :param Union[str, Callable] call: A callable function call or a command in string
        :param str remote_ip: The IP address of the target machine to execute the task, 
            if ``remote_ip`` is ``None``, then the task would be executed locally.
        :param int remote_port: The opening RPYC port of the target machine to connect, if ``port`` 
            is ``None``, then it use default port.
        """
        if isinstance(call, str):
           self.is_str = True
        elif callable(call):
            self.is_str = False
        else:
            raise TypeError(f"Parameter 'call' must be either string or callable")

        self.__type_check(obj=remote_ip, obj_type=str, obj_name='remote_ip', is_allow_none=True)
        self.__type_check(obj=remote_port, obj_type=int, obj_name='remote_port', is_allow_none=True)

        self.call = call
        self.param_list = []
        self.param_list_str = ''
        self.param_list_dict = {}
        self.remote_ip = remote_ip
        self.remote_port = remote_port

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
    
    def __convert_param_list_to_str(self) -> str:
        """
        Convert the parameter list into the string for command line

        returns: The options of the call in string
        :rtype: str
        """
        params = []
        for param in self.param_list:
            if param[0] == '':
                params.append(''.join([str(param[0]), str(param[1])]))
            else:
                params.append(' '.join([str(param[0]), str(param[1])]))
        return ' '.join(params)
        
    def __convert_param_list_to_dict(self) -> dict:
        """
        Convert the parameter list into the dictionary for a callable function call

        returns: The options of the call in dictionary
        :rtype: dict
        """
        params = {}
        for param in self.param_list:
            params[param[0]] = param[1]
        return params
    
    def __update_task(self):
        """
        Update the list and the dictionary of the arguments
        """
        self.param_list_str = self.__convert_param_list_to_str()
        self.param_list_dict = self.__convert_param_list_to_dict()

    def __execute_cmd(self, call: str, param: str):
        """
        Execute the task 
        """
        cmd = f"{call} {param}"
        if self.remote_ip == None:
            os.system(cmd)
        else:
            if self.remote_port == None:
                conn = rpyc.classic.connect(self.remote_ip)
            else:
                conn = rpyc.classic.connect(self.remote_ip, port=self.remote_port)
            conn.execute('import os')
            conn.eval(cmd)
            conn.close()

    def __execute_fn(self, call: Callable, param: dict):
        """
        Execute the task 
        """
        if self.remote_ip == None:
            call(**param)
        else:
            if self.remote_port == None:
                conn = rpyc.classic.connect(self.remote_ip)
            else:
                conn = rpyc.classic.connect(self.remote_ip, port=self.remote_port)
            fn = conn.teleport(call)
            fn(**param)
            conn.close()
    
    def add_arguments(self, options: list) -> None:
        """
        Add new arguments for the command or callable function call

        :param list options: The arguments for the call
        """
        self.__type_check(obj=options, obj_type=list, obj_name='options', is_allow_none=True)

        if isinstance(options, list):
            if len(options) > 0:
                self.param_list += options
                self.__update_task()

    def get(self) -> Union[Tuple[Callable, dict], Tuple[str, str]]:
        """
        Get the call and the arguments of the task

        returns: the call and the arguments of the task
        :rtype: Union[Tuple[Callable, dict], Tuple[str, str]]
        """
        if self.is_str:
            param = self.__convert_param_list_to_str()
        else:
            param = self.__convert_param_list_to_dict()
        return self.call, param
    
    def run(self) -> None:
        """
        Run the task. If the task cannot be executed, log an error.
        """
        call, param = self.get()
        if self.is_str:
            # print([call, param])
            if isinstance(param, str) and len(param) > 0:
                try:
                    # subprocess.run([call, param])
                    # os.system(f"{call} {param}")
                    self.__execute_cmd(call=call, param=param)
                except:
                    self.__error(f"Cannot execute the task: {call} {param}")
                    traceback.print_exc()
            else:   
                try: 
                    # subprocess.run([call])
                    # os.system(f"{call} {param}")
                    self.__execute_cmd(call=call, param=param)
                except:
                    self.__error(f"Cannot execute the task: {call}")
                    traceback.print_exc()
        else:
            try: 
                # call(**param)
                self.__execute_fn(call=call, param=param)
            except:
                self.__error(f"Cannot execute the task: {call.__name__}({param})")
                traceback.print_exc()
    
    def __str__(self) -> str:
        """
        Override the method of __str__
        """
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
        """
        Override the method of __repr__
        """
        return self.__str__()

class ProcessArg():
    """
    A structure of the argument of the process
    """
    def __init__(self, id: int, msg_in_queue: Queue, msg_out_queue: Queue):
        """
        :param int id: The process id
        :param ``Queue`` msg_in_queue: A message queue that the master can send message to slaves with
        :param ``Queue`` msg_out_queue: A message queue that the slaves can send message to master with
        """
        self.id = id
        self.msg_in_queue = msg_in_queue
        self.msg_out_queue = msg_out_queue
        
class ProcessPool(BaseClass):
    """
    A process pool manager
    """
    # Status msg
    RUNNING_MSG = 'ongoing'
    DONE_MSG = 'done'
    MSG_NAME = 'msg'
    TASK_NAME = 'task'
    RES_NAME = 'result'

    def __init__(self, num_process: int, delay: float=0.0):
        """
        :param int num_process: The number of the processes
        :param float delay: The waiting gap between launching processes
        """
        self.__type_check(obj=num_process, obj_type=int, obj_name='num_process', is_allow_none=False)
        self.__type_check(obj=delay, obj_type=float, obj_name='delay', is_allow_none=False)

        self.num_process = num_process
        self.delay = delay

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

    def __wait(self):
        """
        Sleep for a while between launching processes
        """
        sleep(self.delay)
        
    def init(self, get_task_fn: Callable[[int], TypeVar("T")], on_recv_fn: Callable[[TypeVar("T")], None]) -> None:
        """
        Set up the callback to get new tasks and the callback while recieve the result of the task after the task has been finished

        :param Callable[[int], T] get_task_fn: 
        :param Callable[[T], None] on_recv_fn: The callback function that would be call when the master recieve the message sent from slaves
        """

        self.__type_check(obj=get_task_fn, obj_type=callable, obj_name='get_task_fn', is_allow_none=False)
        self.__type_check(obj=on_recv_fn, obj_type=callable, obj_name='on_recv_fn', is_allow_none=False)
        
        self.get_task_fn = get_task_fn
        self.on_recv_fn = on_recv_fn
        
    def __pack_to_slave_msg(self, msg: str, task: ProcessTask) -> dict:
        """
        Pack the message from master to the slaves

        :param int msg: The message from the master to the slaves in string
        :param ProcessTask task: The task assigned to the slave
        returns: The pack of the message from the master to the slaves containing message and task
        :rtype: dict
        """
        self.__type_check(obj=msg, obj_type=str, obj_name='the message to be packed from the master to the slaves', is_allow_none=False)
        self.__type_check(obj=task, obj_type=ProcessTask, obj_name='task', is_allow_none=False)

        return {self.MSG_NAME: msg, self.TASK_NAME: task}
    
    def __unpack_to_slave_msg(self, queue_msg: dict) -> Tuple[str, ProcessTask]:
        """
        Uppack the message from the master to the slaves

        :param dict queue_msg: The message from the master to the slaves
        returns: The message from the master to the slaves containing the string message and the task
        :rtype: Tuple[str, ProcessTask]
        """
        self.__type_check(obj=queue_msg, obj_type=dict, obj_name='queue_msg', is_allow_none=False)

        msg = queue_msg.get(self.MSG_NAME, None)
        task = queue_msg.get(self.TASK_NAME, None)
        # print(f"Msg: {msg}, Task: {task}")
        
        self.__type_check(obj=msg, obj_type=str, obj_name='the unpacked message to be packed from the master to the slaves', is_allow_none=False)
        self.__type_check(obj=task, obj_type=ProcessTask, obj_name='task', is_allow_none=False)
        return msg, task
    
    def __send_to_slave(self, id: int, msg: str, task: ProcessTask) -> None:
        """
        Send the assigned tasks to the slaves from the master, used by the master

        :param int id: The ID of the slave which the task is assigned to
        :param str msg: The string message to the slaves
        :param ProcessTask task: The task that assigned to the slaves
        """
        self.__type_check(obj=id, obj_type=int, obj_name='id', is_allow_none=False)
        self.__type_check(obj=msg, obj_type=str, obj_name='the message to be packed from the master to the slaves', is_allow_none=False)
        self.__type_check(obj=task, obj_type=ProcessTask, obj_name='task', is_allow_none=False)
        # print(f"Send to Slave {id}, Msg: {msg}")
        self.send_msg_to_slave_queue_list[id].put(self.__pack_to_slave_msg(msg=msg, task=task))
        
    def __recv_from_slave(self) -> Tuple[str, object]:
        """
        Receive message and the results from the slaves, used by the master

        returns: The message from slaves to the master contains the slave process ID and the return value after the slave finished the task
        :rtype: Tuple[int, Union[object, None]]
        """
        new_res = self.recv_msg_form_slave_queue.get()
        msg, res = self.__unpack_to_master_msg(queue_msg=new_res)
        # print(f"Recv from Slave {msg}, Msg: {msg}, Result: {res}")
        self.__type_check(obj=msg, obj_type=int, obj_name='the message received from the slaves', is_allow_none=False)
        return msg, res
        
    def __pack_to_master_msg(self, msg: int, result: object) -> dict:
        """
        Pack the message from slaves to the master

        :param int msg: The message from slaves to the master in string
        :param object result: The results of the task after the slave finished the task
        returns: The message from slaves to the master contains the slave process ID and the return value after the slave finished the task
        :rtype: dict
        """
        self.__type_check(obj=msg, obj_type=int, obj_name='the message to be packed from slaves to the master', is_allow_none=False)

        return {self.MSG_NAME: msg, self.RES_NAME: result}
    
    def __unpack_to_master_msg(self, queue_msg: dict) -> Tuple[int, Union[object, None]]:
        """
        Uppack the message from slaves to the master

        :param dict queue_msg: The message from slaves to the master
        returns: The message from slaves to the master contains the slave process ID and the return value after the slave finished the task
        :rtype: Tuple[int, Union[object, None]]
        """
        self.__type_check(obj=queue_msg, obj_type=dict, obj_name='queue_msg', is_allow_none=False)

        msg = queue_msg.get(self.MSG_NAME, None)
        res = queue_msg.get(self.RES_NAME, None)
        
        self.__type_check(obj=msg, obj_type=int, obj_name='the unpacked message from slaves to the master', is_allow_none=False)
        return msg, res
        
    def __acquire_task(self, arg: ProcessArg) -> Tuple[str, ProcessTask]:
        """
        Aquire the tasks that assigned to the slave processes, used by the slaves
        msg = {
            'msg': string message
            'task': Callable function to execute
        }

        :param ``ProcessArg`` arg: The arguments of the process that wants to aquire a task
        returns: The message of the task and the task object in type of ``ProcessTask``
        :rtype: Tuple[str, ProcessTask]
        """
        self.__type_check(obj=arg, obj_type=ProcessArg, obj_name='the argument of the slave processes', is_allow_none=False)

        queue_msg = arg.msg_in_queue.get()
        return self.__unpack_to_slave_msg(queue_msg=queue_msg)
    
    def __send_res(self, arg: ProcessArg, res: object) -> None:
        """
        Send the results to the master after the slave finished the task, used by the slaves

        :param ``ProcessArg`` arg: The arguments of the process that wants to aquire a task
        :param object res: The return values of the task
        """
        self.__type_check(obj=arg, obj_type=ProcessArg, obj_name='the argument of the slave processes', is_allow_none=False)

        arg.msg_out_queue.put(self.__pack_to_master_msg(msg=arg.id, result=res))
        
    def __slave_fn(self, arg: ProcessArg) -> None:
        """
        :param ``ProcessArg`` arg: The arguments of the slave processes
        """
        self.__type_check(obj=arg, obj_type=ProcessArg, obj_name='the argument of the slave processes', is_allow_none=False)

        # Acquire new task
        msg, task = self.__acquire_task(arg=arg)
        while(msg != self.DONE_MSG):
            # Execute task
            res = task.run()
            # Send back results
            self.__send_res(arg=arg, res=res)
            # Acquire new task
            msg, task = self.__acquire_task(arg=arg)
    
    def __create_processes(self) -> None:
        """
        Create processes for the process pool
        """
        # Message queue
        self.send_msg_to_slave_queue_list = [Queue() for i in range(self.num_process)]
        self.recv_msg_form_slave_queue = Queue()
        
        self.proc_list = [Process(name=f"ProcPool.{str(i)}",
                                  target=self.__slave_fn, 
                                  args=(ProcessArg(id=i, 
                                                  msg_in_queue=self.send_msg_to_slave_queue_list[i], 
                                                  msg_out_queue=self.recv_msg_form_slave_queue), )) for i in range(self.num_process)]
    
    def run(self) -> None:
        """
        Run the process pool
        """
        # Phase 1
        self.__create_processes()
        slave_counter = 0
        new_task = self.get_task_fn(id=slave_counter)
        
        # Send one task to each slave
        while(new_task != None):
            if not isinstance(new_task, ProcessTask):
                raise TypeError(f"The task that acquired from 'self.get_task_fn' should inherit from the class 'ProcessTask'.")
            self.__send_to_slave(id=slave_counter, msg=self.RUNNING_MSG, task=new_task)
            
            slave_counter += 1
            if slave_counter >= self.num_process:
                break
            new_task = self.get_task_fn(id=slave_counter)
        
        # Start the processes
        for proc in self.proc_list:
            proc.start()
            self.__wait()
        
        # Phase 2
        # Receive results and send task repeatly
        while(True):
            # Receive results
            recv_msg, recv_res = self.__recv_from_slave()
            self.on_recv_fn(recv_msg, recv_res)
            
            # Send task to slaves
            new_task = self.get_task_fn(id=recv_msg)
            if new_task == None:
                break
            self.__send_to_slave(id=recv_msg, msg=self.RUNNING_MSG, task=new_task)
            
        # Phase 3
        # Send finished message
        for id in range(self.num_process):
            self.__send_to_slave(id=id, msg=self.DONE_MSG, task=ProcessTask())
        
        # Join processes
        for proc in self.proc_list:
            proc.join()
        
        while(not self.recv_msg_form_slave_queue.empty()):
            recv_msg, recv_res = self.__recv_from_slave()
            self.on_recv_fn(recv_msg, recv_res)

class ResourceManager(BaseClass):
    """
    Manage the resources
    """
    def __init__(self, resources: List[TypeVar('T')]) -> None:
        """
        :param list resources: The list of the resources
        """
        self.__type_check(obj=resources, obj_type=list, obj_name='resources', is_allow_none=False)

        self.share_lock = threading.Lock()
        self.resources = resources
        self.resource_occupation_list = []
        for i in range(len(resources)):
            # If no process occupation
            self.resource_occupation_list.append(None)
            
        if len(resources) <= 0:
            self.is_multi_resources = False
        else:
            self.is_multi_resources = True

    def __type_check(self, *args, **kwargs) -> None:
        """
        Type check via function ``type_check`` in module ``Util``

        :param *args args: The positional arguments of function ``type_check`` in module ``Util``
        :param **kwargs kwargs: The keyword arguments of function ``type_check`` in module ``Util``
        """
        super()._type_check(*args, **kwargs)
    
    def release_resource(self, id: int) -> None:
        """
        Release the reource of the process

        :param int id: The process id of the process which occupies the resource
        """
        self.__type_check(obj=id, obj_type=int, obj_name='id', is_allow_none=False)

        # If there are no multiple resources, just skip
        if self.is_multi_resources:
            self.share_lock.acquire(blocking=True)
            # Release resource
            for idx, occup_id in enumerate(self.resource_occupation_list):
                if id == occup_id:
                    self.resource_occupation_list[idx] = None
                    break
            self.share_lock.release()
    
    def acquire_resource(self, id: int, is_blocking: bool=True) -> Tuple[Union[int, None], Union[TypeVar('T'), None]]:
        """
        Allocate the reource for the process

        :param int id: The process id of the process which need to allocate the resource
        :param bool is_blocking: Determine whether to wait until there is free resource
        returns: The index of the resource and the resouce object that was passed into the class
        :rtype: Tuple[Union[int, None], Union[T, None]]
        """
        self.__type_check(obj=id, obj_type=int, obj_name='id', is_allow_none=False)
        self.__type_check(obj=is_blocking, obj_type=bool, obj_name='is_blocking', is_allow_none=False)

        first_loop = True
        if self.is_multi_resources:
            # If is_blocking = False, only try to get resource once. If is_blocking = False, try until it successes.
            while(is_blocking or first_loop):
                is_get_lock = self.share_lock.acquire(blocking=True)
                if is_get_lock:
                    for idx, occup_id in enumerate(self.resource_occupation_list):
                        # If no process occupation
                        if occup_id is None:
                            self.resource_occupation_list[idx] = id
                            src = self.resources[idx]
                            self.share_lock.release()
                            return idx, src
                    first_loop = False
                self.share_lock.release()
        return None, None

class GroupController(BaseClass):
    """
    The executor of a group(resource group)
    """
    # Entry keywords
    CALL_ENTRY_NAME = 'Call'
    PARAM_ENTRY_NAME = 'Param'
    ASYNC_ENTRY_NAME = 'Async'
    REMOTE_ENTRY_NAME = 'Remote'
    REMOTE_PORT_ENTRY_NAME = 'Port'
    CALLBACK_ENTRY_NAME = 'Callback'

    def __init__(self, section_name: str, group_name: str, tasks: dict, delay: float=0.0) -> None:
        """
        :param str section_name: The name of the section of the group
        :param str group_name: The name of a specific group 
        :param dict tasks: The tasks of the group
        """
        # if not isinstance(tasks, dict):
        #     raise TypeError(f"Parameter 'tasks' of section-group '{section_name} - {group_name}' is not a dictionary")
        self.__type_check(obj=section_name, obj_type=str, obj_name='section_name', is_allow_none=False)
        self.__type_check(obj=group_name, obj_type=str, obj_name='group_name', is_allow_none=False)
        self.__type_check(obj=tasks, obj_type=dict, obj_name='tasks', is_allow_none=False)
        self.__type_check(obj=delay, obj_type=float, obj_name='delay', is_allow_none=False)
        
        if (tasks.keys() == None) or (len(tasks.keys()) == 0):
            raise ValueError(f"Parameter 'tasks' of section-group '{section_name} - {group_name}' shouldn't be empty")
        
        self.section_name = section_name
        self.group_name = group_name
        self.tasks = tasks
        self.delay = delay

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
        
    def __build_combination_list(self, param_list: list, entry_idx: int=0, param_obj: list=[]) -> list:
        """
        Build the list of the combinations of the parameter grid.

        :param list param_list: The list of the parameter grid.
        :param int entry_idx: The depth(i-th entry of parameter grid) of the traversed(built) parameter entries.
        :param list param_obj: The parameter combinations of the previous parameter grid entries.
        returns: A list of all combinations below the depth of entry_idx
        :rtype: list
        """
        self.__type_check(obj=param_list, obj_type=list, obj_name='param_list', is_allow_none=False)
        
        # If the list of the parameter grid if empty
        if param_list == []:
            return []
        
        # print(f"param_list[entry_idx][0]: {param_list[entry_idx][0]}")
        # print(f"param_list[entry_idx][1]: {param_list[entry_idx][1]}")
        # print(f"entry_idx: {entry_idx}")
        
        # Retrive the list of the entry of the parameter grid
        entry_name = param_list[entry_idx][0]
        vals = param_list[entry_idx][1]
        
        self.__type_check(obj=vals, obj_type=list, obj_name='the value of the entry of the parameters', is_allow_none=False)
        
        # If the parameter grid is empty
        param_entry_num = len(param_list)
        if param_entry_num == 0:
            return None

        # List of all combinations
        param_combination_list = []
        # DFS traverse the combinations of the parameters
        for val in vals:
            # Copy the parameter combinations of previous entries
            new_param = param_obj.copy()
            new_param.append((entry_name, val))
            
            # The depth(the number of entry) of the next parameter grid entry
            next_entry_idx = entry_idx + 1
            # Check the entry exists
            if next_entry_idx < param_entry_num:
                # Traverse the next entry
                res = self.__build_combination_list(entry_idx=next_entry_idx, param_obj=new_param, param_list=param_list)
                # If the combinations of the next entry isn't None, concatenate the new combinations
                if res != None:
                    param_combination_list += res
            else:
                # Append the new parameter combinations
                param_combination_list.append(new_param)
        return param_combination_list
    
    def __build_list(self, params: dict) -> list:
        """
        Convert parameter grid(in the type of dictionary) to parameter list(in the type of list).

        :param dict params: The parameter grid.
        returns: A parameter list of the parameter grid
        :rtype: list
        """
        if isinstance(params, dict):
            return [(k, params[k]) for k in params.keys()]
        else:
            return []
    
    def __init_tasks(self) -> None:
        """
        Build the list of all parameter combinations and set up resource manager and ProcessPool
        """
        self.call = self.tasks.get(self.CALL_ENTRY_NAME, None)
        self.remote_ip = self.tasks.get(self.REMOTE_ENTRY_NAME, None)
        self.remote_port = self.tasks.get(self.REMOTE_PORT_ENTRY_NAME, None)
        if not (isinstance(self.call, str) or callable(self.call)):
            raise TypeError(f"Value of entry 'Call' of section-group '{self.section_name} - {self.group_name}' is neither a callable function nor string")
        
        # Convert parameter config to list
        self.param_list = self.__build_list(params=self.tasks.get(self.PARAM_ENTRY_NAME, None))
        self.async_list = self.__build_list(params=self.tasks.get(self.ASYNC_ENTRY_NAME, None))
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
            # self.process_pool = ProcessPool(num_process=1)
            self.is_multi_resources = False
        else:    
            self.process_pool = ProcessPool(num_process=len(self.async_combination_list), delay=self.delay)
            self.is_multi_resources = True
            
        self.results_list = []
        self.is_init_tasks = True
        # print(f"self.param_combination_list: {self.param_combination_list}")
        # print(f"self.async_combination_list: {self.async_combination_list}")
        
    def __next_param(self) -> Tuple[int, dict]:
        """
        Generate next parameter combination

        returns: Index of the returned parameter combination and the next parameter combination
        :rtype: Tuple[int, dict]
        """
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
    
    def __make_task(self, call: Union[str, Callable], param: list, async_opt: list) -> Task:
        """
        Craft a new task according to the given parameters

        :param Union[str, Callable] call: A string of command or a python function/callable
        :param list param: The list of the parameters
        :param list async_opt: The options of 'Async' entry
        returns: The ``Task`` object according to the parameters
        :rtype: ``Task``
        """
        if not self.is_init_tasks:
            raise BaseException(f"Please call method 'self.__init_tasks()' before using __next_async()")
            
        task = Task(call=call, remote_ip=self.remote_ip, remote_port=self.remote_port)
        task.add_arguments(options=param)
        task.add_arguments(options=async_opt)
        
        return task
        
    def __next_task(self, id: int, is_blocking: bool=True) -> Task:
        """
        Generate next task and allocate free resources

        :param int id: Process id of the process requiring a new task
        :param bool is_blocking: Determine to wait or not while there is no free resources
        returns: The next task in type of class ``Task``
        :rtype: ``Task``
        """
        self.__type_check(obj=id, obj_type=int, obj_name='id', is_allow_none=False)
        self.__type_check(obj=is_blocking, obj_type=bool, obj_name='is_blocking', is_allow_none=False)

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
        """
        Execute the given task and release allocated resources after the task was finished.

        :param int id: Process id of the process requiring a new task
        :param ``Task`` task: The given task need to be executed
        """
        self.__type_check(obj=id, obj_type=int, obj_name='id', is_allow_none=False)
        self.__type_check(obj=task, obj_type=Task, obj_name='task', is_allow_none=False)
        self.__info(f"Run Task: {self.section_name} | {self.group_name} | {task}")
        
        task.run()
        self.resourceMgr.release_resource(id=id)
            
    def run(self) -> None:
        """
        Execute TaskRunner according to the given configuration.
        """
        self.__init_tasks()
        if self.is_single_task:
            # print(f"Getting next task")
            next_task = self.__next_task(id=0, is_blocking=False)
            # print(f"Running task")
            self.__run_task(id=0, task=next_task)
        else:
            if self.is_multi_resources:
                # Multi-resources, multi-process
                # The callback function would be executed while the ProcessPool need to fetch the new task
                def get_task_fn(id):
                    # print(f"Getting next task by __next_task()")
                    next_task = self.__next_task(id=id)
                    if not next_task == None:
                        self.__info(f"Run Task: {self.section_name} | {self.group_name} | {next_task}")
                    
                    return next_task
                
                # The callback function would be executed when receive the return value of the completed task
                def on_recv_fn(id, result):
                    self.results_list.append(result)
                    self.resourceMgr.release_resource(id=id)
                
                # Mutliprocessing with process pool
                self.process_pool.init(get_task_fn=get_task_fn, on_recv_fn=on_recv_fn)
                self.process_pool.run()
            else:
                # Single resource, single process
                next_task = self.__next_task(id=0, is_blocking=False)
                while next_task is not None:
                    self.__run_task(id=0, task=next_task)
                    next_task = self.__next_task(id=0, is_blocking=False)

class TaskRunner(BaseClass):
    """
    The ``TaskRunner`` can execute a command in string or a Callable/function in Python. It can also allocate resources like GPUs and 
    schedule tasks to the multiple resources concurrently.

    A config is like:
    {
        section-1:{ # Sequential execution
            group-1:{ # Asynchronous / Synchronous execution
                Call: python GAN.py # A string command | run_fn # A function callable 
                Param: {
                    '-epoch': [100, 10000, 100000]
                    '-decay': ['exp', 'anneal', 'static']
                }
                'Async': {
                    '-gpu': [0, 1, 2, 3]
                }
            },
            group-2:{
                Call: 'python GAN2.py'
                Param: {
                    '-epoch': [1002, 100002, 1000002]
                    '-decay': ['exp', 'anneal', 'static']
                }
                'Async': {
                    '-gpu': [0, 1, 2, 3]
                }
            }
        },
        section-2:{...
    }
    """

    def __init__(self, config: dict, delay: float=0.0) -> None:
        """
        :param dict config: The configuration of the ``TaskRuner``
        """
        self.__type_check(obj=config, obj_type=dict, obj_name='config', is_allow_none=False)
        self.__type_check(obj=delay, obj_type=float, obj_name='delay', is_allow_none=False)

        self.config = config
        self.delay = delay

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

    def __wait(self):
        """
        Sleep for a while between launching groups
        """
        sleep(self.delay)

    def __run_group(self, section_name: str, group_name: str, tasks: dict) -> Process:
        """
        Run the tasks of specific section-group

        :param str section_name: The entry name of specific section
        :param str group_name: The group_name corresponding to the section_name
        :param dict tasks: The configurations of multiple tasks under the specific section-group
        returns: The processes that have been launched in type of ``Process``
        :rtype: ``Process``
        """
        self.__type_check(obj=section_name, obj_type=str, obj_name='section_name', is_allow_none=False)
        self.__type_check(obj=group_name, obj_type=str, obj_name=f'group_name of {section_name}', is_allow_none=False)
        self.__type_check(obj=tasks, obj_type=dict, obj_name=f'tasks of {group_name} of {section_name}', is_allow_none=False)

        def run_group_controller(section_name: str, group_name: str, tasks: dict):
            try:
                gc = GroupController(section_name=section_name, group_name=group_name, tasks=tasks, delay=self.delay)
                gc.run()
            except:
                self.__error(f"Something wrong with the section-group '{section_name} - {group_name}', fail to finish the task of the group")
                traceback.print_exc()
            
        proc = Process(name=f"{section_name}.{group_name}", target=run_group_controller, args=(section_name, group_name, tasks))
        proc.start()
        
        return proc
        
    def __run_section(self, section_name: str, groups: dict) -> None:
        """
        Run the specific group of the section

        :param str section_name: The entry name of specific section
        :param dict groups: The group corresponding to the section_name
        """
        self.__type_check(obj=section_name, obj_type=str, obj_name='section_name', is_allow_none=False)
        self.__type_check(obj=groups, obj_type=dict, obj_name=f'groups of {section_name}', is_allow_none=False)
        
        if (groups.keys() == None) or (len(groups.keys()) == 0):
            raise ValueError(f"The parameter 'groups' of section '{section_name}' shouldn't be empty")
        
        # Run groups concurrently
        group_proc_list = []
        for group_name in groups.keys():
            self.__info(f"Start Group: {section_name} | {group_name}")
            tasks = groups.get(group_name, None)

            try:
                new_group_proc = self.__run_group(section_name=section_name, group_name=group_name, tasks=tasks)
                group_proc_list.append(new_group_proc)
            except:
                self.__error(f"Something wrong with the section-group '{section_name} - {group_name}', fail to finish the tasks of the group")
                traceback.print_exc()
            self.__wait()
            
        # Block until all processes finish
        for group_proc in group_proc_list:
            group_proc.join()
        
    def run(self) -> None:
        """
        Run tasks according to the configuration
        """
        if (self.config.keys() == None) or (len(self.config.keys()) == 0):
            raise ValueError(f"The config shouldn't be empty")
        
        for section_name in self.config.keys():
            self.__info(f"Start Section: {section_name}")
            groups = self.config.get(section_name, None)
            
            try:
                self.__run_section(section_name=section_name, groups=groups)
            except:
                self.__error(f"Something wrong with the section '{section_name}', fail to finish the tasks of the section")
                traceback.print_exc()