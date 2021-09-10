from enum import Enum, auto
from time import sleep
import traceback

import paramiko
from scp import SCPClient

from src.runner.util import info, warning, error, progress, progress4, type_check, update

class RemoteType(Enum):
    SSH = auto()
    SCP = auto()
    SFTP = auto()

class SSH():
    RECONNECT_TIME_OUT = 20
    RECONNECT_COUNT = 1
    RECONNECT_WAITING = 0

    def __init__(self, hostname: str, username: str, password: str=None, port: int=22) -> None:
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.scpClient = None

    def __info(self, *args, **kwargs) -> None:
        print(f"[SSH] Info: {info(*args, **kwargs)}")

    def __warning(self, *args, **kwargs) -> None:
        print(f"[SSH] Warning: {warning(*args, **kwargs)}")
        
    def __error(self, *args, **kwargs) -> None:
        print(f"[SSH] Error: {error(*args, **kwargs)}")

    def __type_check(self, *args, **kwargs) -> None:
        type_check(*args, **kwargs)
    
    def __retrying_execution(self, remote_type: RemoteType, fn_name: str, name: str, retry_count: int, *args, **kargs):
        self.__type_check(obj=remote_type, obj_type=RemoteType, obj_name='remote_type', is_allow_none=False)
        self.__type_check(obj=fn_name, obj_type=str, obj_name='fn_name', is_allow_none=False)
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)
        
        try_counter = retry_count + 1
        is_successed = False
        res = None
        try:
            for i in range(try_counter):
                try:
                    if self.client.get_transport() is not None:
                        while not self.client.get_transport().is_active():
                            pass
                    if remote_type is RemoteType.SSH:
                        res = getattr(self.client, fn_name)(*args, **kargs)
                    elif remote_type is RemoteType.SCP:
                        res = getattr(self.scpClient, fn_name)(*args, **kargs)
                    else:
                        raise ValueError(f"No such type of operation in 'RemoteType'")
                    is_successed = True
                    break
                except:
                    is_successed = False
                    if i == 0 and try_counter > 1:
                        traceback.print_exc()
                        self.__warning(f"{name} failed, re-trying.")
                        self.reconnect()
                    elif i > 0:
                        traceback.print_exc()
                        self.__warning(f"{i}-th re-try failed.")
                        self.reconnect()
        finally:
            if is_successed:
                self.__info(f"SUCCESSED: {name}")
            else:
                self.__error(f"FAILED: {name}")
            return res

    def connect(self, timeout: int=20, retry_count: int=3):
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='connect', name="SSH connection", retry_count=retry_count, 
                                  hostname=self.hostname, port=self.port, username=self.username, password=self.password, timeout=timeout)
        # self.scpClient = SCPClient(self.client.get_transport(), progress=progress)
        self.scpClient = SCPClient(self.client.get_transport(), progress4=progress4)

    def reconnect(self):
        self.__warning(f"Reconnecting... Waiting for {self.RECONNECT_WAITING}s")
        self.close()
        sleep(self.RECONNECT_WAITING)
        self.connect(timeout=self.RECONNECT_TIME_OUT, retry_count=self.RECONNECT_COUNT)

    def close(self):
        self.client.close()

    def exec_command(self, command: str, is_show_result: bool=True, retry_count: int=3):
        self.__type_check(obj=is_show_result, obj_type=bool, obj_name='is_show_result', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        stdin, stdout, stderr = self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='exec_command', name=f"SSH command: {command}", retry_count=retry_count, command=command)

        if is_show_result:
            output = ""
            for line in stdout:
                output = output + line
            if output != "":
                print(output)

            output = ""
            for line in stderr:
                output = output + line
            if output != "":
                self.__error(f"An error occured while executing SSH remote command.")
                print(output)

        return stdin, stdout, stderr

    def put(self, files: str, remote_path: str, recursive: bool=False, preserve_times: bool=False, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='put', name='SCP put', retry_count=retry_count, files=files, 
                                      remote_path=remote_path, recursive=recursive, preserve_times=preserve_times)

    def putfo(self, fl, remote_path: str, mode: str='0644', size: int=None, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")

        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='putfo', name='SCP put byte', retry_count=retry_count, fl=fl, 
                                      remote_path=remote_path, mode=mode, size=size)

    def get(self, remote_path: str, local_path: str='', recursive: bool=False, preserve_times: bool=False, retry_count: int=3):
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        with self.scpClient as scp:
            self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='get', name='SCP get', retry_count=retry_count, remote_path=remote_path, 
                                      local_path=local_path, recursive=recursive, preserve_times=preserve_times)