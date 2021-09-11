from enum import Enum, auto
from time import sleep
import time
import traceback
from typing import Tuple

import paramiko
from scp import SCPClient

from runner.util import progress, progress4, UtilLogger, BaseClass

class RemoteType(Enum):
    """
    Types of remote connection, used by class SSH.
    """
    SSH = auto()
    SCP = auto()
    SFTP = auto()

class SSH(BaseClass):
    """
    Warpper of 'paramiko' and 'SCPClient', implement auto-retry feature to guarantee the 
        completeness of the operations while connection error occurs.
    """
    RECONNECT_TIME_OUT = 20
    RECONNECT_COUNT = 1
    RECONNECT_WAITING = 0

    def __init__(self, hostname: str, username: str=None, password: str=None, port: int=22, default_is_raise_err: bool=False) -> None:
        """
        :param str hostname: The server to connect to
        :param str username: The username to authenticate as (defaults to the current local username)
        :param int port: The server port to connect to
        :param str password: Used for password authentication; is also used for private key decryption if ``passphrase`` is not given.
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.is_raise_err = default_is_raise_err

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.scpClient = None
        
        # Logger
        self.logger = self._set_UtilLogger(module='Runner', submodule='SSH', verbose=UtilLogger.INFO)

    def __info(self, *args, **kwargs) -> None:
        """
        Log info via `UtilLogger.info`
        :param *args args: The positional arguments of method `UtilLogger.info`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.info`
        """
        super()._info(*args, **kwargs)

    def __warning(self, *args, **kwargs) -> None:
        """
        Log warning via `UtilLogger.warning`
        :param *args args: The positional arguments of method `UtilLogger.warning`
        :param **kwargs kwargs: The keyword arguments of method `UtilLogger.warning`
        """
        super()._warning(*args, **kwargs)
        
    def __error(self, *args, **kwargs) -> None:
        super()._error(*args, **kwargs)

    def __type_check(self, *args, **kwargs) -> None:
        super()._type_check(*args, **kwargs)
    
    def __retrying_execution(self, remote_type: RemoteType, fn_name: str, name: str, retry_count: int, is_raise_err: bool, *args, **kargs):
        self.__type_check(obj=remote_type, obj_type=RemoteType, obj_name='remote_type', is_allow_none=False)
        self.__type_check(obj=fn_name, obj_type=str, obj_name='fn_name', is_allow_none=False)
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=False)
        
        try_counter = retry_count + 1
        is_successed = False
        res = None
        try:
            for i in range(try_counter):
                try:
                    if self.client.get_transport() is not None:
                        start_time = time.time()
                        # While SCP isn't active and waiting time < 1 microsecond, keep waiting
                        while (not self.client.get_transport().is_active()) and (time.time() - start_time < 1):
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
                self.__info(f"SUCCESSED - {name}")
            else:
                self.__error(f"FAILED - {name}")
                if is_raise_err:
                    raise BaseException(f"Fail to execute command, even reconnect to the host")
            return res

    def __process_is_raise_err(self, is_raise_err: bool):
        if is_raise_err is None:
            return self.is_raise_err
        else:
            return is_raise_err

    def set_default_is_raise_err(self, is_raise_err: bool):
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=False)
        self.is_raise_err = is_raise_err

    def connect(self, timeout: int=20, retry_count: int=3, is_raise_err: bool=None) -> None:
        """
        Establish the connection to host.

        :param float timeout: An optional timeout (in seconds) for the TCP connect
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        """
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='connect', name=f"SSH connect to '{self.hostname}'", retry_count=retry_count, 
                                  is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err), 
                                  hostname=self.hostname, port=self.port, username=self.username, password=self.password, timeout=timeout)
        # self.scpClient = SCPClient(self.client.get_transport(), progress=progress)
        self.scpClient = SCPClient(self.client.get_transport(), progress4=progress4)

    def reconnect(self, timeout: int=20, retry_count: int=3, is_raise_err: bool=None) -> None:
        """
        Close the old SSH connection and establish a new one.
        """
        self.__warning(f"Reconnecting... Waiting for {self.RECONNECT_WAITING}s")
        self.close()
        sleep(self.RECONNECT_WAITING)
        self.connect(timeout=timeout, retry_count=retry_count, is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err))

    def close(self, retry_count: int=3, is_raise_err: bool=None) -> None:
        """
        Close the connection.

        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        """
        # self.client.close()
        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='close', name=f"SSH closes the connecttion to '{self.hostname}'", retry_count=retry_count, is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err))

    def exec_command(self, command: str, bufsize: int=-1, timeout: int=None, get_pty: bool=False, environment: dict=None, 
                     is_show_result: bool=True, retry_count: int=3, cmd_retry_count: int=2, is_raise_err: int=None) -> Tuple[paramiko.channel.ChannelStdinFile, paramiko.channel.ChannelFile, paramiko.channel.ChannelStderrFile]:
        """
        Execute the command on the remote host.

        :param str command: The command would be executed on the remote host
        :param int bufsize: interpreted the same way as by the built-in ``file()`` function in Python
        :param int timeout: Set command's channel timeout. See `.Channel.settimeout`
        :param bool get_pty: Request a pseudo-terminal from the server (default ``False``). See `.Channel.get_pty`
        :param dict environment: A dict of shell environment variables, to be merged into 
            the default environment that the remote command executes within.
        :param str is_show_result: An indicater decide whether to print the result of the command or not.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        :param int cmd_retry_count: How many time of redoing command while an error occurs 
            during execute the command on the remote machine, excluding the connection error.
        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: list, list, list
        """
        self.__type_check(obj=is_show_result, obj_type=bool, obj_name='is_show_result', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        cmd_retry_counter = 0
        is_successsed = False
        while cmd_retry_counter < cmd_retry_count:
            stdin, stdout, stderr = self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='exec_command', name=f"SSH execute command '{command}'", retry_count=retry_count, is_raise_err=False,
                                                              command=command, bufsize=bufsize, timeout=timeout, get_pty=get_pty, environment=environment)

            # Stdout
            output = ""
            for line in stdout:
                output = output + line
            if output != "":
                if is_show_result:
                    print(output)
            # Stderr
            output = ""
            for line in stderr:
                output = output + line
            if output != "":
                self.__error(f"An error occured while executing SSH remote command.")
                if is_show_result:
                    print(output)
                cmd_retry_counter += 1
            else:
                is_successsed = True
                break

        if self.__process_is_raise_err(is_raise_err=is_raise_err) and (not is_successsed):
            raise BaseException(f"Fail to execute command on the remote host")

        return stdin, stdout, stderr, is_successsed

    def put(self, files: str, remote_path: str='.', recursive: bool=False, preserve_times: bool=False, retry_count: int=3, is_raise_err: int=None) -> None:
        """
        Transfer files and directories to remote host.

        :param str files: A single path, or a list of paths to be transferred. `recursive` must be True to transfer directories.
        :param str remote_path: path in which to receive the files on the remote host. defaults to '.'
        :param bool recursive: Transfer files and directories recursively
        :param bool preserve_times: Preserve mtime and atime of transferred files and directories.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='put', name=f"SCP put files from '{files}' to '{remote_path}'", 
                                  retry_count=retry_count, is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err), 
                                  files=files, remote_path=remote_path, recursive=recursive, preserve_times=preserve_times)

    def putfo(self, fl, remote_path: str, mode: str='0644', size: int=None, retry_count: int=3, is_raise_err: int=None):
        """
        Transfer file-like object to remote host.

        :param file-like object fl: opened file or file-like object to copy
        :param str remote_path: full destination path
        :param str mode: permissions (posix-style) for the uploaded file
        :param int size: size of the file in bytes. If ``None``, the size will be computed using `seek()` and `tell()`.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")

        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='putfo', name=f"SCP put bytes to '{remote_path}'", 
                                  retry_count=retry_count, is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err), 
                                  fl=fl, remote_path=remote_path, mode=mode, size=size)

    def get(self, remote_path: str, local_path: str='', recursive: bool=False, preserve_times: bool=False, retry_count: int=3, is_raise_err: int=None):
        """
        Transfer files and directories from remote host to localhost.

        :param str remote_path: path to retrieve from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        :param str local_path: path in which to receive files locally
        :param bool recursive: transfer files and directories recursively
        :param bool preserve_times: preserve mtime and atime of transferred files
            and directories.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=False)

        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='get', name=f"SCP get files from '{remote_path}' to '{local_path}'", 
                                  retry_count=retry_count, is_raise_err=self.__process_is_raise_err(is_raise_err=is_raise_err), 
                                  remote_path=remote_path, local_path=local_path, recursive=recursive, preserve_times=preserve_times)