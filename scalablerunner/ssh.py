from enum import Enum, auto
import os
import sys
from time import sleep
import time
import traceback
from typing import Tuple
import socket
from stat import S_IMODE, S_ISDIR, S_ISREG

import paramiko
from scp import SCPClient

from scalablerunner.util import progress, progress4, UtilLogger, BaseClass

class RemoteType(Enum):
    """
    Types of remote connection, used by class SSH.
    """
    SSH = auto()
    SCP = auto()
    SFTP = auto()
    THIS = auto()

class SSH(BaseClass):
    """
    Warpper of 'paramiko' and 'SCPClient', implement auto-retry feature to guarantee the 
        completeness of the operations while connection error occurs.
    """
    RECONNECT_TIME_OUT = 20
    RECONNECT_COUNT = 1
    RECONNECT_WAITING = 0

    # SCP settings
    SCP_BUFFER_SIZE = pow(2, 22)
    SCP_SOCKET_TIMEOUT = 60

    # Default functionalities
    DEFAULT_IS_RAISE_ERR = False
    DEFAULT_RETRY_COUNT = 3
    DEFAULT_TIMEOUT = None # 15 mins

    # Type of file transfer
    STABLE = 'stable'
    SCP = 'scp'
    SFTP = 'SFTP'

    def __init__(self, hostname: str, username: str=None, password: str=None, port: int=22) -> None:
        """
        Warpper of 'paramiko' and 'SCPClient', implement auto-retry feature to guarantee the 
            completeness of the operations while connection error occurs.

        :param str hostname: The server to connect to
        :param str username: The username to authenticate as (defaults to the current local username)
        :param int port: The server port to connect to
        :param str password: Used for password authentication; is also used for private key decryption if ``passphrase`` is not given.
        """

        paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = pow(2, 22) # 4MB per chunk

        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.default_is_raise_err = self.DEFAULT_IS_RAISE_ERR
        self.default_retry_count = self.DEFAULT_RETRY_COUNT
        self.default_timeout = self.DEFAULT_TIMEOUT

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.scpClient = None
        self.sftpClient = None
        
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

    def __success_info(self, msg: str):
        self.__info(f"SUCCESSED - {msg}")

    def __fail_info(self, msg: str):
        self.__error(f"FAILED - {msg}")

    def __type_check(self, *args, **kwargs) -> None:
        """
        Type check via function ``type_check`` in module ``Util``

        :param *args args: The positional arguments of function ``type_check`` in module ``Util``
        :param **kwargs kwargs: The keyword arguments of function ``type_check`` in module ``Util``
        """
        super()._type_check(*args, **kwargs)
    
    def __retrying_execution(self, remote_type: RemoteType, fn_name: str, name: str, retry_count: int, is_raise_err: bool, is_show_success: bool, *args, **kargs) -> Tuple:
        """
        Retrying while an error occurs

        :param RemoteType remote_type: 
        :param str fn_name: The name of the function/method that would be executed.
        :param str name: 
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :return: The return of the executed function.
        :rtype: Tuple
        """
        self.__type_check(obj=remote_type, obj_type=RemoteType, obj_name='remote_type', is_allow_none=False)
        self.__type_check(obj=fn_name, obj_type=str, obj_name='fn_name', is_allow_none=False)
        self.__type_check(obj=name, obj_type=str, obj_name='name', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)
        
        try_counter = self.__process_retry_count(retry_count=retry_count) + 1
        is_successed = False
        res = None
        try:
            for i in range(try_counter):
                try:
                    if self.client.get_transport() is not None:
                        start_time = time.time()
                        # While SCP isn't active and waiting time < 1 second, keep waiting
                        while (not self.client.get_transport().is_active()) and (time.time() - start_time < 1):
                            pass
                    if remote_type is RemoteType.SSH:
                        res = getattr(self.client, fn_name)(*args, **kargs)
                    elif remote_type is RemoteType.SCP:
                        res = getattr(self.scpClient, fn_name)(*args, **kargs)
                    elif remote_type is RemoteType.SFTP:
                        res = getattr(self.sftpClient, fn_name)(*args, **kargs)
                    elif remote_type is RemoteType.THIS:
                        res = getattr(self, fn_name)(*args, **kargs)
                    else:
                        raise ValueError(f"No such type of operation in 'RemoteType'")
                    is_successed = True
                    break
                except:
                    is_successed = False
                    if i == 0 and try_counter > 1:
                        self.__warning(f"{name} failed, retrying...")
                        traceback.print_exc()
                        self.reconnect()
                    elif i > 0:
                        self.__warning(f"{i}-th retry of {name} failed.")
                        traceback.print_exc()
                        self.reconnect()
        finally:
            if is_successed:
                if is_show_success:
                    self.__success_info(msg=name)
                    # self.__info(f"SUCCESSED - {name}")
            else:
                # self.__error(f"FAILED - {name}")
                self.__fail_info(msg=name)
                if self.__process_is_raise_err(is_raise_err=is_raise_err):
                    raise BaseException(f"Fail to execute the operation, even reconnect to the host")
            return res

    def __process_is_raise_err(self, is_raise_err: bool) -> bool:
        """
        Determine to use the new value 'is_raise_err' passed by the user or the default value.
        If the argument is ``None``, use default value instead.
        """
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        if is_raise_err is None:
            return self.default_is_raise_err
        else:
            return is_raise_err

    def __process_retry_count(self, retry_count: int) -> int:
        """
        Determine to use the new value of 'retry_count' passed by the user or the default value.
        If the argument is ``None``, use default value instead.
        """
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)

        if retry_count is None:
            return self.default_retry_count
        else:
            return retry_count

    def __process_timeout(self, timeout: int) -> int:
        """
        Determine to use the new value of 'timeout' passed by the user or the default value.
        If the argument is ``None``, use default value instead.
        """
        self.__type_check(obj=timeout, obj_type=int, obj_name='timeout', is_allow_none=True)

        if timeout is None:
            return self.default_timeout
        else:
            return timeout

    def set_default_is_raise_err(self, default_is_raise_err: bool) -> 'SSH':
        """
        Overwrite default value of ``is_raise_err`` of this instance.

        :param bool default_is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        """
        self.__type_check(obj=default_is_raise_err, obj_type=bool, obj_name='default_is_raise_err', is_allow_none=False)

        self.default_is_raise_err = default_is_raise_err
        return self

    def set_default_retry_count(self, default_retry_count: bool) -> 'SSH':
        """
        Overwrite default value of ``retry_count`` of this instance.

        :param int default_retry_count: Determine the default retry-count of the class SSH.
        """
        self.__type_check(obj=default_retry_count, obj_type=int, obj_name='default_retry_count', is_allow_none=False)

        self.default_retry_count = default_retry_count
        return self

    def set_default_timeout(self, default_timeout: int) -> 'SSH':
        """
        Overwrite default value of ``timeout`` of this instance. The default value is 900 secs, which is 15 mins.

        :param int default_timeout: Determine the default time-limit of the class SSH. The time-limit of an operation including 
            ``exec_command``, ``get``, ``put``...etc means the maximum execution time in seconds of an operation.
        """
        self.__type_check(obj=default_timeout, obj_type=int, obj_name='default_timeout', is_allow_none=False)

        self.default_timeout = default_timeout
        return self

    def connect(self, timeout: int=20, retry_count: int=None, is_raise_err: bool=None) -> None:
        """
        Establish the connection to host.

        :param float timeout: An optional timeout (in seconds) for the TCP connect
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        """
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='connect', name=f"SSH connect to '{self.hostname}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                  hostname=self.hostname, port=self.port, username=self.username, password=self.password, timeout=timeout)
        # self.scpClient = SCPClient(self.client.get_transport(), buff_size=self.SCP_BUFFER_SIZE, socket_timeout=self.SCP_SOCKET_TIMEOUT, progress=progress)
        self.scpClient = SCPClient(self.client.get_transport(), buff_size=self.SCP_BUFFER_SIZE, socket_timeout=self.SCP_SOCKET_TIMEOUT, progress4=progress4)
        self.sftpClient = self.client.open_sftp()

    def reconnect(self, timeout: int=20, retry_count: int=None, is_raise_err: bool=None) -> None:
        """
        Close the old SSH connection and establish a new one.

        :param float timeout: An optional timeout (in seconds) for the TCP connect
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        """
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__warning(f"Reconnecting... Waiting for {self.RECONNECT_WAITING}s")
        self.close()
        sleep(self.RECONNECT_WAITING)
        self.connect(timeout=timeout, retry_count=retry_count, is_raise_err=is_raise_err)

    def is_active(self) -> bool:
        """
        Return true if this session is active (open).

        :return: Whether the session is active
        :rtype: bool
        """
        if self.client.get_transport() is not None:
            return self.client.get_transport().is_active()
        else:
            return False

    def close(self, retry_count: int=None, is_raise_err: bool=None) -> None:
        """
        Close the connection.

        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs.
        """
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__retrying_execution(remote_type=RemoteType.SFTP, fn_name='close', name=f"SFTP closes the connecttion to '{self.hostname}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True)
        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='close', name=f"SCP closes the connecttion to '{self.hostname}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True)
        self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='close', name=f"SSH closes the connecttion to '{self.hostname}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True)

    def exec_command(self, command: str, bufsize: int=-1, timeout: int=None, get_pty: bool=False, environment: dict=None, 
                     is_show_result: bool=True, retry_count: int=None, cmd_retry_count: int=2, is_raise_err: int=None) -> Tuple[paramiko.channel.ChannelStdinFile, paramiko.channel.ChannelFile, paramiko.channel.ChannelStderrFile]:
        """
        Execute the command on the remote host. It's an wrapper of [paramiko.client.SSHClient.exec_command](http://docs.paramiko.org/en/stable/api/client.html#paramiko.client.SSHClient.exec_command)

        :param str command: The command would be executed on the remote host
        :param int bufsize: interpreted the same way as by the built-in ``file()`` function in Python
        :param int timeout: Set command's channel timeout. See `.Channel.settimeout`
        :param bool get_pty: Request a pseudo-terminal from the server (default ``False``). See `.Channel.get_pty`
        :param dict environment: A dict of shell environment variables, to be merged into 
            the default environment that the remote command executes within.
        :param str is_show_result: An indicater decide whether to print the result of the command or not.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param int cmd_retry_count: How many time of redoing command while an error occurs 
            during execute the command on the remote machine, excluding the connection error.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: list, list, list
        """
        self.__type_check(obj=is_show_result, obj_type=bool, obj_name='is_show_result', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=cmd_retry_count, obj_type=int, obj_name='cmd_retry_count', is_allow_none=False)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        cmd_retry_counter = 0
        stdin, stdout, stderr = None, None, None
        is_successsed = True
        op_name = f"SSH execute command '{command}'"
        # CMD-Retry count exclude the first time exceution
        while cmd_retry_counter <= cmd_retry_count:
            stdin, stdout, stderr = self.__retrying_execution(remote_type=RemoteType.SSH, fn_name='exec_command', name=op_name, retry_count=retry_count, is_raise_err=False, is_show_success=False, 
                                                              command=command, bufsize=bufsize, timeout=self.__process_timeout(timeout=timeout), get_pty=get_pty, environment=environment)
            # Stdout
            output = ""
            try:
                for line in stdout:
                    output = output + line
            except socket.timeout:
                # Catch timeout error
                is_successsed = False
                self.__warning(f"SSH remote command timeout: {command}")
            except:
                # Catch other errors
                is_successsed = False
                self.__warning(f"An error occured while reading stdout returned from SSH remote command: {command}")
                traceback.print_exc()
            finally:
                if output != "":
                    if is_show_result:
                        print(output)

            # Stderr
            output = ""
            try:
                for line in stderr:
                    output = output + line
            except:
                is_successsed = False
                self.__warning(f"An error occured while reading stderr returned from SSH remote command: {command}")
                traceback.print_exc()
            finally:
                if output != "":
                    is_successsed = False
                    self.__warning(f"An error occured while executing SSH remote command: {command}")
                    if is_show_result:
                        print(output)

            if is_successsed:
                # Check whether the command successed or not
                self.__success_info(msg=op_name)
                break
            else:
                # Increase the counter of CMD-Retry-Count
                if cmd_retry_counter == 0 and cmd_retry_count > 0:
                    self.__warning(f"{op_name} failed, redoing...")
                if cmd_retry_counter > 0:
                    self.__warning(f"{cmd_retry_counter}-th redo of {op_name} failed")
                cmd_retry_counter += 1
        # Determeine whether to raise error or not
        if not is_successsed:
            self.__fail_info(msg=op_name)
            if self.__process_is_raise_err(is_raise_err=is_raise_err):
                raise BaseException(f"Fail to execute command on the remote host")

        return stdin, stdout, stderr, is_successsed

    def put(self, files: str, remote_path: str='.', recursive: bool=False, preserve_times: bool=False, retry_count: int=None, is_raise_err: int=None) -> None:
        """
        Transfer files and directories to remote host. It's an wrapper of [scp.SCPClient.put](https://github.com/jbardin/scp.py/blob/master/scp.py#L151)

        :param str files: A single path, or a list of paths to be transferred. `recursive` must be True to transfer directories.
        :param str remote_path: path in which to receive the files on the remote host. defaults to '.'
        :param bool recursive: Transfer files and directories recursively
        :param bool preserve_times: Preserve mtime and atime of transferred files and directories.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='put', name=f"SCP put files from '{files}' to '{remote_path}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                  files=files, remote_path=remote_path, recursive=recursive, preserve_times=preserve_times)

    def putfo(self, fl, remote_path: str, mode: str='0644', size: int=None, retry_count: int=None, is_raise_err: int=None):
        """
        Transfer file-like object to remote host. It's an wrapper of [scp.SCPClient.putfo](https://github.com/jbardin/scp.py/blob/master/scp.py#L189)

        :param file-like object fl: opened file or file-like object to copy
        :param str remote_path: full destination path
        :param str mode: permissions (posix-style) for the uploaded file
        :param int size: size of the file in bytes. If ``None``, the size will be computed using `seek()` and `tell()`.
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")

        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__retrying_execution(remote_type=RemoteType.SCP, fn_name='putfo', name=f"SCP put bytes to '{remote_path}'", 
                                  retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                  fl=fl, remote_path=remote_path, mode=mode, size=size)

    def __recursive_get(self, remote_path: str, local_path: str, fn_transfer: callable, **kwargs):
        """
        Traverse the ``remote_path`` recursively and copy the whole folder of ``remote_path`` to ``local_path``. 
            Refers to [StackOverflow](https://stackoverflow.com/questions/56268940/sftp-copy-download-all-files-in-a-folder-recursively-from-remote-server)

        :param str remote_path: path to retrieve from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        :param str local_path: path in which to receive files locally
        :param callable fn_transfer: the callable function that can transfer remote file to local and it should be in the form of 
            ``fn_transfer(remote_file, local_file)``
        """
        # Make root directory of the folder on the local host
        remote_dir_name = os.path.basename(remote_path)
        local_dir_path = os.path.join(local_path, remote_dir_name)
        if not os.path.exists(local_dir_path):
            os.mkdir(local_dir_path)

        def recursive_traverse(remote_path_fn, local_dir_path_fn):
            # Traverse the directory recursively and transfer
            for entry in self.sftpClient.listdir(remote_path_fn):
                # Traverse each entry
                entry_remote_path = os.path.join(remote_path_fn, entry)
                entry_local_path = os.path.join(local_dir_path_fn, entry)
                # Get the type of the entry
                mode = self.sftpClient.stat(entry_remote_path).st_mode
                if S_ISDIR(mode):
                    # Handle folder
                    try:
                        if not os.path.exists(entry_local_path):
                            os.mkdir(entry_local_path)
                    except OSError:     
                        pass
                    # Traverse the deeper folder
                    recursive_traverse(entry_remote_path, entry_local_path)
                elif S_ISREG(mode):
                    # Handle file
                    fn_transfer(entry_remote_path, entry_local_path, **kwargs)

        recursive_traverse(remote_path_fn=remote_path, local_dir_path_fn=local_dir_path)
                
    def _sftp_get_stable(self, remote_path: str, local_path: str, recursive: bool) -> None:
        """
        It opens the file on the remote directly vis [paramiko.sftp_client.SFTPClient.file](http://docs.paramiko.org/en/stable/api/sftp.html#paramiko.sftp_client.SFTPClient.file) 
            and transfer the whole file object. In our experience, this way is more stable and less error than other 2 ways.

        :param str remote_path: path to retrieve from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        :param str local_path: path in which to receive files locally
        :param bool recursive: transfer files and directories recursively
        """
        def fn_transfer(remote_path_fn, local_path_fn):
            def progress(sent, size):
                rate = float(sent)/float(size)
                is_finished = False
                if rate >= 1:
                    is_finished = True
                    rate = 1.00
                sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (self.hostname, self.port, remote_path_fn, rate*100))
                if is_finished:
                    sys.stdout.write("\n")

            with self.sftpClient.file(remote_path_fn, mode='rb') as rem_file:
                rem_file.MAX_REQUEST_SIZE = 1024
                chunck_size = 4096
                local_file_path = local_path_fn
                # if os.path.isfile(local_path_fn):
                #     remote_file_name = os.path.basename(remote_path_fn)
                #     local_file_path = os.path.join(local_path_fn, remote_file_name)
                with open(local_file_path, 'wb') as f:
                    # f.write(rem_file.read())
                    size = self.sftpClient.stat(remote_path_fn).st_size
                    sent = 0
                    byte = rem_file.read(chunck_size)
                    while byte != b"":
                        f.write(byte)
                        byte = rem_file.read(chunck_size)
                        sent += chunck_size
                        progress(sent, size)

        if recursive:
            self.__recursive_get(remote_path=remote_path, local_path=local_path, fn_transfer=fn_transfer)
        else:
            fn_transfer(remote_path_fn=remote_path, local_path_fn=local_path)

    def _sftp_get(self, remote_path: str, local_path: str, recursive: bool) -> None:
        """
        It's an wrapper of [paramiko.sftp_client.SFTPClient.get](https://github.com/jbardin/scp.py/blob/master/scp.py#L216) 
            and we also support transfering a nested folder by setting the parameter ``recursive`` as ``True``.

        :param str remote_path: path to retrieve from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        :param str local_path: path in which to receive files locally
        :param bool recursive: transfer files and directories recursively
        """
        def fn_transfer(remote_path_fn, local_path_fn):
            def progress(sent, size):
                rate = float(sent)/float(size)
                sys.stdout.write("(%s:%s) %s's progress: %.2f%%   \r" % (self.hostname, self.port, remote_path_fn, rate*100))
                if rate >= 1:
                    sys.stdout.write("\n")
            self.sftpClient.get(remote_path_fn, local_path_fn, callback=progress)

        if recursive:
            self.__recursive_get(remote_path=remote_path, local_path=local_path, fn_transfer=fn_transfer)
        else:
            fn_transfer(remote_path_fn=remote_path, local_path_fn=local_path)

    def get(self, remote_path: str, local_path: str='', recursive: bool=False, preserve_times: bool=False, mode: str='scp', retry_count: int=None, is_raise_err: int=None):
        """
        Transfer files and directories from remote host to localhost. There are 3 mode to transfer files: ``SSH.SCP``, ``SSH.STABLE``, ``SSH.SFTP``.
        The mode ``SSH.SCP`` is an wrapper of [scp.SCPClient.get](https://github.com/jbardin/scp.py/blob/master/scp.py#L216)
        The mode ``SSH.STABLE`` open the file on the remote directly vis [paramiko.sftp_client.SFTPClient.file](http://docs.paramiko.org/en/stable/api/sftp.html#paramiko.sftp_client.SFTPClient.file) 
            and transfer the whole file object. In our experience, this way is more stable and less error than other 2 ways.
        The mode ``SSH.SFTP`` is an wrapper of [paramiko.sftp_client.SFTPClient.get](https://github.com/jbardin/scp.py/blob/master/scp.py#L216) 
            and we also support transfering a nested folder by setting the parameter ``recursive`` as ``True``.

        :param str remote_path: path to retrieve from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        :param str local_path: path in which to receive files locally
        :param bool recursive: transfer files and directories recursively
        :param bool preserve_times: preserve mtime and atime of transferred files
            and directories.
        :param str mode: the way to transfer the folder/files and there are 3 choices: ``SSH.SCP``(default), ``SSH.STABLE`` and ``SSH.SFTP``
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.scpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=mode, obj_type=str, obj_name='mode', is_allow_none=True)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        if mode == self.STABLE:
            remote_type = RemoteType.THIS
            fn_name = '_sftp_get_stable'
            name=f"STABLE-SFTP get files from '{remote_path}' to '{local_path}'"

            self.__retrying_execution(remote_type=remote_type, fn_name=fn_name, name=name, 
                                      retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                      remote_path=remote_path, local_path=local_path, recursive=recursive)
        elif mode == self.SFTP:
            remote_type = RemoteType.THIS
            fn_name = '_sftp_get'
            name=f"SFTP get files from '{remote_path}' to '{local_path}'"

            self.__retrying_execution(remote_type=remote_type, fn_name=fn_name, name=name, 
                                      retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                      remote_path=remote_path, local_path=local_path, recursive=recursive)
        else:
            remote_type = RemoteType.SCP
            fn_name = 'get'
            name=f"SCP get files from '{remote_path}' to '{local_path}'"

            self.__retrying_execution(remote_type=remote_type, fn_name=fn_name, name=name, 
                                      retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                      remote_path=remote_path, local_path=local_path, recursive=recursive, preserve_times=preserve_times)

    def _sftp_put_stable(self, files: str, remote_path: str, recursive: bool) -> None:
        """
        Transfer the *single* file to remote host. Trandfer the ``files`` to the file of the ``remote_path`` on the host.
        The recursive transmission is under construction.

        :param str files: A single path, or a list of paths to be transferred. `recursive` must be True to transfer directories.
        :param str remote_path: path in which to receive the files on the remote host. defaults to '.'
        :param bool recursive: Transfer files and directories recursively
        """
        with self.sftpClient.file(remote_path, mode='w') as rem_file:
            rem_file.MAX_REQUEST_SIZE = 1024
            with open(files, 'rb') as f:
                rem_file.write(f.read())        

    def large_put(self, files: str, remote_path: str, recursive: bool=False, retry_count: int=None, is_raise_err: int=None) -> None:
        """
        Transfer the *single* file to remote host. Trandfer the ``files`` to the file of the ``remote_path`` on the host.
        The recursive transmission is under construction.

        :param str files: A single path, or a list of paths to be transferred. `recursive` must be True to transfer directories.
        :param str remote_path: path in which to receive the files on the remote host. defaults to '.'
        :param bool recursive: Transfer files and directories recursively
        :param int retry_count: How many time of reconnection and redoing the command 
            while an connection error occurs, like SSH tunnel disconect accidently, session not active...
            If value is ``None``, use default retry-count.
        :param bool is_raise_err: Determine whether to throw an error or just show in log then keep going while an error occurs. 
            Default value is None, means same as default setting. You can pass true/false to overwrite the default one, 
            but the modification only affect to this function.
        :raises BaseException: if the connection hasn't been established yet
        """
        if self.sftpClient is None:
            raise BaseException(f"Please establish a SSH connection at first.")
        
        self.__type_check(obj=recursive, obj_type=bool, obj_name='recursive', is_allow_none=False)
        self.__type_check(obj=retry_count, obj_type=int, obj_name='retry_count', is_allow_none=True)
        self.__type_check(obj=is_raise_err, obj_type=bool, obj_name='is_raise_err', is_allow_none=True)

        self.__retrying_execution(remote_type=RemoteType.THIS, fn_name='_sftp_put_stable', name=f"custome put files from '{files}' to '{remote_path}'", 
                                retry_count=retry_count, is_raise_err=is_raise_err, is_show_success=True, 
                                files=files, remote_path=remote_path, recursive=recursive)