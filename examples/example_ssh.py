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