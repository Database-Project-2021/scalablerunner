from typing import Callable

from scalablerunner.dbrunner import DBRunner
from cost_estimator import Loader

class DBRunnerAdapter(DBRunner):
    """
    An adapter layer to provide an interface for ``DBRunner`` and ``TaskRunner``
    """
    def __init__(self, reports_path: str, workspace: str='db_runner_workspace_adapter'):
        """
        An adapter layer to provide an interface for ``DBRunner`` and ``TaskRunner``

        :param str reports_path: The download path of the reports on the local host
        :param str workspace: Customize the name of the workspace of the ``DBRunner``. If you need to run 
            multiple task including benchmark, load test-bed etc concurrently, you need to ensure that 
            ensure that each task executed concurrently has an individual workspace with different names.
        """
        self.__type_check(obj=reports_path, obj_type=str, obj_name='reports_path', is_allow_none=False)
        self.__type_check(obj=workspace, obj_type=str, obj_name='workspace', is_allow_none=False)
        # self.__type_check(obj=log_file, obj_type=str, obj_name='log_file', is_allow_none=True)

        super().__init__(workspace=workspace)
        self.reports_path = reports_path
        # self.dr = DBRunner(workspace)
        # if log_file is not None:
        #     self.output_log(file_name=log_file)

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

    def __update_config(self, config: dict, kwargs: dict):
        """
        Update configuration dictionary of the argument ``config`` with ``kwargs``
        
        :param dict config: The configuration to be modified.
        :param dict kwargs: The keyword arguments to be modified.
        """
        if kwargs is None:
            return config

        if not isinstance(config, dict):
            config = {}

        # print(f"kwargs: {kwargs}")
        # print(f"kwargs.items(): {kwargs.items()}")
        for k, v in kwargs.items():
            # print(f"v: {v}")
            config[v[0]] = {v[1]: v[2]}

        return config

    def config(self, server_count: int=None, sequencer: str=None, servers: list=None, clients: list=None, package_path: str=None, 
               server_client_ratio: float=None, max_server_per_machine: int=None, max_client_per_machine: int=None, jar_dir: str='latest', 
               user_name: str=None, remote_work_dir: str=None, dir_name: str=None, base_config: str=None, alts: dict=None):
        """
        Configurate ``bencher.toml`` and the section ``auto_bencher`` of the configuration ``load.toml``

        :param str sequencer: The IP address of the sequencer.
        :param list servers: The list of IP addresses of the servers.
        :param list clients: The list of IP addresses of the clients.
        :param str package_path: The path of the JDK package.
        :param str jar_dir: The directory of the .jar file of the database.
        :param int server_count: The number of the database server instances.
        :param float server_client_ratio: The ratio of the number of the client divided by the number of the server, 
            that is client / server.
        :param int max_server_per_machine: The maximum number of the server instance per machine.
        :param int max_client_per_machine: The maximum number of the client instance per machine.
        :param str user_name: The login username of the .
        :param str remote_work_dir: The working directory of the remote host.
        :param str dir_name: The directory of the JDK package.
        :param str base_config: The path of the base configuration of ``bencher.toml``.
        :param dict alts: The alternative options of configuration.
        """
        self.__type_check(obj=server_count, obj_type=int, obj_name='server_count', is_allow_none=True)
        self.__type_check(obj=sequencer, obj_type=str, obj_name='sequencer', is_allow_none=True)
        self.__type_check(obj=servers, obj_type=list, obj_name='servers', is_allow_none=True)
        self.__type_check(obj=clients, obj_type=list, obj_name='clients', is_allow_none=True)
        self.__type_check(obj=package_path, obj_type=str, obj_name='package_path', is_allow_none=True)
        self.__type_check(obj=server_client_ratio, obj_type=float, obj_name='server_client_ratio', is_allow_none=True)
        self.__type_check(obj=max_server_per_machine, obj_type=int, obj_name='max_server_per_machine', is_allow_none=True)
        self.__type_check(obj=max_client_per_machine, obj_type=int, obj_name='max_client_per_machine', is_allow_none=True)
        self.__type_check(obj=jar_dir, obj_type=str, obj_name='jar_dir', is_allow_none=True)
        self.__type_check(obj=user_name, obj_type=str, obj_name='user_name', is_allow_none=True)
        self.__type_check(obj=remote_work_dir, obj_type=str, obj_name='remote_work_dir', is_allow_none=True)
        self.__type_check(obj=dir_name, obj_type=str, obj_name='dir_name', is_allow_none=True)
        self.__type_check(obj=base_config, obj_type=str, obj_name='base_config', is_allow_none=True)
        self.__type_check(obj=alts, obj_type=dict, obj_name='alts', is_allow_none=True)

        self.config_bencher(sequencer=sequencer, servers=servers, clients=clients, package_path=package_path, 
                            user_name=user_name, remote_work_dir=remote_work_dir, dir_name=dir_name, base_config=base_config, alts=alts)
        self.config_cluster(server_count=server_count, server_client_ratio=server_client_ratio, max_server_per_machine=max_server_per_machine, max_client_per_machine=max_client_per_machine, jar_dir=jar_dir)

    def init_autobencher(self, server_jar: str, client_jar: str):
        """
        Initialize ``auto-bencher``

        :param str server_jar: The path of the ``server.jar``
        :param str client_jar: The path of the ``client.jar``
        """
        self.__type_check(obj=server_jar, obj_type=str, obj_name='server_jar', is_allow_none=False)
        self.__type_check(obj=client_jar, obj_type=str, obj_name='client_jar', is_allow_none=False)

        # If call is_active(), it will hang. Don't know why
        # if not self.is_active():
        #     self.close()

        self.reconnect()
        self.init()
        self.upload_jars(server_jar=server_jar, client_jar=client_jar, use_stable=True)
        self.close()

    def load_test_bed(self, alts: dict=None, base_config: str=None, **kwargs):
        """
        Load test bed for database.

        :param dict alts: The modification would be applied to ``base_config``
        :param str base_config: The path of the load-config for Auto-Bencher and it would be modified by ``alts``
        """
        self.__type_check(obj=alts, obj_type=dict, obj_name='alts', is_allow_none=True)
        self.__type_check(obj=base_config, obj_type=str, obj_name='base_config', is_allow_none=True)

        # If call is_active(), it will hang. Don't know why
        # if not self.is_active():
        #     self.close()

        self.reconnect()
        alts = self.__update_config(config=alts, kwargs=kwargs)
        self.load(is_kill_java=True, alts=alts, base_config=base_config)
        self.close()

    def init_autobencher_load_test_bed(self, server_jar: str, client_jar: str, base_config: str=None, alts: dict=None, **kwargs):
        """
        Initialize ``auto-bencher`` and load test bed for database.

        :param str server_jar: The path of the ``server.jar``
        :param str client_jar: The path of the ``client.jar``
        :param dict alts: The modification would be applied to ``base_config``
        :param str base_config: The path of the load-config for Auto-Bencher and it would be modified by ``alts``
        :param **kwargs kwargs: The custom parameters of the configuration that want to modify. Each pair of custom parameter, should be a list in the format arbitrary_name=[Class, Parameter name, Value]
            , like custom_param1=["elasqlbench", "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE", "1"].
        """
        self.__type_check(obj=server_jar, obj_type=str, obj_name='server_jar', is_allow_none=False)
        self.__type_check(obj=client_jar, obj_type=str, obj_name='client_jar', is_allow_none=False)
        self.__type_check(obj=alts, obj_type=dict, obj_name='alts', is_allow_none=True)
        self.__type_check(obj=base_config, obj_type=str, obj_name='base_config', is_allow_none=True)

        self.init_autobencher(server_jar=server_jar, client_jar=client_jar)
        # Unpack kwargs. If use kwargs=kwargs, it would pass adict {kwargs: ...} into function
        self.load_test_bed(alts=alts, base_config=base_config, **kwargs)

    def benchmark(self, name_fn: Callable[[str, dict], str], alts: dict=None, base_config: str=None, callback_fn: callable=None, **kwargs):
        """
        Benchmark for database.

        :param callable name_fn: The naming function that is given parameters ``reports_path`` and ``alts`` of the benchmark
        :param dict alts: The modification would be applied to ``base_config``
        :param str base_config: The path of the bench-config for Auto-Bencher and it would be modified by ``alts``
        :param callable callback_fn: The callback function that is given parameters ``reports_path``, ``name_fn``, ``alts``, and ``base_config``
        :param **kwargs kwargs: The custom parameters of the configuration that want to modify. Each pair of custom parameter, should be a list in the format arbitrary_name=[Class, Parameter name, Value]
            , like custom_param1=["elasqlbench", "org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE", "1"].
        """
        self.__type_check(obj=name_fn, obj_type=callable, obj_name='name_fn', is_allow_none=False)
        self.__type_check(obj=alts, obj_type=dict, obj_name='alts', is_allow_none=True)
        self.__type_check(obj=base_config, obj_type=str, obj_name='base_config', is_allow_none=True)
        self.__type_check(obj=callback_fn, obj_type=callable, obj_name='callback', is_allow_none=True)

        # If call is_active(), it will hang. Don't know why
        # if not self.is_active():
        #     self.close()

        self.reconnect()
        alts = self.__update_config(config=alts, kwargs=kwargs)
        rp_path = name_fn(reports_path=self.reports_path, alts=alts)
        self.bench(is_kill_java=True, reports_path=rp_path, alts=alts, base_config=base_config)
        self.close()

        if callback_fn is not None:
            callback_fn(self.reports_path, name_fn, alts, base_config)

    def process_pickle(self, name_fn: callable, alts: dict):
        self._not_implement_error("The public method 'process_pickle' ")
        # rp_path = name_fn(reports_path=self.report_dest, alts=alts)
        # loader = Loader(f'{rp_path}', server_count=4, n_jobs=16)
        
    def __remote_estimator(self):
        """
        Call the cost estimator on local client.

        :return: A tupel contains the standard input/output/error stream after executing the command.
        :rtype: Tuple[``paramiko.channel.ChannelStdinFile``, ``paramiko.channel.ChannelFile``, ``paramiko.channel.ChannelStderrFile``]
        """
        self._not_implement_error("The private method '__remote_estimator' ")
        # self.dr.__type_check(obj=estimator_py, obj_type=str, obj_name='estimator_py', is_allow_none=False)
        # stdin, stdout, stderr, is_successed = self.dr.__client_exec(self.dr, fn_name = 'python', going_msg = 'Calling CostEstimator', finished_msg = 'Called CostEstimator', error_msg = 'Failed calling CostEstimator', py_file = self.ESTIMATOR_PY)

    def __local_estimator(self):
        self._not_implement_error("The private method '__local_estimator' ")

    def local_estimator(self, **kwargs) -> None:
        self._not_implement_error("The public method 'local_estimator' ")
        # self.__local_estimator(**kwargs)

    def remote_estimator(self, hostname: str, username: str, passward: str, port: int, event: str, **kwargs) -> None:
        self._not_implement_error("The public method 'remote_estimator' ")
        # for server in kwargs['remote_servers']:
        #     conn = rpyc.classic.connect(server)
        #     fn = conn.teleport(self.__remote_estimator)
        #     fn()
        # ssh.exec_command(command=f'source /home/{username}/.bashrc; conda activate jax-ntk; python estimator.py')