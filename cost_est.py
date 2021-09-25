import argparse
from typing import List

try:
    from cost_estimator import Loader, Preprocessor, RegressionModels, Task
except:
    raise ImportError(f"Pleas install cost_estimator manually: $ pip install git+https://github.com/elasql/cost-estimator.git")

class CostEstimatorArgs():
    """
    The arguments class for call_costestimator(), provide a flexible solution for that function.
    """

    def __init__(self, **init_values):
        default_init_values = {
        'task_name' : 'DummyTask',
        'sub_task_name' : ['Subtask1', 'Subtask2', 'Subtask3'],
        'csv_dir' : ['dir1', 'dir2', 'dir3'],
        'server_count' : 1,
        'ou_name' : 'DummyOU',
        'warmup_interval_second' : 0,
        'drop_alg' : '',
        'std_times' : '',
        'sample_size' : 10000,
        'feature_names' : [],
        'train_size' : 0.8,
        'n_jobs' : -1
    }
        for(key, val) in default_init_values.items():
            if init_values[key] is not None:
                self.values = init_values[key]
            else:
                self.values = default_init_values[key]
        self.inited = True
        return

    def add_argument(self, **kwargs):
        for(key, val) in kwargs.items():
            self.values[key] = val
        return
        
    def get(self, key:str):
        return self.values[key]

    def exec(self):
        if self.inited is not True:
            raise BaseException(f"Please call method 'self.__init__()' before using exec()")
        call_estimator(task_name=self.values['task_name'], sub_task_name=self.values['sub_task_name'], csv_dir=self.values['csv_dir'], server_count=self.values['server_count'], ou_name=self.values['ou_name'],
                        warmup_interval_second=self.values['warmup_interval_second'], drop_alg=self.values['drop_alg'], std_times=self.values['std_times'],
                        sample_size=self.values['sample_size'], feature_names=self.values['feature_names'], train_size=self.values['train_size'], n_jobs=self.values['n_jobs'])
        return

def call_estimator(task_name: str, sub_task_name: List[str], csv_dir: List[str], server_count: int, ou_name: str, 
                   warmup_interval_second: int=0, drop_alg: str=None, std_times: str=None, 
                   sample_size: int=10000, feature_names: list=None, train_size: float=0.8, n_jobs: int=-1):
    # Loader
    loader = Loader(csv_dir=csv_dir, server_count=server_count, n_jobs=n_jobs)
    df_features = loader.load_features_as_df()
    df_latencies = loader.load_latencies_as_df()

    # Preprocess
    psr = Preprocessor(df_features=df_features, df_label=df_latencies, ou_name=ou_name, warmup_interval_second=warmup_interval_second)
    psr.drop_warmup_data()

    # Mean and Std before filter
    mean_before_filter = float(psr.get_label().mean())
    std_before_filter = float(psr.get_label().std())

    if drop_alg is not None:
        psr.drop_outlier_and_na(drop_alg=drop_alg, std_times=std_times)
    
    # Mean and Std after filter
    mean_after_filter = float(psr.get_label().mean())
    std_after_filter = float(psr.get_label().std())

    psr.sample_features(sample_size=sample_size)
    psr.specify_features(feature_names=feature_names)

    X_train, X_test, Y_train, Y_test = psr.train_test_split(train_size=train_size)

    # Task
    task = Task(ou_name, task_name)
    task.set_sub_task_dataset_info(
        feature_names,
        sub_task_name,
        psr.info['before_drop_outlier_data_size'],
        mean_before_filter,
        std_before_filter,
        psr.info['outlier_alg'],
        psr.info['outlier_size'],
        mean_after_filter,
        std_after_filter,
        psr.info['sample_size'],
        psr.info['train_test_ratio'],
    )

    models = RegressionModels()
    models.fit(X_train, X_test, Y_train, Y_test, task, sub_task_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Execute cost-estimator')
    parser.add_argument('--task_name', type=str,
                        help='The name of the task')
    parser.add_argument('--sub_task_name', type=str, nargs='+', 
                        help='The name of the subtasks')
    parser.add_argument('--csv_dir', type=str, nargs='+', 
                        help='The path of the reports')
    parser.add_argument('--server_count', type=int,
                        help='The number of the server machines')
    parser.add_argument('--ou_name', type=str,
                        help='The name of the training OU model')
    parser.add_argument('--warmup_interval_second', type=int,
                        help='The warmup-interval in seconds')
    parser.add_argument('--drop_alg', type=str,
                        help='The algorithm to drop ouliner&na value, available: iqr | zscore | std')
    parser.add_argument('--std_times', type=int,
                        help='The number of time to do the standard dev')
    parser.add_argument('--sample_size', type=int,
                        help='The size of sample')
    parser.add_argument('--feature_names', type=str, nargs='+',
                        help='The name of features')
    parser.add_argument('--train_size', type=float,
                        help='The slice rate of training/test dataset')
    parser.add_argument('--n_jobs', type=int,
                        help='The number of jobs')
    args = parser.parse_args()

    # Args = CostEstimatorArgs
    # Args.__init__(args)
    
    # call_estimator(Args)
    call_estimator(**args)