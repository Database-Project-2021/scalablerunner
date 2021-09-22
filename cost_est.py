import argparse
from typing import List

from cost_estimator import Loader, Preprocessor, RegressionModels, Task

def check_none():
    return 

class CostEstimatorArgs():
    def __init__(self):
        pass
    def add_argument(**kwargs):
        pass

def call_costestimator(task_name: str, sub_task_name: List[str], csv_dir: List[str], server_count: int, ou_name: str, 
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
    parser.add_argument('--csv_dir', type=str,
                        help='The path of the reports')
    parser.add_argument('--server_count', type=int,
                        help='The number of the server machines')
    parser.add_argument('--ou_name', type=str,
                        help='The name of the training OU model')
    parser.add_argument('--warmup_interval_second', type=int,
                        help='The warmup-interval in seconds')
    parser.add_argument('--drop_alg', type=str,
                        help='The algorithm to drop ouliner&na value, available: iqr | zscore | std')
    parser.add_argument('--std_times', type=float,
                        help='')
    parser.add_argument('--target_distribution', type=str,
                        help='target_distribution = single | all')
    parser.add_argument('--loss_type', type=str,
                        help='the type of loss function = origin | mse | cross | similarity')
    parser.add_argument('--name', type=str,
                        help='the folder name of the results')
    parser.add_argument('--save_raw_img', action='store_true',
                        help='save the raw image')
    args = parser.parse_args()
    
    call_costestimator(**args)