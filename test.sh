# /bin/bash

python -m unittest tests.test_all.TestSSH
python -m unittest tests.test_all.TestTaskRunner
python -m unittest tests.test_all.TestUtil

# Test DBRunner
python -m unittest tests.test_all.TestDBRunner.test_connect
python -m unittest tests.test_all.TestDBRunner.test_upload_jars
python -m unittest tests.test_all.TestDBRunner.test_load
python -m unittest tests.test_all.TestDBRunner.test_bench