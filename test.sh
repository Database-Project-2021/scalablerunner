# /bin/bash

python -m unittest tests.test_all.TestSSH
wait
python -m unittest tests.test_all.TestTaskRunner
wait
python -m unittest tests.test_all.TestUtil
wait

# Test DBRunner
python -m unittest tests.test_all.TestDBRunner.test_connect
wait
python -m unittest tests.test_all.TestDBRunner.test_upload_jars
wait
python -m unittest tests.test_all.TestDBRunner.test_load
wait
python -m unittest tests.test_all.TestDBRunner.test_bench
wait