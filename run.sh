#!/usr/bin/env bash

# one example of run.sh script for implementing the features using python
# the contents of this script could be replaced with similar files from any major language

# I'll execute my programs, with the input directory log_input and output the files in the directory log_output
# python ./src/process_log.py ./log_input/log.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt

export SPARK_HOME=/home/sparkit/Spark/spark-2.0.2-bin-hadoop2.7
export PY4J=${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip
export PYTHON_PATH=${SPARK_HOME}/python

spark-submit ./src/process_log.py ./log_input/log_sample.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt