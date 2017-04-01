#!/usr/bin/env bash

# one example of run.sh script for implementing the features using python
# the contents of this script could be replaced with similar files from any major language

# I'll execute my programs, with the input directory log_input and output the files in the directory log_output
# python ./src/process_log.py ./log_input/log.txt ./log_output/hosts.txt ./log_output/hours.txt ./log_output/resources.txt ./log_output/blocked.txt

export SPARK_HOME=/home/sparkit/Spark/spark-2.0.2-bin-hadoop2.7
export PY4J=${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip
export PYTHON_PATH=${SPARK_HOME}/python

cd src
LOG_INPUT='../log_input/log.txt'
LOG_OUTPUT='../log_output'
OUTPUT_HOSTS=${LOG_OUTPUT}/hosts.txt
OUTPUT_HOURS=${LOG_OUTPUT}/hours.txt
OUTPUT_RESOURCES=${LOG_OUTPUT}/resources.txt
OUTPUT_BLOCKED=${LOG_OUTPUT}/blocked.txt

spark-submit process_log.py ${LOG_INPUT} ${OUTPUT_HOSTS} ${OUTPUT_HOURS} ${OUTPUT_RESOURCES} ${OUTPUT_BLOCKED}