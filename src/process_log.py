import os
import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext
from utility import util_resources
from utility import util_hosts
from utility import util_hours
from utility import util_blocked
from utility import util
from utility.timer import Timer


"""
The main python script of project fansite-analytics-challenge
"""

log_input = sys.argv[1]
hosts_output = sys.argv[2]
hours_output = sys.argv[3]
resources_output = sys.argv[4]
blocked_output = sys.argv[5]

conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
timer = Timer()
timer.start()
input_rdd = sc.textFile(log_input).persist()
timer.stamp("Created input RDD")

"""
Feature 1: Top 10 hosts/IPs
"""
util_hosts.output_top_hosts(input_rdd, hosts_output)
timer.stamp("Output top hosts / IPs completed")

"""
Feature 2: Top 10 Resources
"""
util_resources.output_top_resource(input_rdd, resources_output)
timer.stamp("Output top resources completed")

"""
Feature 3: Top 10 60-minute periods
"""
log_start_time = util.get_start_time(log_input)
util_hours.output_top_hours_tuned(input_rdd, log_start_time, hours_output)
timer.stamp("Output top hours completed")

"""
Feature 4: Blocked Hosts/IPs
"""
util_blocked.output_blocked_hosts(input_rdd, log_start_time, blocked_output)
timer.stamp("Output blocked hosts completed")
