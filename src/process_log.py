from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)


sc.textFile('../log_input/log.txt')
