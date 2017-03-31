from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)


def print_rdd(x):
    print (x)

input_rdd = sc.textFile('../log_input/log.txt')
split_rdd = input_rdd.map(lambda line: line.split('- -'))
pair_rdd = split_rdd.map(lambda x: (x[0].rstrip(), 1))
res = pair_rdd.reduceByKey(lambda x, y: x + y)\
    .top(10, key=lambda x: x[1])
print (res)
# .foreach(print_rdd)

# pair_rdd =
