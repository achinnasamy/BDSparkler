
from pyspark import SparkConf
from pyspark import SparkContext


sparkconfig = SparkConf()
sparkconfig.setMaster("local[*]")
sparkconfig.setAppName("SparkCSVJOB")

def print_each_line(eachLine):
    print eachLine
    return


sparkcontext = SparkContext(conf= sparkconfig)

textFileRDD = sparkcontext.textFile("/home/dharshekthvel/Downloads/query_result.csv")

textFileRDD.foreach(print_each_line)


