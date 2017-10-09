
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType


def print_each_line(eachLine):
    print eachLine
    return

def main(file):

    sparkconfig = SparkConf()
    sparkconfig.setMaster("local[*]")
    sparkconfig.setAppName("SparkCSVJOB")

    sparkcontext = SparkContext(conf= sparkconfig)
    sqlContext = SQLContext(sparkcontext)

    csvDF = sqlContext.read \
                      .option("header", "true") \
                      .csv(file)

    csvDF.printSchema()
    csvDF.show(2)


