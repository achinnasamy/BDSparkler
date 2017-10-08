
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType

sparkconfig = SparkConf()
sparkconfig.setMaster("local[*]")
sparkconfig.setAppName("SparkCSVJOB")

def print_each_line(eachLine):
    print eachLine
    return



sparkcontext = SparkContext(conf= sparkconfig)
sqlContext = SQLContext(sparkcontext)

csvDF = sqlContext.read \
                  .option("header", "true") \
                  .csv("/home/dharshekthvel/Downloads/stop.csv")

csvDF.printSchema()
# csvDF.show(2)


