
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

sqlContext.udf.register("STRING_LENGTH", lambda s: len(s), IntegerType())

csvDF = sqlContext.read \
                  .option("header", "true") \
                  .csv("/home/dharshekthvel/Downloads/stop.csv")

csvDF.registerTempTable("STOP_TABLE")

length_of_line_number = sqlContext.sql("SELECT STRING_LENGTH(VEHICLE_ID) FROM STOP_TABLE")
length_of_line_number.show(3)

#csvDF.printSchema()
# csvDF.show(2)


