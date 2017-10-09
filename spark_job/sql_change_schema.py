

from datetime import datetime
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType, FloatType, BooleanType, DateType

sparkconfig = SparkConf()
sparkconfig.setMaster("local[*]")
sparkconfig.setAppName("SparkCSVJOB")

def print_each_line(eachLine):
    print eachLine
    return



sparkcontext = SparkContext(conf= sparkconfig)
sqlContext = SQLContext(sparkcontext)


toIntegerfunc = UserDefinedFunction(lambda eachElement: int(eachElement),IntegerType())
toBooleanfunc = UserDefinedFunction(lambda eachElement : True if eachElement=='P'
                                                              else False , BooleanType())

toDateFunc = UserDefinedFunction(lambda eachElement: datetime.strptime(eachElement,'%m/%d/%Y'),DateType())


csvDF = sqlContext.read \
                  .option("header", "true") \
                  .csv("/home/dharshekthvel/Downloads/stop.csv")

schema_modified_df = csvDF.withColumn("VEHICLE_ID", toIntegerfunc(csvDF["VEHICLE_ID"]))\
                          .withColumn("PLAN_STATUS", toBooleanfunc(csvDF["PLAN_STATUS"]))\
                          .withColumn("OPD_DATE", toDateFunc(csvDF["OPD_DATE"]))

#csvDF.printSchema()
#csvDF.show(2)

schema_modified_df.printSchema()
schema_modified_df.show(20)

#
# root
#  |-- EVENT_NO: string (nullable = true)
#  |-- EVENT_NO_TRIP: string (nullable = true)
#  |-- EVENT_NO_PREV: string (nullable = true)
#  |-- OPD_DATE: string (nullable = true)
#  |-- VEHICLE_ID: string (nullable = true)
#  |-- MASTER_ID: string (nullable = true)
#  |-- METERS: string (nullable = true)
#  |-- ACT_ARR_TIME: string (nullable = true)
#  |-- ACT_DEP_TIME: string (nullable = true)
#  |-- NOM_ARR_TIME: string (nullable = true)
#  |-- NOM_DEP_TIME: string (nullable = true)
#  |-- POINT_ID: string (nullable = true)
#  |-- STOP_ID: string (nullable = true)
#  |-- STOP_POS: string (nullable = true)
#  |-- DISTANCE_TO_NEXT: string (nullable = true)
#  |-- DISTANCE_TO_TRIP: string (nullable = true)
#  |-- DOORS_OPENING: string (nullable = true)
#  |-- POSITIONING_METHOD: string (nullable = true)
#  |-- STOP_TYPE: string (nullable = true)
#  |-- GPS_LONGITUDE: string (nullable = true)
#  |-- GPS_LATITUDE: string (nullable = true)
#  |-- PATTERN_IDX: string (nullable = true)
#  |-- DOOR_OPEN_TIME: string (nullable = true)
#  |-- POINT_ROLE: string (nullable = true)
#  |-- POINT_ACTION: string (nullable = true)
#  |-- PLAN_STATUS: string (nullable = true)
