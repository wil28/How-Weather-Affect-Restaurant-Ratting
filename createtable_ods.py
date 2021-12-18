"""/*
 * @Author: willy 
 * @Date: 2021-12-18 11:23:12 
 * @Last Modified by:   willy 
 * @Last Modified time: 2021-12-18 11:23:12 
 */
"""

from typing import _SpecialForm
from pyspark.sql import SparkSession,types
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from os import environ
import time
import sys
import ast
import subprocess
from dateutil.relativedelta import relativedelta



spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = sc.applicationId

spark.conf.set("spark.sql.codegen.wholeStage", False)

def write_data(result_data, mode, format, tablename):
    try :
        result_data.write.mode(mode).format(format).saveAsTable(tablename)
        return 0
    except :
        return 1


if __name__ == "__main__":
    date_file = datetime.now().strftime("%Y-%m-%d")
    start_time = datetime.now().replace(microsecond=0)
    filter_one_year = (datetime.strptime(date_file, "%Y-%m-%d") - relativedelta(months=12)).strftime("%Y-%m-%d")


    df_wheater = spark.sql("""select * database.wheater""")
    df_restaurant = spark.sql("""select * database.restaurant""")


    ##we can adjust aggregate on this
    df_agg = df_wheater.groupBy("id","date","name").agg(F.mean('weather_main'))
    temjoin = df_wheater.join(df_agg,['date','name'], how='left').withColumnRenamed("avg(weather_main)",'average_weather')\
                                                                .withColumnRenamed("name","city_name")
    ## just get data last year
    df_restaurant = df_restaurant.filter(df_restaurant("date") >= filter_one_year)

    dataframe = df_restaurant.join(df_agg, ['id','date','name'], how='left')
    dataframe.createOrReplaceTempView("ods_table")
    exitcode  = write_data(dataframe,'overwrite','hive','database.ods_restaurant_ratting')

    ## insert to DW 
    history_time = spark.sql("select `date` from DW").distinct()
    history_time.createOrReplaceTempView("history_time")
    query_insert = spark.sql("""
    INSERT INTO DW SELECT * FROM ods_table 
    where 1=1 and `date` not in (select date_id from history_time)""")

    end_proc = datetime.now().replace(microsecond=0)
    print("Duration Process Query is {}, Finish".format(end_proc - start_time))
    spark.catalog.clearCache()
    spark.stop()