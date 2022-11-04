'''
Created on 7 Jan 2020
@author: Jaydip
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt

'''
from pathlib import Path
p = list(Path(os.getcwd()).parts)
helpersDir = '/' + p[1] + '/' + p[2] #root/KockpitStudio'
'''
helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers.DBInfo import *

def masters_customers(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        CUST_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer") 
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            finalDF = ToDFWitoutPrefix(sqlCtx, hdfspath, CUST_Entity,True)
            finalDF.show()
            exit()
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Masters.Customer", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Customer", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
        
    except Exception as ex:
        exc_type,exc_value,exc_traceback=sys.exc_info()
        print("Error:",ex)
        print("type - "+str(exc_type))
        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
        print("Error Line No. - "+str(exc_traceback.tb_lineno))
        ex = str(ex)
        logger.endExecution()

        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
        
        log_dict = logger.getErrorLoggedRecord('Customer', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_customers completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Collection")
    masters_customers(sqlCtx, spark)
    '''
    conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Stage2:Customer")\
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
            .set("spark.sql.broadcastTimeout", "36000")\
            .set("spark.network.timeout", 10000000)

    sc = SparkContext(conf = conf)
    sqlCtx = SQLContext(sc)
    spark = SparkSession.builder.appName("Customer").getOrCreate() #sqlCtx.sparkSession
    masters_customers(sqlCtx, spark)
    '''
