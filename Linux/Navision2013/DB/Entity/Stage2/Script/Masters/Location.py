from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, udf, broadcast,col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,ltrim

import datetime, time
import datetime as dt
import re

from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *


def masters_Location(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        
        lEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Location")
        table_rename = next (table for table in config["TablesToRename"] if table["Table"] == "Location")
        columns = table_rename["Columns"][0]
        for entityObj in config["DbEntities"]:
            logger = Logger()
            
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            finalDF = ToDFWitoutPrefix(sqlCtx, hdfspath, lEntity, True)
            finalDF.printSchema()
            finalDF = RENAME(finalDF,columns)
            finalDF.printSchema()
            
            finalDF = finalDF.withColumn('DB',lit(DBName))\
                    .withColumn('Entity',lit(EntityName))
            finalDF = finalDF.withColumn('Link Location Key',concat(finalDF["DB"],lit('|'),finalDF["Entity"],lit('|'),finalDF["Link Location"]))
            finalDF.printSchema()
            
            finalDF.write.jdbc(url=postgresUrl, table="masters.Location", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("masters.Location", entityLocation, entityLocation, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
            
    except Exception as ex:
        exc_type,exc_value,exc_traceback=sys.exc_info()
        print("Error:",ex)
        print("type - "+str(exc_type))
        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
        print("Error Line No. - "+str(exc_traceback.tb_lineno))

        logger.endExecution()

        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
        
        log_dict = logger.getErrorLoggedRecord('masters.Location', '', '', ex, exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_Location completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Location")
    masters_Location(sqlCtx, spark)