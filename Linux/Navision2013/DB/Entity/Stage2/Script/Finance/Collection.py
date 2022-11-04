from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, col, udf, broadcast
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

def finance_Collection(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        dcleEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Detailed Cust_ Ledg_ Entry")
        cleEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Cust_ Ledger Entry")
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            dcleDF = ToDFWitoutPrefix(sqlCtx, hdfspath, dcleEntity, True).where((col("EntryType") == 1) & (col("DocumentType") == 1))
            cleDF = ToDFWitoutPrefix(sqlCtx, hdfspath, cleEntity, True)
            
            cond = [dcleDF["Cust_LedgerEntryNo_"] == cleDF["EntryNo_"]]
            finalDF = dcleDF.join(cleDF, cond, 'left')

            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.write.jdbc(url=postgresUrl, table="finance.Collection", mode='overwrite', properties=PostgresDbInfo.props)

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.Collection", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.Collection', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_Collection completed: ' + str((dt.datetime.now()-st).total_seconds()))     

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Collection")
    finance_Collection(sqlCtx, spark)
