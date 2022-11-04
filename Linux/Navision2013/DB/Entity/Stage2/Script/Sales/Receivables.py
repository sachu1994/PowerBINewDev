from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,min as min_,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,last_day,datediff,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re, logging
from builtins import str
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers import udf as Kockpit

def sales_Receivables(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        CLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Cust_ Ledger Entry")
        SIH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Invoice Header")
        DCLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Detailed Cust_ Ledg_ Entry")
        CPG_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer Posting Group")  
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            CLE = ToDFWitoutPrefix(sqlCtx, hdfspath, CLE_Entity,True)
            CLE = CLE.withColumnRenamed('DocumentNo_','CLE_Document_No')
            SIH = ToDFWitoutPrefix(sqlCtx, hdfspath, SIH_Entity,False)
            SIH = SIH.select('No_','PaymentTermsCode').withColumnRenamed('No_','CLE_Document_No')
            DCLE = ToDFWitoutPrefix(sqlCtx, hdfspath, DCLE_Entity,True)
            CPG = ToDFWitoutPrefix(sqlCtx, hdfspath, CPG_Entity,False)
            CPG = CPG.select('Code','ReceivablesAccount').withColumnRenamed('Code','CustomerPostingGroup')\
                        .withColumnRenamed('ReceivablesAccount','GLAccount')
            
            cond = [CLE["EntryNo_"] == DCLE["Cust_LedgerEntryNo_"]]
            finalDF = DCLE.join(CLE, cond, 'left')
            
            finalDF = finalDF.join(SIH, 'CLE_Document_No', 'left')
            finalDF = finalDF.join(CPG,'CustomerPostingGroup','left')
            
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            finalDF = finalDF.join(DSE,"DimensionSetID",'left')
            
            #ARsnapshots = ARsnapshots.withColumn("AdvanceFlag",when(ARsnapshots["AdvanceCollection"]==1,lit('Advance')).otherwise(lit('NA')))
            
            finalDF = Kockpit.RenameDuplicateColumns(finalDF)
            
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Sales.Receivables", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Sales.Receivables", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Sales.Receivables', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('sales_Receivables completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Receivables")
    sales_Receivables(sqlCtx, spark)
pyth