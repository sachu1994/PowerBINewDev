'''
Created on 10 Nov 2016
@author: Abhishek
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,last_day,datediff,length,trim
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

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers import udf as Kockpit

def sales_SalesOrder(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

    try:
        
        SH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Header")
        SL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Line") 
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            SH = ToDFWitoutPrefix(sqlCtx, hdfspath, SH_Entity,True)
            SL = ToDFWitoutPrefix(sqlCtx, hdfspath, SL_Entity,True)
            SL = SL.withColumnRenamed('No_','ItemNo_')
            
            SO = SL.join(SH, SL['DocumentNo_']==SH['No_'], 'left')
            SO = SO.filter(SO['DocumentType']==1).filter(year(SO['PostingDate'])!=1753)
            SO = SO.withColumn("NOD",datediff(SO['PromisedDeliveryDate'],lit(datetime.datetime.today())))
            
            SO =  SO.withColumn("LineAmount",when((SO.LineAmount/SO.CurrencyFactor).isNull(),SO.LineAmount).otherwise(SO.LineAmount/SO.CurrencyFactor))\
                    .withColumn("Transaction_Type",lit("SalesOrder"))
            
            SO.cache()
            list = SO.select("NOD").distinct().rdd.flatMap(lambda x: x).collect()
            list = [x for x in list if x is not None]
            
            Query_PDDBucket="(SELECT *\
                        FROM "+chr(34)+"tblPDDBucket"+chr(34)+") AS df"
            PDDBucket = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_PDDBucket,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            Bucket = RENAME(PDDBucket,{"Min Limit":"LowerLimit","Max Limit":"UpperLimit"
                                       ,"Bucket Name":"BucketName","Bucket Sort":"BucketSort"})
            Bucket = Bucket.drop('DBName','EntityName')
            Bucket = Bucket.collect()
            
            TempBuckets = BUCKET(list,Bucket,spark)
            TempBuckets.show()
            
            SO = LJOIN(SO,TempBuckets,'NOD')
            
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            finalDF = SO.join(DSE,"DimensionSetID",'left')
            
            finalDF = Kockpit.RenameDuplicateColumns(finalDF)
            
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Sales.SalesOrder", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props
            
            
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Sales.Sales_Order", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Sales.Sales_Order', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('sales_SalesOrder completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Sales.SalesOrder")
    sales_SalesOrder(sqlCtx, spark)