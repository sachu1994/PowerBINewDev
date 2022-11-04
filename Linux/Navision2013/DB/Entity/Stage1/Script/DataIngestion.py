from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
import pyspark.sql.functions as F
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt 
#from Configuration.AppConfig import *
helpersDir = abspath(join(join(dirname(__file__), '..'),'..','..'))
sys.path.insert(0, helpersDir)
from Configuration.AppConfig import *

conf = SparkConf().setMaster("local[8]").setAppName("DataIngestion")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession


class ConnectionInfo:
    JDBC_PARAM = "jdbc"
    SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQL_URL="jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}"
 
sqlurl=ConnectionInfo.SQL_URL.format(config["SourceDBConnection"]['url'], str(config["SourceDBConnection"]["port"]), config["SourceDBConnection"]['databaseName'], config["SourceDBConnection"]['userName'], config["SourceDBConnection"]['password'],config["SourceDBConnection"]['dbtable'])
for entityObj in config["DbEntities"]:
            
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
for Table in config["TablesToIngest"]:
    
    table_name = 'Team Computers Pvt Ltd$' + Table['Table']
    
    query= '(SELECT TOP(10) from [dbo].['+table_name+']) as q1'
    try:
    
        schemaquery = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+table_name+chr(39)+") AS data"
        schema = spark.read.format("jdbc").options(url=sqlurl,dbtable=schemaquery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load().collect()

    
    
        temp = ''
        for i in range(0,len(schema)):
            col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
            if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                col_name_temp = col_name_temp
            elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                col_name_temp = "," + col_name_temp
            if schema[i]['DATA_TYPE']=='sql_variant':
                temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
            else:
                temp = temp + col_name_temp
        tableQuery = "(SELECT "+temp+" FROM ["+table_name+"] ) AS data1"
        schemaDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
        schemaDF.show()
        exit()    
        result_df = schemaDF.select([F.col(col).alias(col.replace(" ","")) for col in schemaDF.columns])
        result_df = result_df.select([F.col(col).alias(col.replace("(","")) for col in result_df.columns])
        result_df = result_df.select([F.col(col).alias(col.replace(")","")) for col in result_df.columns])           
        Type = Table["TableType"]
        table =Table["Table"]
    
        if Type == "Master":
            result_df.coalesce(1).write.mode("overwrite").parquet("D:\AMIT_KUMAR\Kockpit\DB1\E2\Stage1\ParquetData\\"+table_name)
            print("record saved")
    except Exception as ex:
        print('Error in Task: ', Table, ' -> ', str(ex))
