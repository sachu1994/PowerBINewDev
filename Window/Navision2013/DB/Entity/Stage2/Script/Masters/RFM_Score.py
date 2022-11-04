from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit
import time,re,os,sys,traceback
import os,sys
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str
import pandas
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
sys.path.insert(0,'../../../..')
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DBEntity = DBName+EntityName

from Configuration.AppConfig import * 
from Configuration.Constants import *
from Configuration.udf import *
conf = SparkConf().setMaster("local[*]").setAppName("DSE").\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:  
            logger = Logger()
            RFMcsv =DBNamepath+"\\"+"Configuration"+"\\"+'RFM_SCORE.csv'
            finalDF = spark.read.csv(RFMcsv,header='true', 
                              inferSchema='true')
            finalDF.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Master/RFM_Score")
            
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("RFM_Score", DBName, EntityName, finalDF.count(), len(result_df.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)            
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 RFM_Score '"+CompanyName+"' "+DBEntity+" "+exc_traceback.tb_lineno+"")    
            log_dict = logger.getErrorLoggedRecord('RFM_Score', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_RFM_Score completed: ' + str((dt.datetime.now()-st).total_seconds()))