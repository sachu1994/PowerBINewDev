
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, concat
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str

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
DBEntity = DBName+EntityName

conf = SparkConf().setMaster("local[*]").setAppName("Vendor")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            columns=Kockpit.TableRename("Vendor")       
            finalDF=spark.read.parquet("../../../Stage1/ParquetData/Vendor" ) 
            finalDF = finalDF.withColumn('Link Vendor Key',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["No_"]))
            finalDF = Kockpit.RENAME(finalDF,columns)       
            result_df = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
            result_df = result_df.select([F.col(col).alias(col.replace("(","")) for col in result_df.columns])
            result_df = result_df.select([F.col(col).alias(col.replace(")","")) for col in result_df.columns])
            result_df.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Master/Vendor")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Vendor", DBName, EntityName, result_df.count(), len(result_df.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 Vendor '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")    
            log_dict = logger.getErrorLoggedRecord('Vendor', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_Vendor completed: ' + str((dt.datetime.now()-st).total_seconds()))