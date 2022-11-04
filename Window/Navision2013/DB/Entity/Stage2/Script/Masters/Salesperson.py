
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, concat,col,count
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import os,datetime,time,sys,traceback
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

conf = SparkConf().setMaster("local[*]").setAppName("Salesperson")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try: 
            logger = Logger()
            columns=Kockpit.TableRename("Salesperson_Purchaser") 
            SalesPerson =spark.read.parquet("../../../Stage1/ParquetData/Salesperson_Purchaser")  
            RSM = SalesPerson.select('Code','Name')\
                        .withColumnRenamed('Name','RSM_Name').withColumnRenamed('Code','RSM')          
            SalesPerson = SalesPerson.join(RSM,'RSM','left')                     
            BUHead = SalesPerson.select('Code','Name')\
                            .withColumnRenamed('Name','BUHead_Name').withColumnRenamed('Code','BUHead')              
            finalDF = SalesPerson.join(BUHead,'BUHead','left').drop('BUHead','RSM')
            finalDF = Kockpit.RENAME(finalDF,columns)
            finalDF = finalDF.withColumn('Link SalesPerson Key',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["Link SalesPerson"]))
            finalDF = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace("(","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace(")","")) for col in finalDF.columns])
            finalDF.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Master/Salesperson")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Salesperson", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 Salesperson '"+CompanyName+"' "+DBEntity+" "+exc_traceback.tb_lineno+"")
            log_dict = logger.getErrorLoggedRecord('Salesperson', '', '', ex, exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('masters_salesPerson completed: ' + str((dt.datetime.now()-st).total_seconds()))