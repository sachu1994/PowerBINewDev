from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit,concat,month,year,substring,when,ceil,min as min_,max as max_,from_unixtime,from_utc_timestamp,unix_timestamp,udf
from datetime import timedelta, date
import re,os
import datetime,time,sys
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
import datetime as dt

def masters_calendar(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            Calendar_StartDate = '2018-04-01'
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
            Calendar_EndDate = datetime.datetime.today().date()
            
            
            data =[]
            for single_date in DRANGE(Calendar_StartDate, Calendar_EndDate):
                data.append({'Link_date':single_date})
            
            
            
            schema = StructType([
            StructField("Link_date", DateType(),True)
            ])
            
            records=spark.createDataFrame(data,schema)
            
            
            records=records.select(records.Link_date.alias('LinkDate'),month(records.Link_date).alias('Month'),year(records.Link_date).alias('Year')).distinct().sort('LinkDate')
            
            records = records.withColumn("Fiscal_Monthno",when(records.Month>(int(MnSt)-1),records.Month-int(MnSt)+1).otherwise(records.Month+(13-int(MnSt))))\
                        .withColumn("Fiscal_Year",when(records.Month>(int(MnSt)-1),concat(records.Year,lit('-'),substring(records.Year+1,3,2))).otherwise(concat(records.Year-1,lit('-'),substring(records.Year,3,2))))\
                        .withColumn("FY_Year",when(records.Month>(int(MnSt)-1),records.Year).otherwise(records.Year-1))
            
            records = records.na.fill({'LinkDate':'NA'})
            records = records.withColumn("Fiscal_Quarter",when(records.Month>(int(MnSt)-1),concat(lit("Q"),ceil((records.Month-int(MnSt)+1)/3))).otherwise(concat(lit("Q"),ceil((records.Month+(13-int(MnSt)))/3))))
            
            ######### ADDED ARoy 13 May 3 lines
            records = records.withColumn("LinkDateKey",concat(lit(DBName),lit("|"),lit(EntityName),lit("|"),records.LinkDate))\
                .withColumn("DBName",lit(DBName))\
                .withColumn("EntityName",lit(EntityName))
            
            
            records = records.withColumn("Fiscal_Month",when(records.Month == 1,"Jan").when(records.Month == 2,"Feb").when(records.Month == 3,"Mar").when(records.Month == 4,"Apr").when(records.Month == 5,"May").when(records.Month == 6,"Jun").when(records.Month == 7,"Jul").when(records.Month == 8,"Aug").when(records.Month == 9,"Sep").when(records.Month == 10,"Oct").when(records.Month == 11,"Nov").when(records.Month == 12,"Dec").otherwise('null'))
            records = records.withColumn("Reload_Time",lit(datetime.datetime.now()))
            # records.show()
            # df_train = records.withColumn("dates", from_unixtime(unix_timestamp(records.LinkDate, 'MMMMM dd  yyy')))
            # month_udf = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime("%B"), returnType = StringType())
            # dftest = records.withColumn("monthname", month_udf(df_train.dates))
            # dftest.show()
            
            records.write.jdbc(url=postgresUrl, table="masters.Calendar", mode="overwrite", properties=PostgresDbInfo.props)
          
            logger.endExecution()
                
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("Calendar", DBName, EntityName, records.count(), len(records.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Calendar', '', '', ex, exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_calendar completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Calendar")
    masters_calendar(sqlCtx, spark)
