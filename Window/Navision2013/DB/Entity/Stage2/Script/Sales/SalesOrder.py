from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year,when,count,datediff,concat,concat_ws,to_date,concat,col
from pyspark.sql.types import *
import os,sys
from os.path import dirname, join, abspath
import re,os,datetime,time,sys,traceback
import datetime as dt 
from builtins import str, int
from datetime import date
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
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..'))
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DBEntity = DBName+EntityName
conf = SparkConf().setMaster("local[*]").setAppName("SalesOrder").\
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

IMpath =DBNamepath+"\\"+"Configuration"+"\\"+'IncrementalMonth.txt'
SalesParquet_Path = abspath(join(join(dirname(__file__),'..','..','ParquetData','Sales')))
IMfile=open(IMpath)
imonth = IMfile.read()
imonth=int(imonth)
columns = StructType([])
cy = date.today().year
cm = date.today().month
SH = spark.createDataFrame(data = [],schema=columns)
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            path = SalesParquet_Path+"\\"+"SalesOrder"
            if os.path.exists(path1):
                for i in range(imonth+1):    
                    mon = cm-i
                    if mon==0:
                        cy = cy-1
                        mon = mon+12
                    if mon<10:
                        mon = '0'+str(mon)
                    mon = str(mon)    
                    ym = str(cy)+mon
                    partitionedSH = spark.read.parquet("../../../Stage1/ParquetData/SalesHeader/YearMonth="+ym+"")
                    partitionedSH =partitionedSH.withColumn('YearMonth',lit(ym))
                    SH = partitionedSH.unionByName(SH, allowMissingColumns = True)
                SL = spark.read.parquet("../../../Stage1/ParquetData/SalesLine")
                DSE = spark.read.parquet("../../../Stage2/ParquetData/Master/DSE").drop("DBName","EntityName")
                SH=SH.select("DocumentType","No_","DBName","EntityName","YearMonth","ShipmentDate","PostingDate","CurrencyFactor","PromisedDeliveryDate","Cust_OrderRec_Date")
                
                SL=SL.select("DocumentNo_","DimensionSetID","No_","OutstandingQuantity","Amount","LineAmount")
                SL = SL.withColumnRenamed('No_','ItemNo_')
                # Left join on sales line an sales header to get customer info w.r.t  every item 
                SO = SL.join(SH, SL['DocumentNo_']==SH['No_'], 'left')
               
                #filter rows with Posting Date !=1753 , because we dont include because 1st January 1753 (1753-01-01)is the minimum date value for a datetime in SQl
                SO = SO.filter(SO['DocumentType']==1).filter(year(SO['PostingDate'])!=1753)
                 
                # Adding column NOD , which is difference of PromisedDeliverydate and  till today to get how many days an item is late for delivery
                SO = SO.withColumn("NOD",datediff(SO['PromisedDeliveryDate'],lit(datetime.datetime.today())))
                SO =  SO.withColumn("LineAmount",when((SO.LineAmount/SO.CurrencyFactor).isNull(),SO.LineAmount).otherwise(SO.LineAmount/SO.CurrencyFactor))\
                        .withColumn("Transaction_Type",lit("SalesOrder"))
                PDDBucket =spark.read.parquet("../../../Stage1/ConfiguratorData/tblPDDBucket").drop('DBName','EntityName')
                Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
                MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
        
                Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
                MinLimit = int(Minofgt.select('MinLimit').first()[0])
                SO=LJOIN(SO,PDDBucket,'NOD').drop('ID','MinLimit','MaxLimit')
                SO=SO.withColumn('BucketName',when(SO['NOD']>=MinLimit, lit("61+")))
                SO=SO.withColumn('BucketName',when(SO['NOD']<=MaxLimit, lit("61-")))
                
            #Apply Left join on SO and DSE to get keys w.r.t every dimension
                finalDF = SO.join(DSE,"DimensionSetID",'left').drop('DimensionSetID','NOD','Bucket_Sort','Transaction_Type','PromisedDeliveryDate','No_','DocumentType','CurrencyFactor')
                finalDF=finalDF.withColumn("PostingDate",to_date(col("PostingDate")))
                finalDF = finalDF.withColumn('LinkItemKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["ItemNo_"]))\
                        .withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"])).drop('ItemNo_')
                spark.conf.set(
                                "spark.sql.sources.partitionOverwriteMode", "dynamic")
                 
                finalDF.coalesce(1).write.mode("overwrite").partitionBy("YearMonth").parquet("../../../Stage2/ParquetData/Sales/SalesOrder")
                     
            else:
            
                SH = spark.read.parquet("../../../Stage1/ParquetData/SalesHeader")
                SH=SH.select("DocumentType","No_","DBName","EntityName","YearMonth","ShipmentDate","PostingDate","CurrencyFactor","PromisedDeliveryDate","Cust_OrderRec_Date")
                
                SL = spark.read.parquet("../../../Stage1/ParquetData/SalesLine")
                SL=SL.select("DocumentNo_","DimensionSetID","No_","OutstandingQuantity","Amount","LineAmount")
                
                SL = SL.withColumnRenamed('No_','ItemNo_')
                # Left join on sales line an sales header to get customer info w.r.t  every item 
                SO = SL.join(SH, SL['DocumentNo_']==SH['No_'], 'left')
                #filter rows with Posting Date !=1753 , because we dont include because 1st January 1753 (1753-01-01)is the minimum date value for a datetime in SQl
                SO = SO.filter(SO['DocumentType']==1).filter(year(SO['PostingDate'])!=1753)
                # Adding column NOD , which is difference of PromisedDeliverydate and  till today to get how many days an item is late for delivery
                SO = SO.withColumn("NOD",datediff(SO['PromisedDeliveryDate'],lit(datetime.datetime.today())))
                SO =  SO.withColumn("LineAmount",when((SO.LineAmount/SO.CurrencyFactor).isNull(),SO.LineAmount).otherwise(SO.LineAmount/SO.CurrencyFactor))\
                        .withColumn("Transaction_Type",lit("SalesOrder"))
                # filtering distinct rows in NOD Column and fetching in a list , which will be used in below Bucket Function
                
                PDDBucket =spark.read.parquet("../../../Stage1/ConfiguratorData/tblPDDBucket").drop('DBName','EntityName')
                Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
                MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
        
                Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
                MinLimit = int(Minofgt.select('MinLimit').first()[0])
                SO=LJOIN(SO,PDDBucket,'NOD').drop('ID','MinLimit','MaxLimit')
                SO=SO.withColumn('BucketName',when(SO['NOD']>=MinLimit, lit("61+")))
                SO=SO.withColumn('BucketName',when(SO['NOD']<=MaxLimit, lit("61-"))) 
                DSE = spark.read.parquet("../../../Stage2/ParquetData/Master/DSE").drop("DBName","EntityName")
            
            #Apply Left join on SO and DSE to get keys w.r.t every dimension
                finalDF = SO.join(DSE,"DimensionSetID",'left').drop('DimensionSetID','NOD','Bucket_Sort','Transaction_Type','PromisedDeliveryDate','No_','DocumentType','CurrencyFactor')
                finalDF=finalDF.withColumn("PostingDate",to_date(col("PostingDate")))
                finalDF = finalDF.withColumn('LinkItemKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["ItemNo_"]))\
                        .withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"])).drop('ItemNo_')
                
                finalDF.coalesce(1).write.mode("overwrite").partitionBy("YearMonth").parquet("../../../Stage2/ParquetData/Sales/SalesOrder")
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Sales.Sales_Order", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
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
            
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1  SalesOrder"+" "+CompanyName+" "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
        #   
            log_dict = logger.getErrorLoggedRecord('Sales.Sales_Order', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_SalesOrder completed: ' + str((dt.datetime.now()-st).total_seconds()))