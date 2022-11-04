
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year,count,concat,concat_ws,to_date,concat,col
from pyspark.sql.types import *
import os,sys
from os.path import dirname, join, abspath
import re,os,datetime,time,sys,traceback
import datetime as dt 
from builtins import str
from datetime import date

Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
sys.path.insert(0,'../../../..')
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
st = dt.datetime.now()
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..'))
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DBEntity = DBName+EntityName

conf = SparkConf().setMaster("local[*]").setAppName("Receivables").\
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
SalesParquet_Path = abspath(join(join(dirname(__file__),'..','..','ParquetData','Receivables')))
IMfile=open(IMpath)
imonth = IMfile.read()
imonth=int(imonth)
columns = StructType([])
cy = date.today().year
cm = date.today().month
CLE =spark.createDataFrame(data = [],schema=columns)
DCLE=spark.createDataFrame(data = [],schema=columns)
SIH=spark.createDataFrame(data = [],schema=columns)
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:    
            logger = Logger()
            path = SalesParquet_Path+"\\"+"Receivables"
            if os.path.exists(path):
                
                for i in range(imonth+1):    
                    mon = cm-i
                    if mon==0:
                        cy = cy-1
                        mon = mon+12
                    if mon<10:
                        mon = '0'+str(mon)
                    mon = str(mon)    
                    ym = str(cy)+mon
                    partitionedCLE = spark.read.parquet("../../../Stage1/ParquetData/Cust_LedgerEntry//YearMonth="+ym+"")
                    partitionedCLE =partitionedCLE.withColumn('YearMonth',lit(ym))
                    CLE = partitionedCLE.unionByName(CLE, allowMissingColumns = True)
                    partitionedDCLE = spark.read.parquet("../../../Stage1/ParquetData/DetailedCust_Ledg_Entry//YearMonth="+ym+"" )
                    partitionedDCLE =partitionedDCLE.withColumn('YearMonth',lit(ym))
                    DCLE = partitionedDCLE.unionByName(DCLE, allowMissingColumns = True)
                    partitionedSIH = spark.read.parquet("../../../Stage1/ParquetData/SalesInvoiceHeader//YearMonth="+ym+"" )
                    partitionedSIH =partitionedSIH.withColumn('YearMonth',lit(ym))
                    SIH = partitionedSIH.unionByName(SIH, allowMissingColumns = True)
                CPG =  spark.read.parquet("../../../Stage1/ParquetData/CustomerPostingGroup")
                CLE =CLE.select('DocumentNo_','EntryNo_','DimensionSetID','CustomerPostingGroup','PostingDate','DBName','EntityName','YearMonth')
                CLE = CLE.withColumnRenamed('DocumentNo_','CLE_Document_No')
                SIH = SIH.select('No_','PaymentTermsCode').withColumnRenamed('No_','CLE_Document_No')
                DCLE = DCLE.select('Cust_LedgerEntryNo_','EntryType','DebitAmount')
                # select only Posting Groups and GLAccounts of Customer
                CPG = CPG.select('Code','ReceivablesAccount').withColumnRenamed('Code','CustomerPostingGroup')\
                            .withColumnRenamed('ReceivablesAccount','GLAccount')
                #apply left join on DetailedcustomerledgerEntry and CustomerLedgerEntry to get all the customers with invoices
                cond = [CLE["EntryNo_"] == DCLE["Cust_LedgerEntryNo_"]]
                finalDF = DCLE.join(CLE, cond, 'left')
                #apply left join on finaldf and SalesInvoiceHeader to get bill to customer info (column values of PaymentTermsCode)
                finalDF = finalDF.join(SIH, 'CLE_Document_No', 'left')
                #apply left join on finaldf and CustomerPostingGroup to get GLAccount w.r.t all DCLE
                finalDF = finalDF.join(CPG,'CustomerPostingGroup','left').drop('CustomerPostingGroup','CLE_Document_No','Cust_LedgerEntryNo_','EntryNo_','PaymentTermsCode',)
                DSE =spark.read.parquet("../../../Stage2/ParquetData/Master/DSE").drop("Link_BRANCHKey","Link_TARGETPRODKey","Link_OTBRANCHKey","Link_SUBBUKey","Link_SBUKey","Link_PRODUCT","Link_PRODUCTKey","Link_SALESPER","Link_VENDOR","Link_VENDORKey","Link_PROJECT","Link_PROJECTKey",'DBName','EntityName')
                #apply left join on finaldf and DSE to get keys w.r.t all Dimensions in DSE
                finalDF = finalDF.join(DSE,"DimensionSetID",'left').drop("DimensionSetID")
                finalDF = finalDF.withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"]))
                #Saving the finalDF in form of Parquet at the desired location
                finalDF.coalesce(1).write.mode("overwrite").partitionBy("YearMonth").parquet("../../../Stage2/ParquetData/Sales/Receivables")
        
            else:
                CLE = spark.read.parquet("../../../Stage1/ParquetData/Cust_LedgerEntry")
                CLE =CLE.select('DocumentNo_','EntryNo_','DimensionSetID','CustomerPostingGroup','PostingDate','DBName','EntityName','YearMonth')
                CLE = CLE.withColumnRenamed('DocumentNo_','CLE_Document_No')
                SIH = spark.read.parquet("../../../Stage1/ParquetData/SalesInvoiceHeader" )
                SIH = SIH.select('No_','PaymentTermsCode').withColumnRenamed('No_','CLE_Document_No')
                DCLE = spark.read.parquet("../../../Stage1/ParquetData/DetailedCust_Ledg_Entry" )
                DCLE = DCLE.select('Cust_LedgerEntryNo_','EntryType','DebitAmount')
                CPG =  spark.read.parquet("../../../Stage1/ParquetData/CustomerPostingGroup")
                # select only Posting Groups and GLAccounts of Customer
                CPG = CPG.select('Code','ReceivablesAccount').withColumnRenamed('Code','CustomerPostingGroup')\
                            .withColumnRenamed('ReceivablesAccount','GLAccount')
                #apply left join on DetailedcustomerledgerEntry and CustomerLedgerEntry to get all the customers with invoices
                cond = [CLE["EntryNo_"] == DCLE["Cust_LedgerEntryNo_"]]
                finalDF = DCLE.join(CLE, cond, 'left')
                #apply left join on finaldf and SalesInvoiceHeader to get bill to customer info (column values of PaymentTermsCode)
                finalDF = finalDF.join(SIH, 'CLE_Document_No', 'left')
                #apply left join on finaldf and CustomerPostingGroup to get GLAccount w.r.t all DCLE
                finalDF = finalDF.join(CPG,'CustomerPostingGroup','left').drop('CustomerPostingGroup','CLE_Document_No','Cust_LedgerEntryNo_','EntryNo_','PaymentTermsCode',)
                DSE =spark.read.parquet("../../../Stage2/ParquetData/Master/DSE").drop('DBName','EntityName')
                #apply left join on finaldf and DSE to get keys w.r.t all Dimensions in DSE
                finalDF = finalDF.join(DSE,"DimensionSetID",'left').drop("DimensionSetID")
                finalDF = finalDF.withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"]))
                #Saving the finalDF in form of Parquet at the desired location
                finalDF.coalesce(1).write.mode("overwrite").partitionBy("YearMonth").parquet("../../../Stage2/ParquetData/Sales/Receivables")
                logger.endExecution()
                
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                
                log_dict = logger.getSuccessLoggedRecord("Sales.Receivables", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1  Receivables"+" "+CompanyName+" "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            log_dict = logger.getErrorLoggedRecord('Sales.Receivables', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_Receivables completed: ' + str((dt.datetime.now()-st).total_seconds()))