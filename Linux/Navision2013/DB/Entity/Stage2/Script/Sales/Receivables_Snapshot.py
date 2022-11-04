from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,min as min_,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,last_day,datediff,ltrim,greatest
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

def sales_Receivables_Snapshot(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

    cdate = datetime.datetime.now().strftime('%Y-%m-%d')

    try:
        
        CLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Cust_ Ledger Entry")
        SIH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Invoice Header")
        DCLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Detailed Cust_ Ledg_ Entry")
        CPG_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer Posting Group")
        CD_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Collection Details")  
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
            CurrentYear = Datelog.split('-')[0]
            CurrentMonth = Datelog.split('-')[1]
            YM = int(CurrentYear+CurrentMonth)
            
            cle = ToDFWitoutPrefix(sqlCtx, hdfspath, CLE_Entity,True)
            sih = ToDFWitoutPrefix(sqlCtx, hdfspath, SIH_Entity,False)
            
            sih = sih.select('No_','PaymentTermsCode').withColumnRenamed('No_','Sales_Invoice_No')
            
            dcle = ToDFWitoutPrefix(sqlCtx, hdfspath, DCLE_Entity,True)
            dcle = dcle.withColumn('AmountLCY',dcle['AmountLCY'].cast('decimal(20,4)'))\
                        .withColumn('Amount',dcle['Amount'].cast('decimal(20,4)'))
        
            cpg = ToDFWitoutPrefix(sqlCtx, hdfspath, CPG_Entity,False)
            CD = ToDFWitoutPrefix(sqlCtx, hdfspath, CD_Entity,False)
            
            CollectionDetails = CD.filter(CD["InvoiceNo_"]!='')\
                            .select('InvoiceNo_','DocumentationDueDate','ExpectedCollectionDate'
                                    ,'ExpectedCollectionDate2','ExpectedCollectionDate3','ExpectedCollectionDate4')\
                           .withColumnRenamed('InvoiceNo_','CollectionInvoiceNo').withColumnRenamed('DocumentationDueDate','TargetDate')
                          
            CollectionDetails = CollectionDetails.withColumn('ECD',greatest(CollectionDetails['ExpectedCollectionDate'],
                                                                                               CollectionDetails['ExpectedCollectionDate2'],
                                                                                               CollectionDetails['ExpectedCollectionDate3'],
                                                                                               CollectionDetails['ExpectedCollectionDate4']))
            
            
            CollectionDetails = CollectionDetails.drop('ExpectedCollectionDate').drop('ExpectedCollectionDate2').drop('ExpectedCollectionDate3').drop('ExpectedCollectionDate4')
            CollectionDetails = CollectionDetails.withColumnRenamed('ECD','ExpectedCollectionDate')                               
            CollectionDetails = CollectionDetails.withColumn('TargetDate',when(year(CollectionDetails['TargetDate'])==1753, lit("")).otherwise(CollectionDetails['TargetDate']))
            CollectionDetails = CollectionDetails.withColumn('TargetDate',CollectionDetails.TargetDate.cast('date'))
            CollectionDetails = CollectionDetails.filter(~(CollectionDetails['CollectionInvoiceNo'].isNull()))
            
            cle = cle.withColumn("Link_Customer",when(col("CustomerNo_")=='',"NA").otherwise(col("CustomerNo_")))\
                     .withColumn("Link_SalesPerson",when(col("SalespersonCode")=='',"NA").otherwise(col("SalespersonCode")))\
                     .withColumn("Due_Date",to_date(col("DueDate"))).drop('DBName','EntityName')
            cle = Kockpit.RENAME(cle,{'EntryNo_':'CLE_No','DocumentNo_':'CLE_Document_No','PostingDate':'CLE_Posting_Date','CustomerPostingGroup':'Customer_Posting_Group',\
                        'DimensionSetID':'DimSetID','ExternalDocumentNo_':'ExternalDocumentNo'})
            cle = cle.drop('CurrencyCode')
            
            cle = cle.join(sih,cle['CLE_Document_No']==sih['Sales_Invoice_No'],'left').drop('Sales_Invoice_No')
            dcle = dcle.filter(year(col("PostingDate"))!='1753')
            dcle = dcle.withColumn("YearMonth",concat(year(dcle.PostingDate),month(dcle.PostingDate)))\
                        .withColumn("Today",lit(Datelog))
            dcle = dcle.withColumn("TodayYM",concat(year(dcle.Today),month(dcle.Today)))
            dcle = dcle.withColumn("Original_Amount",when(col("EntryType")==1,col("Amount")))\
                       .withColumn("DCLE_Posting_Date",to_date(col("PostingDate")))\
                       .withColumn("DCLE_Monthend_Posting_Date",when(dcle.YearMonth==dcle.TodayYM, dcle.Today).otherwise(last_day(col("PostingDate"))))\
                       .withColumn("Transaction_Type",lit("CLE Entry"))\
                       .withColumn("Remaining_Amount",col("AmountLCY")).drop("DBName","EntityName","Today","TodayYM","YearMonth")
            dcle = Kockpit.RENAME(dcle,{'Cust_LedgerEntryNo_':'DCCLE_No','DocumentNo_':'DCLE_Document_No','EntryNo_':'DCLE_No'})
            dcle.show()
            
            cond = [cle.CLE_No==dcle.DCCLE_No]
            df = Kockpit.RJOIN(cle,dcle,cond)
            
            df1 = Kockpit.RENAME(cpg,{'Code':'Customer_Posting_Group','ReceivablesAccount':'GLAccount'})
            
            cond = [df.Customer_Posting_Group == df1.Customer_Posting_Group]
            df2 = Kockpit.LJOIN(df,df1,cond)

            Query_GLMapping="(SELECT * from public."+chr(34)+"tblGLAccountMapping"+chr(34)+") AS COA"
            GLRange = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_GLMapping,\
                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            GLRange = GLRange.filter(GLRange['DBName'] == DBName ).filter(GLRange['EntityName'] == EntityName ).filter(GLRange['GL Range Category']== 'Customer')
            GLRange = RENAME(GLRange,{'From GL Code':'FromGL','To GL Code':'ToGL','GL Range Category':'GLCategory'})
            GLRange = GLRange.select("GLCategory","FromGL","ToGL")
            
            Range=df2['GLAccount']!=0
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                if i==0:
                        FromGL = "%s"%GLRange.select('FromGL').collect()[0]["FromGL"]
                        ToGL = "%s"%GLRange.select('ToGL').collect()[0]["ToGL"]
                        Range = (df2['GLAccount']>=FromGL) & (df2['GLAccount']<=ToGL)
                else:
                        FromGL = "%s"%GLRange.select('FromGL').collect()[i]["FromGL"]
                        ToGL = "%s"%GLRange.select('ToGL').collect()[i]["ToGL"]
                        Range = (Range) | ((df2['GLAccount']>=FromGL) & (df2['GLAccount']<=ToGL))
            df2 = df2.filter(Range)
            
            df4 = df2.groupBy('CLE_No').agg({'Remaining_Amount':'sum'})
            df4 = Kockpit.RENAME(df4,{'sum(Remaining_Amount)':'Remaining_Amount','CLE_No':'CLENo'})
            df4 = df4.select('CLENo','Remaining_Amount').filter('Remaining_Amount!=0')

            
            df_DCLE_Temp1=df.select(df2.CLE_No,df2.CLE_Document_No,df2.DCLE_Monthend_Posting_Date)
            
            cond1 = [df_DCLE_Temp1.CLE_No==df4.CLENo]
            Df_min_max_date=Kockpit.LJOIN(df_DCLE_Temp1,df4,cond1)
            Df_min_max_date=Df_min_max_date.select(df_DCLE_Temp1.CLE_No,df_DCLE_Temp1.CLE_Document_No,df_DCLE_Temp1.DCLE_Monthend_Posting_Date,df4.Remaining_Amount).distinct()
            Df_min_max_date=Df_min_max_date.select('CLE_No','CLE_Document_No','DCLE_Monthend_Posting_Date','Remaining_Amount')
            
            sqldf = Df_min_max_date.withColumn('Max_MonthEnd',when(Df_min_max_date['Remaining_Amount']!=0, datetime.datetime.now().date().replace(month=12, day=31))\
                    .otherwise(Df_min_max_date['DCLE_Monthend_Posting_Date']))
            sqldf = sqldf.groupby('CLE_No','CLE_Document_No','Remaining_Amount').agg({'DCLE_Monthend_Posting_Date':'min','Max_MonthEnd':'max'})
            sqldf = Kockpit.RENAME(sqldf,{'CLE_No':'TempCLE_No','min(DCLE_Monthend_Posting_Date)':'Min_MonthEnd','max(Max_MonthEnd)':'Max_MonthEnd'})
            
            Query_Company="(SELECT *\
                        FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Company,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            df = Company.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            
            if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                    UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            
            Calendar_EndDate_conf=df.select(df.EndDate).collect()[0]["EndDate"]
            Calendar_EndDate_conf = datetime.datetime.strptime(Calendar_EndDate_conf,"%m/%d/%Y").date()
            Calendar_EndDate_file=datetime.datetime.strptime(cdate,"%Y-%m-%d").date()
            Calendar_EndDate=min(Calendar_EndDate_conf,Calendar_EndDate_file)
            days = (Calendar_EndDate-UIStartDate).days
            
            def last_day_of_month(date):
                    if date.month == 12:
                            return date.replace(day=31)
                    return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
            
            def daterange(start_date,end_date):
                    for n in range(int ((end_date - start_date).days)):
                            yield start_date + timedelta(n)
            
            data =[]
            for single_date in daterange(UIStartDate, Calendar_EndDate+timedelta(days=1)):
                    data.append({'Link_date':single_date})
            
            schema = StructType([StructField("Link_date", DateType(),True)])
            records=spark.createDataFrame(data,schema)
            
            records=records.select(last_day(records.Link_date).alias('Link_date')).distinct().sort('Link_date')
            
            records=records.withColumn("Link_date", \
                                      when(records["Link_date"] == last_day_of_month(Calendar_EndDate), Calendar_EndDate).otherwise(records["Link_date"]))
            
            sqldf = Kockpit.JOIN(sqldf,records)
            sqldf = sqldf.select('TempCLE_No','CLE_Document_No','Min_MonthEnd','Max_MonthEnd','Link_date')
            
            sqldf=sqldf.filter(sqldf['Link_date']<= sqldf['Max_MonthEnd']).filter(sqldf['Link_date']>= sqldf['Min_MonthEnd'])\
                       .select('TempCLE_No','CLE_Document_No','Link_date')
            sqldf = Kockpit.RENAME(sqldf,{'Link_date':'DCLE_MonthEnd'})
            
            CLE_DCLE_Joined = df2.select('DimSetID','CLE_No','DCLE_Posting_Date','Due_Date','DocumentType','ExternalDocumentNo','CurrencyCode','PaymentTermsCode','Remaining_Amount','Original_Amount')
            cond = [sqldf.TempCLE_No == CLE_DCLE_Joined.CLE_No]
            ARsnapshots = Kockpit.LJOIN(sqldf,CLE_DCLE_Joined,cond)
            ARsnapshots=ARsnapshots.select('DimSetID','TempCLE_No','DCLE_Posting_Date','Due_Date','DocumentType','ExternalDocumentNo','CurrencyCode','PaymentTermsCode','Remaining_Amount','Original_Amount','CLE_Document_No','DCLE_MonthEnd')\
                                   .filter(ARsnapshots['DCLE_Posting_Date']<= ARsnapshots['DCLE_MonthEnd'])                    
            
            ARsnapshots = ARsnapshots.groupBy('DimSetID','TempCLE_No','CLE_Document_No','DCLE_MonthEnd','Due_Date','DocumentType','ExternalDocumentNo','PaymentTermsCode','CurrencyCode').agg({'Remaining_Amount':'sum','Original_Amount':'sum'})
            ARsnapshots = Kockpit.RENAME(ARsnapshots,{'sum(Remaining_Amount)':'Remaining_Amount','sum(Original_Amount)':'Original_Amount'})
            
            CLE_DCLE_Joined = df2.select('CLE_No','Link_Customer','CLE_Posting_Date','Link_SalesPerson','AdvanceCollection').distinct()
            
            ARsnapshots = ARsnapshots.join(CLE_DCLE_Joined,ARsnapshots['TempCLE_No']==CLE_DCLE_Joined['CLE_No'],'left')
            
            ARsnapshots = ARsnapshots.join(CollectionDetails,ARsnapshots["CLE_Document_No"]==CollectionDetails["CollectionInvoiceNo"],'left')
            
            ARsnapshots = ARsnapshots.withColumn("NOD_AR_TargetDate",datediff(ARsnapshots['DCLE_MonthEnd'],ARsnapshots['TargetDate']))
            ARsnapshots = ARsnapshots.na.fill({'NOD_AR_TargetDate':-1})
            
            ARsnapshots = ARsnapshots.withColumn("NOD_AR_Due_Date",datediff(ARsnapshots['DCLE_MonthEnd'],ARsnapshots['Due_Date']))\
                                .withColumn("NOD_AR_Posting_Date",datediff(ARsnapshots['DCLE_MonthEnd'],ARsnapshots['CLE_Posting_Date']))\
                                .withColumn("TransactionType",lit('CLE_Entry'))\
                                .withColumn("AR_AP_Type",when(ARsnapshots['Remaining_Amount']<0, lit('Adv/UnAdj')).otherwise(lit('AR')))
            ARsnapshots = RENAME(ARsnapshots,{'CLE_Document_No':'Document_No','DCLE_MonthEnd':'Link_Date'})
            
            ARsnapshots.cache()
            list = ARsnapshots.select('NOD_AR_Posting_Date').distinct().rdd.flatMap(lambda x: x).collect()
            
            TargetDate_List = ARsnapshots.select('NOD_AR_TargetDate').distinct().rdd.flatMap(lambda x: x).collect()
            ############################### Buckets ########################################
            Query_ARBucket="(SELECT *\
                        FROM "+chr(34)+"tblARBucket"+chr(34)+") AS df"
            ARBucket = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_ARBucket,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            Bucket = ARBucket.withColumnRenamed("Lower Limit","LowerLimit")\
                            .withColumnRenamed("Upper Limit","UpperLimit")\
                            .withColumnRenamed("Bucket Name","BucketName")\
                            .withColumnRenamed("Bucket Sort","BucketSort")
            Bucket = Bucket.select("LowerLimit","UpperLimit","BucketName","BucketSort")
            #Bucket.show()
            #sys.exit()
            Bucket = Bucket.collect()
            
            TempBuckets = BUCKET(list,Bucket,spark)
            
            cond = [ARsnapshots.NOD_AR_Posting_Date == TempBuckets.NOD]
            ARsnapshots = LJOIN(ARsnapshots,TempBuckets,cond)
            ARsnapshots = RENAME(ARsnapshots,{'Link_Customer':'LinkCustomer','Link_SalesPerson':'LinkSalesPerson'})
            
            TempBuckets = BUCKET(TargetDate_List,Bucket,spark)
            TempBuckets = TempBuckets.withColumnRenamed('Bucket_Sort','Collectable_Bucket_Sort')\
                                    .withColumnRenamed('Interval','Collectable_Interval')\
                                    .withColumnRenamed('NOD','Collectable_NOD')
            #sys.exit()
            cond = [ARsnapshots.NOD_AR_TargetDate == TempBuckets.Collectable_NOD]
            ARsnapshots = LJOIN(ARsnapshots,TempBuckets,cond)
            
            ARsnapshots = ARsnapshots.withColumn("AdvanceFlag",when(ARsnapshots["AdvanceCollection"]==1,lit('Advance')).otherwise(lit('NA')))\
                                    .withColumnRenamed('DimSetID','DimensionSetID')
            
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            finalDF = ARsnapshots.join(DSE,"DimensionSetID",'left')
            
            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Sales.Receivables_Snapshot", mode='overwrite', properties=PostgresDbInfo.props)

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Sales.Receivables_Snapshot", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Sales.Receivables_Snapshot', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('sales_Receivables_Snapshot completed: ' + str((dt.datetime.now()-st).total_seconds()))
    

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Receivables_Snapshot")
    sales_Receivables_Snapshot(sqlCtx, spark)