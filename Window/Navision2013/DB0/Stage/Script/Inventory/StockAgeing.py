'''
Created on 6 Feb 2019
@author: Abhishek - Jaydip
'''

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,to_date,lit,last_day,datediff
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

def inventory_Stock(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

    cdate = datetime.datetime.now().strftime('%Y-%m-%d')
    
    def last_day_of_month(date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)
    
    try:
        
        VE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Value Entry") 
        
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
            
            Query_FieldSelection="(SELECT *\
                        FROM "+chr(34)+"tblFieldSelection"+chr(34)+") AS df"
            table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_FieldSelection,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            table = table.filter(table['Flag'] == 1).filter(table['TableName'] == 'Value Inventory')
            FieldName = table.select("FieldType").collect()[0]["FieldType"]
            FieldName = re.sub("[\s+]",'',FieldName)

            ve = ToDFWitoutPrefix(sqlCtx, hdfspath, VE_Entity,False)
            purchase_entries = ve.filter(ve["InvoicedQuantity"]!=0) #Need to Check Line down below changes done 9th Sep
            purchase_entries = purchase_entries.select('ItemLedgerEntryNo_','DocumentNo_','PostingDate')\
                                            .withColumnRenamed('ItemLedgerEntryNo_','VE_ILE_NO').withColumnRenamed('DocumentNo_','VEDocumentNo')\
                                            .withColumnRenamed('PostingDate','VEPostingDate')

            ve = ve.filter((ve.CapacityLedgerEntryNo_==0) & (year(ve.PostingDate)!=1753))
            ve = ve.withColumn("PostingDate",to_date(ve.PostingDate))
            ve = ve.withColumnRenamed("GlobalDimension1Code","SBU_Code").withColumnRenamed('DocumentNo_','Document_No')
            ve = ve.withColumnRenamed("GlobalDimension14Code","BU_Code")
        
            ve = ve.groupby("ItemLedgerEntryNo_","SBU_Code","BU_Code","PostingDate","ItemNo_","LocationCode").agg({"CostPostedtoG_L":"sum",FieldName:"sum","ValuedQuantity":"avg"})\
                    .drop("CostPostedtoG_L",FieldName,"ValuedQuantity")
            ve.persist(StorageLevel.MEMORY_AND_DISK)
            max_postingdate = max(ve.select('PostingDate').rdd.flatMap(lambda x: x).collect())
            ve = RENAME(ve,{"ItemLedgerEntryNo_":"ILENo","sum(CostPostedtoG_L)":"VEValue","sum("+FieldName+")":"VEQuantity","avg(ValuedQuantity)":"ValuedQuantity"})
            ItemInv = ve.withColumn("LinkItem",when(ve.ItemNo_=='',lit("NA")).otherwise(ve.ItemNo_)).drop("ItemNo_")\
                .withColumn("Monthend",when(ve.PostingDate>=max_postingdate.replace(day=1),max_postingdate).otherwise(last_day(ve.PostingDate)))\
                .withColumn("LocationCode",when(ve.LocationCode=='',lit("NA")).otherwise(ve.LocationCode))
            
            IAP_query="(SELECT  "+chr(34)+"ItemLedgerEntryNo_"+chr(34)+" AS "+chr(34)+"ILENo1"+chr(34)+",\
                 "+chr(34)+"Quantity"+chr(34)+" AS "+chr(34)+"IAEQuantity"+chr(34)+",\
                 "+chr(34)+"RefNo"+chr(34)+" \
                FROM inventory.iap_inc) AS IAP"
            IAP0 = sqlCtx.read.format("jdbc").options(url=postgresUrl, dbtable=IAP_query,\
                      user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],\
                      driver= PostgresDbInfo.props["driver"],partitionColumn=chr(34)+"ILENo1"+chr(34),\
                      lowerBound=0, upperBound=1791335, numPartitions=128).load()
            
            IAP1=IAP0.groupBy('ILENo1').agg({'RefNo': 'count'})
            IAP1 = RENAME(IAP1,{"ILENo1":"ILENo2","count(RefNo)":"RefNoCount"})
            CountJoin=[IAP0.ILENo1==IAP1.ILENo2]
            IAP = LJOIN(IAP0,IAP1,CountJoin)
            IAP = IAP.drop("ILENo2")

            Cond=[ItemInv.ILENo == IAP.ILENo1]
            ItemInv1 = LJOIN(ItemInv,IAP,Cond)
            ItemInv1 = ItemInv1.drop("ILENo1").na.fill({'RefNoCount':0,'RefNo':0})
            #ItemInv1.filter(ItemInv1['RefNo']==21).show()
            #sys.exit()
            ItemInv2 = ItemInv1.withColumn("RefNo1",when(ItemInv1.RefNo==0,ItemInv1.ILENo).otherwise(ItemInv1.RefNo))\
                                .withColumn("StockValue",when(ItemInv1.RefNoCount<2,ItemInv1.VEValue)\
                                .otherwise(when(ItemInv1.VEQuantity!=0,((ItemInv1.VEValue)/(ItemInv1.VEQuantity/ItemInv1.IAEQuantity))).otherwise((ItemInv1.VEValue)/(ItemInv1.ValuedQuantity/ItemInv1.IAEQuantity))))\
                                .withColumn("StockQuantity",when(ItemInv1.RefNoCount<2,ItemInv1.VEQuantity).otherwise(when(ItemInv1.VEQuantity!=0,ItemInv1.IAEQuantity).otherwise(lit(0))))\
                                .drop("RefNo")
            ItemInv2 = RENAME(ItemInv2,{"RefNo1":"RefNo"})
            InDates = ItemInv1.select("ILENo","PostingDate")
            InDates = InDates.groupby("ILENo").agg({"PostingDate":"min"})
            InDates = RENAME(InDates,{"ILENo":"RefNo2","min(PostingDate)":"InDate"})
            CondMinDate=[ItemInv2.RefNo == InDates.RefNo2]
            ItemInv3 = LJOIN(ItemInv2,InDates,CondMinDate)
            ItemInv3 = ItemInv3.drop("RefNo2")
            #ItemInv3.filter(ItemInv3['RefNo']==21).show()
            #sys.exit()
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
            
            data =[]
            for single_date in daterange(UIStartDate, Calendar_EndDate):
                data.append({'RollupMonth':single_date})
        
            schema = StructType([
                StructField("RollupMonth", DateType(),True)
            ])
            records=sqlCtx.createDataFrame(data,schema)
            records=records.select(last_day(records.RollupMonth).alias('RollupMonth')).distinct().sort('RollupMonth')
            records=records.withColumn("RollupMonth", \
                          when(records["RollupMonth"] == last_day_of_month(Calendar_EndDate_file), Calendar_EndDate_file).otherwise(records["RollupMonth"]))
            
            records.persist(StorageLevel.MEMORY_AND_DISK)
            ItemInv5=ItemInv3.join(records).where(records.RollupMonth>=ItemInv3.Monthend)
            #ItemInv5.filter(ItemInv1['RefNo']==21).show()
            #sys.exit()
            
            ItemInv5 = ItemInv5.select("LinkItem","LocationCode","SBU_Code","BU_Code","RefNo","InDate","RollupMonth","StockQuantity","StockValue")
            ItemInv5 = RENAME(ItemInv5,{"LocationCode":"LinkLocation","RefNo":"RefILENo"})
            
            ItemInv6 = ItemInv5.groupby("LinkItem","LinkLocation","RefILENo","SBU_Code","BU_Code","InDate","RollupMonth")\
                                .agg({"StockQuantity":"sum","StockValue":"sum"})\
                                .drop("StockQuantity","StockValue")
            ItemInv6 = RENAME(ItemInv6,{"sum(StockQuantity)":"StockQuantity","sum(StockValue)":"StockValue","RollupMonth":"Monthend"})
            
            ItemInv7 = ItemInv6.filter((ItemInv6.StockQuantity!=0) | (ItemInv6.StockValue!=0))
            #print(ItemInv7.count())
            #sys.exit()
            
            ItemInv7 = ItemInv7.withColumn("PostingDate",ItemInv7.Monthend)\
                        .withColumn("LinkDate",ItemInv7.Monthend)\
                        .withColumn("StockAge",datediff(ItemInv7.Monthend,ItemInv7.InDate))\
                        .withColumn("TransactionType",lit("StockAgeing")).drop('Monthend')
            ItemInv7=ItemInv7.na.fill({'StockAge':0})
            ItemInv7.persist(StorageLevel.MEMORY_AND_DISK)
            list = ItemInv7.select("StockAge").distinct().rdd.flatMap(lambda x: x).collect()
            
            Query_InvBucket="(SELECT *\
                        FROM "+chr(34)+"tblInventoryBucket"+chr(34)+") AS df"
            InvBucket = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_InvBucket,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            Bucket = InvBucket.withColumnRenamed("Lower Limit","LowerLimit")\
                            .withColumnRenamed("Upper Limit","UpperLimit")\
                            .withColumnRenamed("Bucket Name","BucketName")\
                            .withColumnRenamed("Bucket Sort","BucketSort")
            Bucket = Bucket.select("LowerLimit","UpperLimit","BucketName","BucketSort")
            Bucket = Bucket.collect()
            TempBuckets = BUCKET(list,Bucket,spark)
            
            CondBucket = [ItemInv7.StockAge == TempBuckets.NOD]
            ItemInv8 = LJOIN(ItemInv7,TempBuckets,CondBucket)
            
            
            ItemInv8 = ItemInv8.na.fill({'LinkLocation':'NA','LinkDate':'NA','LinkItem':'NA'})
            
            ItemInv8 = ItemInv8.withColumn("DBName",lit(DBName)).withColumn("EntityName",lit(EntityName))
            
            ItemInv8 = ItemInv8.withColumn("LinkLocationKey",concat_ws("|",ItemInv8.DBName,ItemInv8.EntityName,ItemInv8.LinkLocation))\
                            .withColumn("LinkDateKey",concat_ws("|",ItemInv8.DBName,ItemInv8.EntityName,ItemInv8.LinkDate))\
                            .withColumn("LinkItemKey",concat_ws("|",ItemInv8.DBName,ItemInv8.EntityName,ItemInv8.LinkItem))\
                            .withColumn("LinkSBUKey",concat_ws("|",ItemInv8.DBName,ItemInv8.EntityName,ItemInv8.SBU_Code))\
                            .withColumn("LinkBUKey",concat_ws("|",ItemInv8.DBName,ItemInv8.EntityName,ItemInv8.BU_Code))
            
            finalDF = ItemInv8.join(purchase_entries,ItemInv8["RefILENo"]==purchase_entries["VE_ILE_NO"],'left')
            
            finalDF.persist(StorageLevel.MEMORY_AND_DISK)
            finalDF.write.jdbc(url=postgresUrl, table="Inventory.Inventory_Snapshot", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Inventory.Inventory_Snapshot", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Inventory.Inventory_Snapshot', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('Inventory_Snapshot completed: ' + str((dt.datetime.now()-st).total_seconds()))
    

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Inventory_Snapshot")
    inventory_Stock(sqlCtx, spark)