from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc
import datetime, time
import datetime as dt
import re

from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath

helpersDir ='/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers import udf

def purchase_PurchaseInvoice(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:

        if datetime.date.today().month>int(MnSt)-1:
            UIStartYr=datetime.date.today().year-int(yr)+1
        else:
            UIStartYr=datetime.date.today().year-int(yr)
        
        pilEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Inv_ Line") 
        pihEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Inv_ Header")
        gpsEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "General Posting Setup")  
        pldEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Posted Str Order Line Details")
        veEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Value Entry")
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            x = entityLocation.index("E")
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)

            pil = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pilEntity, True)
            pih = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pihEntity, True)
            gps = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, gpsEntity, True)
            pld = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pldEntity, True)
            VE = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, veEntity, True)
            
            pil = pil.filter((year(pil.PostingDate)!=1753) & (pil.Quantity!=0) & (pil.Type!=4))
            pil = pil.na.fill({'Gen_Bus_PostingGroup':'NA','Gen_Prod_PostingGroup':'NA','DocumentNo_':'NA','LineNo_':'NA','PostingDate':'NA'})
            Line = pil\
                    .withColumn("No",when(pil.Type=='1',pil['No_'].cast('int')))\
                    .withColumn("GL_Link",concat_ws('|',lit(entityLocation),pil.Gen_Bus_PostingGroup.cast('string'), pil.Gen_Prod_PostingGroup.cast('string')))\
                    .withColumn("LinkValueEntry",concat_ws('|',pil.DocumentNo_.cast('string'),pil.LineNo_.cast('string'),to_date(pil.PostingDate).cast('string')))\
                    .withColumn("Link_GD",concat_ws('-',pil.DocumentNo_.cast('string'),pil.LineNo_.cast('string')))\
                    .withColumn("LinkLocationCode",when(pil.LocationCode=='',lit("NA")).otherwise(pil.LocationCode))\
                    
            Line = udf.RENAME(Line,{"DimensionSetID":"DimSetID","DocumentNo_":"Document_No","Amount":"PurchaseAmount"})
            Line = Line.drop('PostingDate')
            
            Header = pih\
                    .withColumn("LinkVendor",when(pih['Pay-toVendorNo_']=='',lit("NA")).otherwise(pih["Pay-toVendorNo_"])).drop("Pay-toVendorNo_")\
                    .withColumn("MonthNum", month(pih.PostingDate))\
                    .withColumn("LinkPurchaseRep",when(pih.PurchaserCode=='',lit("NA")).otherwise(pih.PurchaserCode)).drop("PurchaserCode")\
                        
            Header = udf.RENAME(Header,{"No_":"Purchase_No","LocationCode":"HeaderLocationCode"})

            Monthsdf = udf.MONTHSDF(sqlCtx)
            mcond = [Header.MonthNum==Monthsdf.MonthNum1]
            Header = udf.LJOIN(Header,Monthsdf,mcond)
            Header = Header.drop('MonthNum','MonthNum1')
            Header = Header.na.fill({'Purchase_No':'NA'})
            
            GL_Master = gps.withColumn("GL_LinkDrop",concat_ws('|',lit(entityLocation),gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
                                    .withColumn("GLAccount",when(gps.Purch_Account=='',0).otherwise(gps.Purch_Account)).drop('Purch_Account')

            Line = Line.withColumn('Document_No_key',concat_ws('|',lit(entityLocation),Line.Document_No))
            Header = Header.withColumn('Purchase_No_key',concat_ws('|',lit(entityLocation),Header.Purchase_No))
            cond = [Line.Document_No_key == Header.Purchase_No_key]

            Purchase = udf.LJOIN(Line,Header,cond)
            
            data=[{'GLCategory':'Purchase Trading','FromGL':402000,'ToGL':406500}]
            GLRange = sqlCtx.createDataFrame(data)

            cond1 = [Purchase.GL_Link == GL_Master.GL_LinkDrop]
            Purchase = udf.LJOIN(Purchase,GL_Master,cond1)
            Purchase = Purchase.drop("GL_LinkDrop","GL_Link","Sales_No","Sales_No_key","Document_No_key")
            
            pld = pld.filter((pld.Type==2) & (pld.DocumentType==2) & (pld.Tax_ChargeType==0))
            LineDetails = pld.withColumn("Link_GDDrop",concat_ws('-',pld.InvoiceNo_,pld.LineNo_)).drop("InvoiceNo_","LineNo_")\
                                                    .withColumnRenamed("AccountNo_","AccountNo")
            
            Range='1=1'
            Range1='1=1'
            Range2='1=1'
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                if i==0:
                    Range = (Purchase.GLAccount>=GLRange.select('FromGL').collect()[0]['FromGL']) \
                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[0]['ToGL'])
        
                    Range1 = (Purchase.No>=GLRange.select('FromGL').collect()[0]['FromGL']) \
                        & (Purchase.No<=GLRange.select('ToGL').collect()[0]['ToGL'])
        
                else:
                    Range = (Range) | ((Purchase.GLAccount>=GLRange.select('FromGL').collect()[i]['FromGL']) \
                                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[i]['ToGL']))
        
                    Range1 = (Range1) | ((Purchase.No>=GLRange.select('FromGL').collect()[i]['FromGL']) \
                                        & (Purchase.No<=GLRange.select('ToGL').collect()[i]['ToGL']))
        
            Purchase = Purchase.filter(((Purchase.Type!=2) | ((Purchase.Type==2) & (Range))) & ((Purchase.Type!=1) | ((Purchase.Type==1) & (Range1))))
                    
            Purchase = Purchase.withColumn("PurchaseAccount",when(Purchase.Type==2,Purchase.GLAccount).otherwise(when(Purchase.Type==1,Purchase.No).otherwise(lit(1))))
            
            Range2 = LineDetails['AccountNo']!=0
            NoOfRows = GLRange.count()
            for j in range(0,NoOfRows):
                if i==0:
                    FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[0]['FromGL']
                    ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[0]['ToGL']
                    Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
                else:
                    FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[i]['FromGL']
                    ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[i]['ToGL']
                    Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
        
            LineDetails = LineDetails[Range2]
            LineDetails=LineDetails.groupby('Link_GDDrop').sum('Amount').withColumnRenamed('sum(Amount)','ChargesfromVendor')
            cond2 = [Purchase.Link_GD == LineDetails.Link_GDDrop]
            Purchase = udf.LJOIN(Purchase,LineDetails,cond2)
            Purchase = Purchase.drop("Link_GDDrop")
            
            Purchase = Purchase.withColumn('TransactionType',lit('Invoice'))
            Purchase = Purchase.na.fill({'LinkLocationCode':'NA'})
            Purchase = Purchase.na.fill({'GLAccount':''})
            Purchase = Purchase.na.fill({'CurrencyFactor':0})
            Purchase = Purchase.na.fill({'ChargesfromVendor':0})
            Purchase = Purchase.withColumn('GLAccountNo',when(Purchase.GLAccount=='', Purchase.No).otherwise(Purchase.GLAccount))\
                            .withColumn('PayableAmount',when(Purchase.CurrencyFactor==0,when(Purchase.ChargesfromVendor==0,Purchase.PurchaseAmount).otherwise(Purchase.PurchaseAmount+Purchase.ChargesfromVendor))\
                            .otherwise(when(Purchase.ChargesfromVendor==0,Purchase.PurchaseAmount/Purchase.CurrencyFactor).otherwise((Purchase.PurchaseAmount+Purchase.ChargesfromVendor)/(Purchase.CurrencyFactor))))
            
            Purchase = Purchase.withColumn('LinkLocation',when(Purchase.LinkLocationCode=='NA', Purchase.HeaderLocationCode).otherwise(Purchase.LinkLocationCode))
            Purchase = Purchase.drop('PurchaseAccount')
            Purchase = Purchase.withColumn('LinkDate',to_date(Purchase['PostingDate']))
            FieldName = "CostAmountExpected"
            '''
            Field = sqlctx.read.parquet(hdfspath+"/Data/FieldSelection")
            Field = Field.filter(Field['Flag'] == 1).filter(Field['TableName'] == 'VE FieldSelection')
            Field = Field.select("FieldType","Flag","TableName")
            FieldName = Field.select('FieldType').collect()[0]["FieldType"]
            FieldName = re.sub('[\s+]', '', FieldName)
            '''

            ValueEntry = VE.withColumn("LinkDate",to_date(VE.PostingDate))\
                        .withColumn("LinkValueEntry1",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))\
                        .withColumn("CostAmount",when(col(FieldName)=='CostAmountExpected',when(VE.CostAmountActual=='0',VE.CostAmountExpected*(-1)).otherwise(VE.CostAmountActual*(-1))).otherwise(col(FieldName)*(-1)))
            
            ValueEntry = udf.RENAME(ValueEntry,{"DimensionSetID":"DimSetID","ItemNo_":"LinkItem","DocumentNo_":"DocumentNo","ItemLedgerEntryNo_":"LinkILENo"})
            
            ValueEntry1 = ValueEntry.select('LinkValueEntry1','CostAmount').groupby('LinkValueEntry1')\
                                    .agg(sum("CostAmount").alias("CostAmount"))
            
            JoinCOGS = [Purchase.LinkValueEntry == ValueEntry1.LinkValueEntry1]
            Purchase = udf.LJOIN(Purchase,ValueEntry1,JoinCOGS)
            Purchase = Purchase.drop("PurchaseAmount","LinkValueEntry1")

            Documents = Purchase.select('Document_No').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1)).withColumnRenamed('Document_No','Document_NoDrop')
            JoinDocs = [ValueEntry.DocumentNo == Documents.Document_NoDrop]
            ValueEntry = udf.LJOIN(ValueEntry,Documents,JoinDocs)
            ValueEntry = ValueEntry.drop("Document_NoDrop")
            
            SysEntries = Purchase.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            JoinSysEntry = [ValueEntry.LinkValueEntry1 == SysEntries.LinkValueEntry]
            ValueEntry = udf.LJOIN(ValueEntry,SysEntries,JoinSysEntry)
            ValueEntry = ValueEntry.drop("LinkValueEntry").withColumnRenamed("LinkValueEntry1","LinkValueEntry")

            ValueEntry = ValueEntry.filter((ValueEntry.SysDocFlag==1) & (ValueEntry.SysValueEntryFlag.isNull()))
            ValueEntry = udf.RENAME(ValueEntry,{'DocumentLineNo_':'LineNo','EntryNo_':'Link_GD','LocationCode':'LinkLocation','DocumentNo':'Document_No'})
            ValueEntry = ValueEntry.select('DimSetID','Document_No','LinkItem','LinkValueEntry','InvoicedQuantity','VariantCode','Gen_Bus_PostingGroup','Gen_Prod_PostingGroup',\
                                    'PostingDate','LinkDate','CostAmountActual','CostAmountExpected','LineNo','Link_GD','LinkLocation').withColumn('Trn_Quantity',ValueEntry.InvoicedQuantity)
            
            ValueEntry = ValueEntry.withColumn('TransactionType',lit('RevaluationEntries')).withColumn('RevenueType',lit('Other'))
            
            dse = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            dse = udf.RENAME(dse,{"DimensionSetID":"DimSetID1"})

            scond = [Purchase.DimSetID==dse.DimSetID1]
            Purchase = udf.LJOIN(Purchase,dse,scond)
            Purchase = Purchase.drop('DimSetID1','PostingDate','COGSAccount')

            vcond = [ValueEntry.DimSetID==dse.DimSetID1]
            ValueEntry = udf.LJOIN(ValueEntry,dse,vcond)

            ValueEntry = ValueEntry.drop('DimSetID1','PostingDate','LineDiscountAmount','Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','ServiceTaxAmount')

            Purchase = udf.CONCATENATE(Purchase,ValueEntry,spark)

            Purchase = Purchase.withColumnRenamed('LinkPurchaseRep','LinkPurchaser')
            
            Purchase = Purchase.drop('BudColumn')
            #//////////////////////////Writing data//////////////////////////
            
            Purchase = Purchase.na.fill({'LinkDate':'NA','LinkVendor':'NA','LinkItem':'NA','LinkLocation':'NA'})
            Purchase = Purchase.withColumn('DBName',lit(DBName)).withColumn('EntityName',lit(EntityName))
            Purchase = Purchase.withColumn('LinkDateKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkDate))\
                            .withColumn('LinkVendorKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkVendor))\
                            .withColumn('LinkPurchaserKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkPurchaser))\
                            .withColumn('LinkItemKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkItem))\
                            .withColumn('LinkLocationKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkLocation))

            Purchase = udf.RenameDuplicateColumns(Purchase)
            
            Purchase.cache()
            Purchase.write.jdbc(url=postgresUrl, table="Purchase.PurchaseInvoice", mode='overwrite', properties=PostgresDbInfo.props)
            
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseInvoice", DBName, EntityName, Purchase.count(), len(Purchase.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseInvoice', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PurchaseInvoice completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = udf.getSparkConfig(SPARK_MASTER, "Stage2:PurchaseInvoice")
    purchase_PurchaseInvoice(sqlCtx, spark)