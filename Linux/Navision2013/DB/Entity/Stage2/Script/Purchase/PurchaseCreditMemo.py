from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
#from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc
import datetime, time
import datetime as dt
import re
from pyspark import StorageLevel
from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers import udf

def purchase_PurchaseCreditMemo(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        
        if datetime.date.today().month>int(MnSt)-1:
            UIStartYr=datetime.date.today().year-int(yr)+1
        else:
            UIStartYr=datetime.date.today().year-int(yr)

        pcmlEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Cr_ Memo Line") 
        pcmhEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Cr_ Memo Hdr_")
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

            pcml = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pcmlEntity, True)
            pcmh = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pcmhEntity, True)
            gps = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, gpsEntity, True)
            pld = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, pldEntity, True)
            VE = udf.ToDFWitoutPrefix(sqlCtx, hdfspath, veEntity, True)

            #pil = pil.na.fill({'Gen_Bus_PostingGroup':'NA','Gen_Prod_PostingGroup':'NA','DocumentNo_':'NA','LineNo_':'NA','PostingDate':'NA'})
            pcml = pcml.filter((year(col("PostingDate"))!='1753')&(col("Quantity")!=0)&(col("Type")!=4))
            Line = pcml\
                    .withColumn("LinkItem",when(pcml["Type"]==2,when(pcml["No_"]=='',lit("NA")).otherwise(pcml["No_"])))\
                    .withColumn("No",when(col("Type")==1,col("No_").cast('int')))\
                    .withColumn("GL_Link",concat_ws("|",lit(entityLocation),pcml.Gen_Bus_PostingGroup,pcml.Gen_Prod_PostingGroup))\
                    .withColumn("LinkValueEntry",concat_ws("|",pcml.DocumentNo_.cast('string'),pcml.LineNo_.cast('string'),to_date(pcml.PostingDate).cast('string')))\
                    .withColumn("Link_GD",concat_ws("-",pcml.DocumentNo_,pcml.LineNo_))\
                    .withColumn("LinkLocationCode",when(col("LocationCode")=='',"NA").otherwise(col("LocationCode")))\
                    .withColumn("InvoicedQuantity",when(col("Type")==2,col("Quantity")*(-1)))\
                    .withColumn("ServiceTaxAmount",(pcml.ServiceTaxeCessAmount+pcml.ServiceTaxSHECessAmount+pcml.ServiceTaxAmount)*(-1))\
                    .withColumnRenamed("DimensionSetID","DimSetID").withColumnRenamed("DocumentNo_","Document_No").withColumnRenamed("LineNo_","LineNo")\
                    .withColumnRenamed("Description","LineDescription").withColumnRenamed("Quantity","Trn_Quantity")\
                    .withColumn("CrMemoAmount",col("Amount")*(-1))\
                    .withColumn("TaxAmount",col("TaxAmount")*(-1))\
                    .withColumn("ExciseAmount",col("ExciseAmount")*(-1))\
                    .withColumnRenamed("Inv_DiscountAmount","InvDiscountAmount")\
                    .withColumnRenamed("Buy-from Vendor No_","BuyfromVendorNumber")
                    
            Line = Line.withColumn('PurchaseTaxAmount',Line['TaxAmount'])
            '''
            .withColumn("No",when(pcml.Type=='1',pcml['No_'].cast('int')))\
            .withColumn("GL_Link",concat_ws('|',lit(entityLocation),pcml.Gen_Bus_PostingGroup.cast('string'), pcml.Gen_Prod_PostingGroup.cast('string')))\
            .withColumn("LinkValueEntry",concat_ws('|',pcml.DocumentNo_.cast('string'),pcml.LineNo_.cast('string'),to_date(pcml.PostingDate).cast('string')))\
            .withColumn("Link_GD",concat_ws('-',pcml.DocumentNo_.cast('string'),pcml.LineNo_.cast('string')))\
            .withColumn("LinkLocationCode",when(pcml.LocationCode=='',lit("NA")).otherwise(pcml.LocationCode))\
            .withColumn("CrMemoAmount",col("Amount")*(-1))\
            .withColumn("TaxAmount",col("TaxAmount")*(-1))\
            .withColumn("ExciseAmount",col("ExciseAmount")*(-1))\
            .withColumnRenamed("Inv_DiscountAmount","InvDiscountAmount")\
            .withColumnRenamed("Buy-from Vendor No_","BuyfromVendorNumber")
            '''
            #Line = udf.RENAME(Line,{"DimensionSetID":"DimSetID","DocumentNo_":"Document_No","Amount":"PurchaseAmount"})
            Line = Line.drop('PostingDate')
            
            Header = pcmh\
                    .withColumn("LinkVendor",when(pcmh['Pay-toVendorNo_']=='','NA').otherwise(pcmh["Pay-toVendorNo_"])).drop("Pay-toVendorNo_","DBName","EntityName")\
                    .withColumn("PaytoName",pcmh['Pay-toName']+" "+pcmh['Pay-toName2']).drop('Pay-toName','Pay-toName2')\
                    .withColumn("PaytoAddress",pcmh['Pay-toAddress']+" "+pcmh['Pay-toAddress2']).drop('Pay-toAddress','Pay-toAddress2')\
                    .withColumn("ShiptoName",pcmh['Ship-toName']+" "+pcmh['Ship-toName2']).drop('Ship-toName','Ship-toName2')\
                    .withColumn("ShiptoAddress",pcmh['Ship-toAddress']+" "+pcmh['Ship-toAddress2']).drop('Ship-toAddress','Ship-toAddress2')\
                    .withColumn("LinkDate",to_date(pcmh.PostingDate))\
                    .withColumn("LinkPurchaseRep",when(pcmh.PurchaserCode=='','NA').otherwise(pcmh.PurchaserCode)).drop("PurchaserCode")\
                    .withColumn("BuyfromVendorName",pcmh["Buy-fromVendorName"]+" "+pcmh["Buy-fromVendorName2"]).drop("Buy-fromVendorName","Buy-fromVendorName2")\
                    .withColumn("Years",when(month(pcmh.PostingDate)>=MnSt,year(pcmh.PostingDate)).otherwise(year(pcmh.PostingDate)-1))\
                    .withColumn("Quarters",when(month(pcmh.PostingDate)>=MnSt,concat(lit('Q'),(quarter(pcmh.PostingDate)-1))).otherwise(lit('Q4')))\
                    .withColumn("MonthNum", month(pcmh.PostingDate))\
                    .withColumnRenamed("No_","Purchase_No").withColumnRenamed("Pay-toCity","PaytoCity").withColumnRenamed("Ship-toCode","ShiptoCode")\
                    .withColumnRenamed("Ship-toCity","ShiptoCity").withColumnRenamed("OnHold","HoldStatus")\
                    .withColumnRenamed("LocationCode","HeaderLocationCode")\
                    .withColumnRenamed("Buy-fromCity","BuyfromCity").withColumnRenamed("Pay-toPostCode","PaytoPostCode")\
                    .withColumnRenamed("Ship-toPostCode","ShiptoPostCode")
            '''    
            .withColumn("LinkVendor",when(pcmh['Pay-toVendorNo_']=='',lit("NA")).otherwise(pcmh["Pay-toVendorNo_"])).drop("Pay-toVendorNo_")\
            .withColumn("MonthNum", month(pcmh.PostingDate))\
            .withColumn("LinkPurchaseRep",when(pcmh.PurchaserCode=='',lit("NA")).otherwise(pcmh.PurchaserCode)).drop("PurchaserCode")\
            '''
            #Header = udf.RENAME(Header,{"No_":"Purchase_No","LocationCode":"HeaderLocationCode"})

            Monthsdf = udf.MONTHSDF(sqlCtx)
            mcond = [Header.MonthNum==Monthsdf.MonthNum1]
            Header = udf.LJOIN(Header,Monthsdf,mcond)
            Header = Header.drop('MonthNum','MonthNum1')
            
            GL_Master = gps.withColumn("GL_LinkDrop",concat_ws('|',lit(entityLocation),gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
                                    .withColumn("GLAccount",when(gps.Purch_Account=='',0).otherwise(gps.Purch_Account)).drop('Purch_Account')
            
            data=[{'GLCategory':'Purchase Trading','FromGL':402000,'ToGL':406500}]
            GLRange = sqlCtx.createDataFrame(data)
            
            cond = [Line.Document_No == Header.Purchase_No]
            Purchase = udf.LJOIN(Line,Header,cond)

            cond1 = [Purchase.GL_Link == GL_Master.GL_LinkDrop]
            Purchase = udf.LJOIN(Purchase,GL_Master,cond1)
            Purchase = Purchase.drop("GL_LinkDrop","GL_Link")
            
            Range='1=1'
            Range1='1=1'
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                if i==0:
                    Range = (Purchase.GLAccount>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[0]["ToGL"])
            
                    Range1 = (Purchase.No>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                        & (Purchase.No<=GLRange.select('ToGL').collect()[0]["ToGL"])
                else:
                    Range = (Range) | (Purchase.GLAccount>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[i]["ToGL"])
            
                    Range1 = (Range1) | (Purchase.No>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                        & (Purchase.No<=GLRange.select('ToGL').collect()[i]["ToGL"])
            Purchase = Purchase.filter(((Purchase.Type!=2) | ((Purchase.Type==2) & (Range))) & ((Purchase.Type!=1) | ((Purchase.Type==1) & (Range1))))
            Purchase = Purchase.withColumn("PurchaseAccount",when(Purchase.Type==2,Purchase.GLAccount).otherwise(when(Purchase.Type==1,Purchase.No).otherwise(lit(1))))
            #Purchase.filter(Purchase['Document_No']=='PCR2152122-008').show()
            #sys.exit()
            pld = pld.filter((pld.Type == 2) & (pld.DocumentType==2) & (pld.Tax_ChargeType==0))
            LineDetails = pld.withColumn("Link_GDDrop",concat_ws('-',pld.InvoiceNo_,pld.LineNo_)).drop("InvoiceNo_","LineNo_")\
                                                    .withColumnRenamed("AccountNo_","AccountNo")
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

            Purchase = Purchase.withColumn('TransactionType',lit('CrMemo'))
            #Purchase.filter(Purchase['Document_No']=='PCR2152122-008').show()
            #sys.exit()

            Purchase = Purchase\
                            .withColumn('GLAccountNo',when(Purchase.GLAccount.isNull(), Purchase.No).otherwise(Purchase.GLAccount))\
                            .withColumn('PayableAmount',when((Purchase.CurrencyFactor==0) | (Purchase.CurrencyFactor.isNull()),when(Purchase.ChargesfromVendor.isNull(),Purchase.CrMemoAmount)\
                            .otherwise(Purchase.CrMemoAmount+Purchase.ChargesfromVendor)).otherwise(when(Purchase.ChargesfromVendor.isNull(),(Purchase.CrMemoAmount/Purchase.CurrencyFactor))\
                            .otherwise(((Purchase.CrMemoAmount+Purchase.ChargesfromVendor)/(Purchase.CurrencyFactor)))))
                    
            Purchase = Purchase.withColumn('LinkLocation',when(Purchase.LinkLocationCode.isNull(),Purchase.HeaderLocationCode).otherwise(Purchase.LinkLocationCode))
            Purchase = Purchase.drop('PurchaseAccount')
            Purchase = Purchase.withColumn('LinkDate',to_date(Purchase['PostingDate']))
            FieldName = "CostAmount(Expected)"
            '''
            Field = sqlctx.read.parquet(hdfspath+"/Data/FieldSelection")
            Field = Field.filter(Field['Flag'] == 1).filter(Field['TableName'] == 'VE FieldSelection')
            Field = Field.select("FieldType","Flag","TableName")
            FieldName = Field.select('FieldType').collect()[0]["FieldType"]
            FieldName = re.sub('[\s+]', '', FieldName)
            '''
            ValueEntry = VE.withColumn("LinkDate",to_date(VE.PostingDate))\
                        .withColumn("LinkValueEntry1",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))\
            
            ValueEntry = udf.RENAME(ValueEntry,{"DimensionSetID":"DimSetID","ItemNo_":"LinkItem","DocumentNo_":"DocumentNo","ItemLedgerEntryNo_":"LinkILENo"})
            
            if FieldName=='CostAmount(Expected)':
                ValueEntry = ValueEntry.withColumn("CostAmount",when(ValueEntry.CostAmountActual==0,ValueEntry.CostAmountExpected*(-1)).otherwise(ValueEntry.CostAmountActual*(-1)))
            else:
                ValueEntry = ValueEntry.withColumn("CostAmount",ValueEntry[FieldName]*(-1))
    
            ValueEntry1 = ValueEntry.select('LinkValueEntry1','CostAmount').groupby('LinkValueEntry1')\
                        .agg(sum("CostAmount").alias("CostAmount"))
            
            JoinCOGS = [Purchase.LinkValueEntry == ValueEntry1.LinkValueEntry1]
            Purchase = udf.LJOIN(Purchase,ValueEntry1,JoinCOGS)
            Purchase = Purchase.drop("CrMemoAmount","LinkValueEntry1")

            Documents = Purchase.select('Document_No').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1)).withColumnRenamed('Document_No','Document_NoDrop')
            
            PurchaseInvQuery="(SELECT * FROM purchase.purchaseinvoice) AS PurchaseInv"
            PurchaseInvoice = sqlCtx.read.format("jdbc").options(url=postgresUrl, dbtable=PurchaseInvQuery, user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"] ,driver= PostgresDbInfo.props["driver"]).load()

            DocumentsCrMemo = PurchaseInvoice.select("Document_No").distinct()
            DocumentsCrMemo = DocumentsCrMemo.withColumnRenamed("Document_No","Document_NoDrop").withColumn("SysDocFlag",lit(2))
            Documents=Documents.unionAll(DocumentsCrMemo)
            
            JoinDocs = [ValueEntry.DocumentNo == Documents.Document_NoDrop]
            ValueEntry = udf.LJOIN(ValueEntry,Documents,JoinDocs)
            ValueEntry = ValueEntry.drop("Document_NoDrop")
            
            SysEntries = Purchase.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            JoinSysEntry = [ValueEntry.LinkValueEntry1 == SysEntries.LinkValueEntry]
            ValueEntry = udf.LJOIN(ValueEntry,SysEntries,JoinSysEntry)
            ValueEntry = ValueEntry.drop("LinkValueEntry").withColumnRenamed("LinkValueEntry1","LinkValueEntry")

            ValueEntry = ValueEntry.filter(((ValueEntry.SysDocFlag==1)&(ValueEntry.SysValueEntryFlag.isNull()))|((ValueEntry.SysDocFlag.isNull())&(ValueEntry.ItemLedgerEntryType==1)))
        
            ValueEntry = ValueEntry.withColumnRenamed('DocumentNo','Document_No').withColumnRenamed('DocumentLineNo_','LineNo')\
                                .withColumnRenamed('EntryNo_','Link_GD').withColumn('Trn_Quantity',ValueEntry['InvoicedQuantity'])\
                                .withColumn('RevenueType',lit('OTHER')).withColumn('TransactionType',lit('RevaluationandRemainingEntries'))\
                                .withColumnRenamed('LocationCode','LinkLocation')
                                
            dse = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            dse = udf.RENAME(dse,{"DimensionSetID":"DimSetID1"})

            scond = [Purchase.DimSetID==dse.DimSetID1]
            Purchase = udf.LJOIN(Purchase,dse,scond)
            Purchase = Purchase.drop('DimSetID1')

            vcond = [ValueEntry.DimSetID==dse.DimSetID1]
            ValueEntry = udf.LJOIN(ValueEntry,dse,vcond)

            ValueEntry = ValueEntry.withColumn("PostingDate",col("PostingDate").cast('string'))\
                    .withColumn("LinkDate",col("LinkDate").cast('string')).drop('DimSetID1')
        
            Purchase = Purchase.withColumn("PostingDate",col("PostingDate").cast('string'))\
                        .withColumn("ExpectedReceiptDate",col("ExpectedReceiptDate").cast('string'))\
                        .drop('LocationCode','Amount','ServiceTaxSHECessAmount','Sell-toCustomerName2','GLCode','Link_GDDrop')
            #Purchase.filter(Purchase['Document_No']=='PCR2152122-008').show()
            #sys.exit()
            Purchase = udf.CONCATENATE(Purchase,ValueEntry,spark)

            Purchase = Purchase.withColumnRenamed('LinkPurchaseRep','LinkPurchaser')
            
            Purchase = Purchase.drop('BudColumn')
            #//////////////////////////Writing data//////////////////////////
            
            Purchase = Purchase.withColumn('DBName',lit(DBName)).withColumn('EntityName',lit(EntityName))
            Purchase = Purchase.withColumn('LinkVendorKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkVendor))\
                                .withColumn('LinkDateKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkDate))\
                                .withColumn('LinkPurchaserKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkPurchaser))\
                                .withColumn('LinkItemKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkItem))\
                                .withColumn('LinkLocationKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkLocation))
            PurchaseInv = udf.CONCATENATE(Purchase,PurchaseInvoice,spark)
            PurchaseInv = PurchaseInv.na.fill({"PayableAmount":0})
            
            PurchaseInv.cache()

            PurchaseInv.write.jdbc(url=postgresUrl, table="Purchase.purchase", mode='overwrite', properties=PostgresDbInfo.props)
            
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseCreditMemo", DBName, EntityName, Purchase.count(), len(Purchase.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseCreditMemo', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PurchaseCreditMemo completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = udf.getSparkConfig(SPARK_MASTER, "Stage2:PurchaseCreditMemo")
    purchase_PurchaseCreditMemo(sqlCtx, spark)