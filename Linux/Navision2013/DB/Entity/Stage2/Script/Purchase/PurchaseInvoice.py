from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col,concat,concat_ws,year,when,month,to_date,lit,expr,sum
from pyspark.sql.types import *
import os,sys
from os.path import dirname, join, abspath
import datetime,traceback
import datetime as dt 
from builtins import len
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB1_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB1_path)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("PurchaseInvoice")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryoserializer.buffer.max","512m")\
        .set("spark.cores.max","24")\
        .set("spark.executor.memory","8g")\
        .set("spark.driver.memory","24g")\
        .set("spark.driver.maxResultSize","20g")\
        .set("spark.memory.offHeap.enabled",'true')\
        .set("spark.memory.offHeap.size","100g")\
        .set('spark.scheduler.mode', 'FAIR')\
        .set("spark.sql.broadcastTimeout", "36000")\
        .set("spark.network.timeout", 10000000)\
        .set("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")\
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .set("spark.databricks.delta.vacuum.parallelDelete.enabled",'true')\
        .set("spark.databricks.delta.retentionDurationCheck.enabled",'false')\
        .set('spark.hadoop.mapreduce.output.fileoutputformat.compress', 'false')\
        .set("spark.rapids.sql.enabled", True)\
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
import delta
from delta.tables import *
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","") 
        try:
            logger = Logger() 
            entityLocation = DBName+EntityName         
            pil =spark.read.format("delta").load(STAGE1_PATH+"/Purch_ Inv_ Line")
            pt=spark.read.format("delta").load(STAGE1_PATH+"/Payment Terms")
            dse=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE")
            pt =pt.select("Code","DueDateCalculation")
            prl=spark.read.format("delta").load(STAGE1_PATH+"/Purch_ Rcpt_ Line")
            prl=prl.select("ExpectedReceiptDate","DocumentNo_","LineNo_")
            prl = prl.withColumn('LinkRcptKey',concat_ws('|',prl.DocumentNo_,prl.LineNo_))
            pih =spark.read.format("delta").load(STAGE1_PATH+"/Purch_ Inv_ Header")
            gps =spark.read.format("delta").load(STAGE1_PATH+"/General Posting Setup")
            pld =spark.read.format("delta").load(STAGE1_PATH+"/Posted Str Order Line Details")
            VE = spark.read.format("delta").load(STAGE1_PATH+"/Value Entry")
            pil = pil.filter((year(pil.PostingDate)!=1753) & (pil.Quantity!=0) & (pil.Type!=4))
            GLRange=spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            pil = pil.na.fill({'Gen_Bus_PostingGroup':'NA','Gen_Prod_PostingGroup':'NA','DocumentNo_':'NA','LineNo_':'NA','PostingDate':'NA'})
            Line = pil\
                    .withColumn("No",when(pil.Type=='1',pil['No_'].cast('int')))\
                    .withColumn("GL_Link",concat_ws('|',lit(entityLocation),pil.Gen_Bus_PostingGroup.cast('string'), pil.Gen_Prod_PostingGroup.cast('string')))\
                    .withColumn("LinkValueEntry",concat_ws('|',pil.DocumentNo_.cast('string'),pil.LineNo_.cast('string'),to_date(pil.PostingDate).cast('string')))\
                    .withColumn("Link_GD",concat_ws('-',pil.DocumentNo_.cast('string'),pil.LineNo_.cast('string')))\
                    .withColumn("LinkLocationCode",when(pil.LocationCode=='',lit("NA")).otherwise(pil.LocationCode))
                    
            Line = Kockpit.RENAME(Line,{"DimensionSetID":"DimSetID","DocumentNo_":"Document_No","Amount":"PurchaseAmount"})
            Line = Line.drop('PostingDate')
            Header = pih\
                    .withColumn("LinkVendor",when(pih['Pay-toVendorNo_']=='',lit("NA")).otherwise(pih["Pay-toVendorNo_"])).drop("Pay-toVendorNo_")\
                    .withColumn("MonthNum", month(pih.PostingDate))\
                    .withColumn("LinkPurchaseRep",when(pih.PurchaserCode=='',lit("NA")).otherwise(pih.PurchaserCode)).drop("PurchaserCode")  
                        
            Header = Kockpit.RENAME(Header,{"No_":"Purchase_No","LocationCode":"HeaderLocationCode"})
            Monthsdf = Kockpit.MONTHSDF(sqlCtx)
            mcond = [Header.MonthNum==Monthsdf.MonthNum1]
            Header = Kockpit.LJOIN(Header,Monthsdf,mcond)
            Header = Header.drop('MonthNum','MonthNum1')
            Header = Header.na.fill({'Purchase_No':'NA'})
            GL_Master = gps.withColumn("GL_LinkDrop",concat_ws('|',lit(entityLocation),gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
                                    .withColumn("GLAccount",when(gps.Purch_Account=='',0).otherwise(gps.Purch_Account)).drop('Purch_Account')
            Line = Line.withColumn('Document_No_key',concat_ws('|',lit(entityLocation),Line.Document_No))
            Header = Header.withColumn('Purchase_No_key',concat_ws('|',lit(entityLocation),Header.Purchase_No))
            cond = [Line.Document_No_key == Header.Purchase_No_key]
            Purchase = Kockpit.LJOIN(Line,Header,cond)
            Purchase = Purchase.withColumn('LinkRcptKey',concat_ws('|',Purchase.ReceiptDocumentNo_,Purchase.ReceiptDocumentLineNo_))
            GLRange = GLRange.filter(GLRange['DBName'] == DBName ).filter(GLRange['EntityName'] == EntityName ).filter(GLRange['GLRangeCategory']== 'Purchase Trading')
            GLRange = Kockpit.RENAME(GLRange,{'FromGLCode':'FromGL','ToGLCode':'ToGL','GLRangeCategory':'GLCategory'})
            GLRange = GLRange.select("GLCategory","FromGL","ToGL")
            cond1 = [Purchase.GL_Link == GL_Master.GL_LinkDrop]
            Purchase = Kockpit.LJOIN(Purchase,GL_Master,cond1)
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
            Purchase = Kockpit.LJOIN(Purchase,LineDetails,cond2)
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
            ValueEntry = VE.withColumn("LinkDate",to_date(VE.PostingDate))\
                        .withColumn("LinkValueEntry1",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))\
                        .withColumn("CostAmount",when(col(FieldName)=='CostAmountExpected',when(VE.CostAmountActual=='0',VE.CostAmountExpected*(-1)).otherwise(VE.CostAmountActual*(-1))).otherwise(col(FieldName)*(-1)))
            ValueEntry = Kockpit.RENAME(ValueEntry,{"DimensionSetID":"DimSetID","ItemNo_":"LinkItem","DocumentNo_":"DocumentNo","ItemLedgerEntryNo_":"LinkILENo"})
            ValueEntry1 = ValueEntry.select('LinkValueEntry1','CostAmount').groupby('LinkValueEntry1')\
                                    .agg(sum("CostAmount").alias("CostAmount")) 
            JoinCOGS = [Purchase.LinkValueEntry == ValueEntry1.LinkValueEntry1]
            Purchase = Kockpit.LJOIN(Purchase,ValueEntry1,JoinCOGS)
            Purchase = Purchase.drop("PurchaseAmount","LinkValueEntry1")
            Documents = Purchase.select('Document_No').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1)).withColumnRenamed('Document_No','Document_NoDrop')
            JoinDocs = [ValueEntry.DocumentNo == Documents.Document_NoDrop]
            ValueEntry = Kockpit.LJOIN(ValueEntry,Documents,JoinDocs)
            ValueEntry = ValueEntry.drop("Document_NoDrop")
            SysEntries = Purchase.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            JoinSysEntry = [ValueEntry.LinkValueEntry1 == SysEntries.LinkValueEntry]
            ValueEntry = Kockpit.LJOIN(ValueEntry,SysEntries,JoinSysEntry)
            ValueEntry = ValueEntry.drop("LinkValueEntry").withColumnRenamed("LinkValueEntry1","LinkValueEntry")
            ValueEntry = ValueEntry.filter((ValueEntry.SysDocFlag==1) & (ValueEntry.SysValueEntryFlag.isNull()))
            ValueEntry = Kockpit.RENAME(ValueEntry,{'DocumentLineNo_':'LineNo','EntryNo_':'Link_GD','LocationCode':'LinkLocation','DocumentNo':'Document_No'})
            ValueEntry = ValueEntry.select('DimSetID','Document_No','VariantCode','LinkItem','LinkValueEntry','InvoicedQuantity','Gen_Bus_PostingGroup','Gen_Prod_PostingGroup',\
                                    'PostingDate','LinkDate','CostAmountActual','CostAmountExpected','LineNo','Link_GD','LinkLocation').withColumn('Trn_Quantity',ValueEntry.InvoicedQuantity)
            
            ValueEntry = ValueEntry.withColumn('TransactionType',lit('RevaluationEntries')).withColumn('RevenueType',lit('Other'))
            dse = Kockpit.RENAME(dse,{"DimensionSetID":"DimSetID1"})
            scond = [Purchase.DimSetID==dse.DimSetID1]
            Purchase = Kockpit.LJOIN(Purchase,dse,scond).drop("Locationtype")
            Purchase = Purchase.drop('DimSetID1','COGSAccount')
            vcond = [ValueEntry.DimSetID==dse.DimSetID1]
            ValueEntry = Kockpit.LJOIN(ValueEntry,dse,vcond)
            ValueEntry = ValueEntry.drop('DimSetID1','LineDiscountAmount','Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','ServiceTaxAmount','Posting Date')
            Purchase = Kockpit.RenameDuplicateColumns(Purchase)
            Purchase = Purchase.unionByName(ValueEntry, allowMissingColumns = True)
            Purchase = Purchase.withColumnRenamed('LinkPurchaseRep','LinkPurchaser')
            Purchase = Purchase.drop('BudColumn')
            Purchase = Purchase.na.fill({'LinkDate':'NA','LinkVendor':'NA','LinkItem':'NA','LinkLocation':'NA'})
            Purchase = Purchase.withColumn('DBName',lit(DBName)).withColumn('EntityName',lit(EntityName))
            Purchase = Purchase.withColumn('LinkDateKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkDate))\
                            .withColumn('LinkVendorKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkVendor))\
                            .withColumn('LinkPurchaserKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkPurchaser))\
                            .withColumn('LinkItemKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkItem))\
                            .withColumn('LinkLocationKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkLocation))
            Purchase = Purchase.withColumn('LinkVariant',concat_ws('|',Purchase['LinkItem'],Purchase['VariantCode']))            
            cond3=[Purchase.LinkRcptKey==prl.LinkRcptKey]
            Purchase = Kockpit.LJOIN(Purchase,prl,cond3)
            cond4=[Purchase.PaymentTermsCode==pt.Code]
            Purchase = Kockpit.LJOIN(Purchase,pt,cond4)
            Purchase = Kockpit.RenameDuplicateColumns(Purchase).drop("expectedreceiptdate_1","LineNo__1","LinkRcptKey_1")
            Purchase.cache()
            print(Purchase.count())
            Purchase.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/PurchaseInvoice")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseInvoice", DBName, EntityName, Purchase.count(), len(Purchase.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 PurchaseInvoice '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
            
            log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseInvoice', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('purchase Invoice completed: ' + str((dt.datetime.now()-st).total_seconds()))
        
