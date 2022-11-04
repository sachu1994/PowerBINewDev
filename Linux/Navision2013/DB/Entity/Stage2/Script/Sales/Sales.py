'''
Created on 1 Feb 2019
@author: Ashish
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc
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

def sales_Sales(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

    try:
        
        SIH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Invoice Header")
        SIL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Invoice Line")
        
        
        SCMH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Cr_Memo Header")
        SCML_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Sales Cr_Memo Line")
        GPS_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "General Posting Setup")
        PSOLD_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Posted Str Order Line Details")
        VE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Value Entry")
        GL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Entry")
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            DBurl = "jdbc:postgresql://192.10.15.134/"+entityLocation
            
            Query_Company="(SELECT *\
                        FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_Company,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            
            
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            
            Query_GLMap = "(SELECT *\
                        FROM "+chr(34)+"tblGLAccountMapping"+chr(34)+") AS df"
            GLMap = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_GLMap,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            GLMap = GLMap.withColumnRenamed('GL Range Category','GLCategory')\
                        .withColumnRenamed('From GL Code','FromGL')\
                        .withColumnRenamed('To GL Code','ToGL')
            GLRange = GLMap.filter(GLMap["GLCategory"] == 'REVENUE').filter(GLMap["DBName"] == DBName)\
                            .filter(GLMap["EntityName"] == EntityName).select("GLCategory","FromGL","ToGL")
            NoOfRows=GLRange.count()
                    
            SIH = ToDFWitoutPrefix(sqlCtx, hdfspath, SIH_Entity,True)

            SIH = SIH.filter(SIH['PostingDate']>=UIStartDate)
            
            SIL = ToDFWitoutPrefix(sqlCtx, hdfspath, SIL_Entity,True)
            SIL = SIL.withColumn("GL_Link",concat_ws('|',SIL.Gen_Bus_PostingGroup.cast('string'), SIL.Gen_Prod_PostingGroup.cast('string')))\
                    .withColumn("LinkValueEntry",concat_ws('|',SIL.DocumentNo_.cast('string'),SIL.LineNo_.cast('string'),to_date(SIL.PostingDate).cast('string')))\
                    .withColumnRenamed('No_','Item_No').withColumnRenamed('PostingDate','SIL_PostingDate')

            
            SIL = SIL.filter(SIL['Type']!=4).filter(SIL['Quantity']!=0)
            SIL_DE = SIL
            SIL = SIL.filter(SIL['SIL_PostingDate']>=UIStartDate)
                    
            SCMH = ToDFWitoutPrefix(sqlCtx, hdfspath, SCMH_Entity,True)
            SCMH = SCMH.filter(SCMH['PostingDate']>=UIStartDate)
            
            SCML = ToDFWitoutPrefix(sqlCtx, hdfspath, SCML_Entity,True)
            SCML = SCML.withColumn("GL_Link",concat_ws('|',SCML.Gen_Bus_PostingGroup.cast('string'), SCML.Gen_Prod_PostingGroup.cast('string')))\
                    .withColumn("LinkValueEntry",concat_ws('|',SCML.DocumentNo_.cast('string'),SCML.LineNo_.cast('string'),to_date(SCML.PostingDate).cast('string')))\
                    .withColumnRenamed('No_','Item_No').withColumnRenamed('PostingDate','SCML_PostingDate')
            SCML = SCML.filter(SCML['Type']!=4).filter(SCML['Quantity']!=0)
            SCML_DE = SCML
            SCML = SCML.filter(SCML['SCML_PostingDate']>=UIStartDate)
            
            GPS = ToDFWitoutPrefix(sqlCtx, hdfspath, GPS_Entity,False)
            GPS = GPS.select('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','SalesAccount','COGSAccount','SalesCreditMemoAccount')
            
            GPS_Sales = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                    .withColumn("GLAccount",when(GPS.SalesAccount=='',0).otherwise(GPS.SalesAccount))
            GPS_Sales = GPS_Sales.select('GL_Link','GLAccount')
            
            GPS_SCM = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                        .withColumn("GLAccount",when(GPS.SalesCreditMemoAccount=='',0).otherwise(GPS.SalesCreditMemoAccount))
            GPS_SCM = GPS_SCM.select('GL_Link','GLAccount')
            
            GPS_DE = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                        .withColumn("GLAccount",when(GPS.COGSAccount=='',0).otherwise(GPS.COGSAccount))
            GPS_DE = GPS_DE.select('GL_Link','GLAccount')
            
            PSOLD = ToDFWitoutPrefix(sqlCtx, hdfspath, PSOLD_Entity,False)
            VE = ToDFWitoutPrefix(sqlCtx, hdfspath, VE_Entity,True)
            #VE.filter(VE['DocumentNo_']=='GST2021KR-4759').show()
            #sys.exit()
            VE = VE.filter(VE['PostingDate']>=UIStartDate)
            
            GL = ToDFWitoutPrefix(sqlCtx, hdfspath, GL_Entity,True)
            GL = GL.filter(GL['PostingDate']>=UIStartDate)
            
            SI = SIL.join(SIH, SIL['DocumentNo_']==SIH['No_'], 'left')
            SI = SI.join(GPS_Sales,'GL_Link','left')
            SI = SI.withColumn('GLAccount',when(SI['Type']==1,SI['Item_No']).otherwise(SI['GLAccount']))
            #print(len(SI.columns))
            #SI.printSchema()
            #sys.exit()
            
            SCM = SCML.join(SCMH, SCML['DocumentNo_']==SCMH['No_'], 'left')
            SCM = SCM.join(GPS_SCM,'GL_Link','left')
            SCM = SCM.withColumn('GLAccount',when(SCM['Type']==1,SCM['Item_No']).otherwise(SCM['GLAccount']))
            SCM = SCM.withColumn('Amount',SCM['Amount']*(-1))
            
            Sales = CONCATENATE(SCM,SI,spark)
            for i in range(0,NoOfRows):
                if i==0:
                    Range = (Sales.GLAccount>=GLRange.select('FromGL').collect()[0]['FromGL']) \
                        & (Sales.GLAccount<=GLRange.select('ToGL').collect()[0]['ToGL'])
        
                else:
                    Range = (Range) | ((Sales.GLAccount>=GLRange.select('FromGL').collect()[i]['FromGL']) \
                                        & (Sales.GLAccount<=GLRange.select('ToGL').collect()[i]['ToGL']))
            
            Sales = Sales.filter(((Sales.Type!=2) | ((Sales.Type==2) & (Range))) & ((Sales.Type!=1) | ((Sales.Type==1) & (Range))))
            Sales = Sales.withColumn('ChargesToCustomer',Sales['ChargesToCustomer'].cast('decimal'))
            Sales = Sales.na.fill({'ChargesToCustomer':0})
            Sales = Sales.withColumn('Amount',when(Sales.CurrencyFactor==0,when(Sales.ChargesToCustomer==0,Sales.Amount).otherwise(Sales.Amount+Sales.ChargesToCustomer))\
                        .otherwise(when(Sales.ChargesToCustomer==0,Sales.Amount/Sales.CurrencyFactor).otherwise((Sales.Amount+Sales.ChargesToCustomer)/(Sales.CurrencyFactor))))
            
            VE = VE.withColumn("LinkValueEntry",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))
            VE = VE.withColumn('CostAmountActual',VE['CostAmountActual']*(-1))
            #VE.filter(VE['DocumentNo_']=='GST2021KR-4759').show()
            #sys.exit()
            ValueEntry = VE
            VE = VE.groupBy('LinkValueEntry').agg({'CostAmountActual':'sum'})\
                    .withColumnRenamed('sum(CostAmountActual)','CostAmountActual')
            
            Sales = Sales.join(VE,'LinkValueEntry','left')
            Sales = Sales.withColumn('SystemEntry',lit(1))
            
            Documents = Sales.select('DocumentNo_').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1))
            ValueEntry = ValueEntry.join(Documents,'DocumentNo_','left')
        
            SysEntries = Sales.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            ValueEntry = ValueEntry.join(SysEntries,'LinkValueEntry','left')
            ValueEntry = ValueEntry.filter(((ValueEntry.SysDocFlag==1)&(ValueEntry.SysValueEntryFlag.isNull()))|((ValueEntry.SysDocFlag.isNull())&(ValueEntry.ItemLedgerEntryType==1)))
            ValueEntry = ValueEntry.withColumn('TransactionType',lit('Revaluation Entries'))

            Sales = CONCATENATE(Sales,ValueEntry,spark)
            
            DocumentNos = Sales.withColumn('link',concat_ws('|',Sales['DocumentNo_'],Sales['GLAccount']))
            DocumentNos = DocumentNos.select('link','SystemEntry').distinct()
            
            SIL_DE = SIL_DE.select('DocumentNo_','GL_Link').distinct()
            SCML_DE = SCML_DE.select('DocumentNo_','GL_Link').distinct()
            ManualCOGSDocuments = SIL_DE.unionByName(SCML_DE)
            ManualCOGSDocuments = ManualCOGSDocuments.join(GPS_DE,'GL_Link','left')
            ManualCOGSDocuments = ManualCOGSDocuments.withColumn('link',
                                concat_ws('|',ManualCOGSDocuments['DocumentNo_'],ManualCOGSDocuments['GLAccount']))\
                                                    .withColumn('SystemEntry',lit(1))
            ManualCOGSDocuments = ManualCOGSDocuments.select('link','SystemEntry').distinct()
            
            
            DocumentNos = DocumentNos.unionByName(ManualCOGSDocuments)
            
            GL = GL.filter(GL['SourceCode']!='CLSINCOME')\
                    .withColumn('link',concat_ws('|',GL['DocumentNo_'],GL['G_LAccountNo_']))
            GL = GL.withColumnRenamed('G_LAccountNo_','GLAccount')
            GL = GL.join(DocumentNos,'link','left')
            GL = GL.na.fill({'SystemEntry':0})
            GL = GL.filter(GL['SystemEntry']==0)
            
            Query_COA = "(SELECT * FROM masters.chartofaccounts) AS COA"
            COA = spark.read.format("jdbc").options(url=DBurl, dbtable=Query_COA,\
                                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            COA = COA.select('GLAccount','GLRangeCategory')
            GL = GL.join(COA,'GLAccount','left')
            GL.show()
            exit()
            GLEntry_Sales = GL.filter(GL['GLRangeCategory'].isin(['REVENUE']))
            GLEntry_DE = GL.filter(GL['GLRangeCategory'].isin(['DE']))
            AggCOGS = GLEntry_DE.groupBy('DocumentNo_').agg({'Amount':'sum'})\
                                .withColumnRenamed('sum(Amount)','Cost_Amount')
            AggCOGS = AggCOGS.filter(AggCOGS['Cost_Amount']!=0)
            GLEntry_DE = AggCOGS.join(GLEntry_DE,'DocumentNo_','left')
            
            GLEntry_Sales = GLEntry_Sales.withColumn('Amount',GLEntry_Sales['Amount']*(-1))
            GLEntry_DE = GLEntry_DE.withColumnRenamed('Amount','CostAmountActual')
            
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            finalDF = Sales.join(DSE,"DimensionSetID",'left')
            
            GLEntry_Sales = GLEntry_Sales.join(DSE,"DimensionSetID",'left')
            GLEntry_Sales.write.jdbc(url=postgresUrl, table="Sales.SalesGLEntry", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            GLEntry_DE = GLEntry_DE.join(DSE,"DimensionSetID",'left')
            GLEntry_DE.write.jdbc(url=postgresUrl, table="Sales.ManualCOGS", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props
            
            #Sales.filter(Sales['Document_No']=='GST2021GJ-00214')\
            #    .filter(Sales['LineNo']=='60000').show()
            #sys.exit()
            
            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Sales.Sales", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Sales.Sales", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Sales.Sales', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('sales_Sales completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Sales")
    sales_Sales(sqlCtx, spark)
