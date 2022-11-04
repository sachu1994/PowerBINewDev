'''
Created on 15 Feb 2021

@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc,round,split,last_day,lag,explode,expr,trunc
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime, time
import datetime as dt
from datetime import datetime
from pyspark.storagelevel import StorageLevel

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

def finance_CashFlow(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.57/Configurator_Linux"

    try:
        GL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Entry")
        FALedger_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "FA Ledger Entry")
        GLA_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Account")
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            DBurl = "jdbc:postgresql://192.10.15.134/"+entityLocation

            Query_COA = "(SELECT * FROM public."+chr(34)+"ChartofAccounts"+chr(34)+") AS COA"
            COA_Table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_COA,\
                                user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            COA_Table = COA_Table.filter(COA_Table['EntityName']==EntityName)
            COA_Table = COA_Table.select('GL Account No','CFReportHeader','Account Description','IsNegativePolarity')
            COA_Table = COA_Table.withColumnRenamed('GL Account No','GLAccount').withColumnRenamed('CFReportHeader','CFReportFlag')\
                                .withColumnRenamed('Account Description','Description')
            COA_Table = COA_Table.withColumn('IsNegativePolarity',COA_Table['IsNegativePolarity'].cast('string'))
            
            CashBeginning = COA_Table.filter(COA_Table['CFReportFlag'].isin(['Opening Funds']))
            GL_List_CashBeginning = [i.GLAccount for i in CashBeginning.select('GLAccount').distinct().collect()]
            CashClosing = COA_Table.filter(COA_Table['CFReportFlag'].isin(['Bank Balances','FDs Pledged to Banks','FDs which are Free']))
            GL_List_CashClosing = [i.GLAccount for i in CashClosing.select('GLAccount').distinct().collect()]                    
                                
            duplicates = COA_Table.withColumn('DuplicateCOA',split(COA_Table['GLAccount'],'_')[1])\
                        .withColumn('MappingGL',split(COA_Table['GLAccount'],'_')[0])

            Query_Company="(SELECT *\
                            FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Company,\
                                user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
                
            GL_Entry_Table= ToDFWitoutPrefix(sqlCtx, hdfspath, GL_Entity,True)
            GL_Account_Table= ToDFWitoutPrefix(sqlCtx, hdfspath, GLA_Entity,True)
            
            FALedgerEntry_Table = ToDFWitoutPrefix(sqlCtx, hdfspath, FALedger_Entity,True)
            FALedgerEntry_Table = FALedgerEntry_Table.select('FAPostingType','DisposalEntryNo_','PostingDate','AmountLCY',
                                                     'DocumentNo_','DimensionSetID')
            FALedgerEntry_Table = FALedgerEntry_Table.withColumnRenamed('AmountLCY','Amount').withColumnRenamed('DocumentNo_','Document_No')
            FALedgerEntry_Table = FALedgerEntry_Table.filter(((FALedgerEntry_Table['FAPostingType']==0) & (FALedgerEntry_Table['DisposalEntryNo_']==0))
                                                             |(FALedgerEntry_Table['FAPostingType']==6))
            
            GL_List = [int(i.GLAccount) for i in COA_Table.select('GLAccount').distinct().collect()]
            Dummy_GL_OPENINGCASH = max(GL_List)*100+10
            Dummy_GL_FAPurchase = max(GL_List)*100+20
            Dummy_GL_FADisposal = max(GL_List)*100+30
            
            Query_GLMapping="(SELECT * from public."+chr(34)+"tblGLAccountMapping"+chr(34)+") AS COA"

            GL_Mapping = sqlCtx.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_GLMapping,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            
            OpeningCash_GL = GL_Mapping.filter(GL_Mapping['GL Range Category']=='Cash & Cash Equivalents')
            OpeningCash_GL = OpeningCash_GL.select('From GL Code','To GL Code')
            
            OpeningCash_GL.cache()
            OpeningCash_GL_List = []
            NoofRows = OpeningCash_GL.count()
            for i in range(0,NoofRows):
                FromGL = int(OpeningCash_GL.select('From GL Code').collect()[i]['From GL Code'])
                ToGL = int(OpeningCash_GL.select('To GL Code').collect()[i]['To GL Code'])
                for i in range(FromGL,ToGL+1,1):
                    if i in GL_List:
                        OpeningCash_GL_List.append(i)
            
            GL_Account_Table = GL_Account_Table.select("No_","Income_Balance")\
                                    .withColumnRenamed('No_','No').drop('DBName','EntityName')
            GL_Entry_Table = GL_Entry_Table.withColumn('PostingDate',to_date(GL_Entry_Table['PostingDate'])).drop('DBName','EntityName')
            GLEntry = GL_Entry_Table.join(GL_Account_Table,GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No'],'left')
            GLEntry = GLEntry.filter(year(GLEntry["PostingDate"])!='1753').filter(GLEntry['SourceCode']!='CLSINCOME')
            GLEntry = GLEntry.withColumnRenamed("EntryNo_","Entry_No")\
                                        .withColumn("LinkDate",GL_Entry_Table["PostingDate"])\
                                        .withColumn("GL_Posting_Date",GL_Entry_Table["PostingDate"])\
                                        .withColumn("linkdoc", concat(GL_Entry_Table["DocumentNo_"],lit('||'),GL_Entry_Table["G_LAccountNo_"]))\
                                        .withColumn("Cost_Amount",GL_Entry_Table["Amount"])\
                                        .withColumnRenamed("DocumentNo_","Document_No")\
                                        .withColumnRenamed("Description","GL_Description")\
                                        .withColumnRenamed("SourceNo_","Source_No")\
                                        .withColumnRenamed("G_LAccountNo_","GLAccount")\
                                        .withColumn("Transaction_Type",lit("GLEntry"))\
                                        .withColumn("Entry_Flag",lit("GL"))
            #------------------------Joining----------------------------
            GLEntry = GLEntry.join(COA_Table,'GLAccount','left')
            GLEntry = GLEntry.withColumn('Amount',GLEntry['Amount'].cast('decimal(30,4)'))
            
            ClosingCash_GLEntry = GLEntry.filter(GLEntry['GLAccount'].isin(GL_List_CashClosing))
            ClosingCash_Grouped = ClosingCash_GLEntry.withColumn('FY Year',when(month(ClosingCash_GLEntry['LinkDate'])>=4,year(ClosingCash_GLEntry['LinkDate']))\
                                                                 .otherwise(year(ClosingCash_GLEntry['LinkDate'])-1))
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Month',when(month(ClosingCash_GLEntry['LinkDate'])>=4,month(ClosingCash_GLEntry['LinkDate'])-3)\
                                                                 .otherwise(month(ClosingCash_GLEntry['LinkDate'])+9))
            YM = str(datetime.datetime.today().year) + str(datetime.datetime.today().month)
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('LinkDate',
                                                    when(concat(year(ClosingCash_Grouped['LinkDate'])
                                                    ,month(ClosingCash_Grouped['LinkDate']))==YM,datetime.datetime.today().date())\
                                                    .otherwise(last_day(ClosingCash_Grouped['LinkDate'])))
            
            GL_Month = ClosingCash_Grouped.groupBy('GLAccount').agg({'LinkDate':'min'})\
                                        .withColumnRenamed('min(LinkDate)','MinDate')
            GL_Month = GL_Month.withColumn('MinDate', trunc('MinDate', 'month'))
            GL_Month = GL_Month.withColumn('MaxDate',lit(datetime.datetime.today().date()))
            GL_Month = GL_Month.withColumn('LinkDate', explode(expr('sequence(MinDate, MaxDate, interval 1 month)')))
            GL_Month = GL_Month.withColumn('LinkYM',concat(year(GL_Month['LinkDate']),month(GL_Month['LinkDate'])))
            GL_Month = GL_Month.withColumn('LinkDate',last_day(GL_Month['LinkDate']))
            GL_Month = GL_Month.withColumn('LinkDate',when(GL_Month['LinkYM']==YM,GL_Month['MaxDate']).otherwise(GL_Month['LinkDate']))
            GL_Month = GL_Month.select('GLAccount','LinkDate')
            ClosingCash_Grouped = ClosingCash_Grouped.groupBy('GLAccount','LinkDate').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
            ClosingCash_Grouped = ClosingCash_Grouped.join(GL_Month,['GLAccount','LinkDate'],'right')
            win_spec = Window.partitionBy('GLAccount').orderBy('LinkDate').rowsBetween(-sys.maxsize, 0)
            win_spec_lag = Window.partitionBy('GLAccount').orderBy('LinkDate')
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Amount',F.sum('Amount').over(win_spec))
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Amount',ClosingCash_Grouped['Amount'].cast('decimal(30,4)'))
            ClosingCash_GLEntry = ClosingCash_Grouped.withColumn('LinkDate',to_date(ClosingCash_Grouped['LinkDate']))
            
            duplicates = duplicates.filter(~(duplicates['DuplicateCOA'].isNull()))
            duplicates = duplicates.withColumn('Polarity',when(duplicates['IsNegativePolarity']==True, lit(-1)).otherwise(lit(1)))
            NoofType = [i.DuplicateCOA for i in duplicates.select('DuplicateCOA').distinct().collect()]
            #print(NoofType)
            for index,i in enumerate(NoofType):
                p = duplicates.filter(duplicates['DuplicateCOA']==i)
                flagging = p.select('GLAccount','Polarity').distinct()
                p.cache()
                list_gl = [int(i.MappingGL) for i in p.select('MappingGL').distinct().collect()]
                GLEntry_Dummy = GLEntry.filter(GLEntry['GLAccount'].isin(list_gl))
                GLEntry_Dummy = GLEntry_Dummy.withColumn('GLAccount',concat_ws('_',GLEntry_Dummy['GLAccount'],lit(i)))
                GLEntry_Dummy = GLEntry_Dummy.join(flagging,'GLAccount','left')
                GLEntry_Dummy = GLEntry_Dummy.withColumn('Amount',GLEntry_Dummy['Amount']*GLEntry_Dummy['Polarity'])
                if index==0:
                    Duplicate_GLEntry = GLEntry_Dummy
                else:
                    Duplicate_GLEntry = CONCATENATE(Duplicate_GLEntry,GLEntry_Dummy,spark)
            #sys.exit()    
            Duplicate_GLEntry = Duplicate_GLEntry.withColumn('Income_Balance',lit('5'))
            Duplicate_GLEntry = Duplicate_GLEntry.withColumn('Amount',Duplicate_GLEntry['Amount'].cast('decimal(30,4)'))
            OpeningCash_GLEntry = Duplicate_GLEntry.filter(Duplicate_GLEntry['GLAccount'].isin(GL_List_CashBeginning))
            
            Duplicate_GLEntry = Duplicate_GLEntry.filter(~(Duplicate_GLEntry['GLAccount'].isin(GL_List_CashBeginning)))
            
            if datetime.datetime.now().month>=4:
                Year = datetime.datetime.now().year
                YTDDate = str(Year)+'-03-31'
            else:
                Year = datetime.datetime.now().year-1
                YTDDate = str(Year)+'-03-31'
            
            OpeningCash_Grouped = OpeningCash_GLEntry.withColumn('FY Year',when(month(OpeningCash_GLEntry['LinkDate'])>=4,year(OpeningCash_GLEntry['LinkDate']))\
                                                                 .otherwise(year(OpeningCash_GLEntry['LinkDate'])-1))
            
            OpeningCash_Grouped = OpeningCash_Grouped.groupBy('GLAccount','FY Year').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
            
            win_spec = Window.partitionBy('GLAccount').orderBy('FY Year').rowsBetween(-sys.maxsize, 0)
            win_spec_lag = Window.partitionBy('GLAccount').orderBy('FY Year')
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',F.sum('Amount').over(win_spec))
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',F.lag('Amount').over(win_spec_lag))
            OpeningCash_Grouped = OpeningCash_Grouped.na.fill({'Amount':0})
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',OpeningCash_Grouped['Amount'].cast('decimal(30,4)'))
            OpeningCash_GLEntry = OpeningCash_Grouped.withColumn('LinkDate',concat(OpeningCash_Grouped['FY Year'],lit('-04-01')))                                      
            OpeningCash_GLEntry = OpeningCash_GLEntry.drop('FY Year')\
                                                    .withColumn('LinkDate',to_date(OpeningCash_GLEntry['LinkDate']))
            GLEntry = GLEntry.filter(~(GLEntry['GLAccount'].isin(GL_List_CashClosing)))
            GLEntry = CONCATENATE(GLEntry,OpeningCash_GLEntry,spark)
            GLEntry = CONCATENATE(GLEntry,ClosingCash_GLEntry,spark)
            GLEntry = CONCATENATE(GLEntry,Duplicate_GLEntry,spark)
            '''
            FALedgerEntry_Table = FALedgerEntry_Table.withColumn('GLAccount',when((FALedgerEntry_Table['FAPostingType']==0) & 
                                                                                  (FALedgerEntry_Table['DisposalEntryNo_']==0), lit(Dummy_GL_FAPurchase))\
                                                                            .when(FALedgerEntry_Table['FAPostingType']==6,lit(Dummy_GL_FADisposal))\
                                                                            .otherwise(lit('GLNOTADDED')))\
                                                    .withColumn('LinkDate',to_date(FALedgerEntry_Table['PostingDate']))
            FALedgerEntry_Table = FALedgerEntry_Table.withColumn('Amount',FALedgerEntry_Table['Amount']*(-1))                                      
            #GLEntry = CONCATENATE(GLEntry,FALedgerEntry_Table,spark)
            #GLEntry = CONCATENATE(GLEntry,Duplicate_GLEntry,spark)
            '''
            #GLEntry = GLEntry.withColumn('Amount',GLEntry['Amount'].cast('decimal(10,4)'))
            #---------------------------Adding Dimension------------------------------
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")    
            GLEntry =  GLEntry.join(DSE,"DimensionSetID",'left')      

            GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.LinkDate))\
                    .withColumn("Link_GLAccount_Key",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.GLAccount))\
                    .withColumn("DBName",lit(DBName))\
                    .withColumn("EntityName",lit(EntityName))

            GLEntry = GLEntry.withColumn("Link_SBU",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000), GLEntry['GlobalDimension1Code'])\
                                        .otherwise(GLEntry['Link_SBU']))\
                            .withColumn("Link_BRANCH",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000), GLEntry['GlobalDimension2Code'])\
                                        .otherwise(GLEntry['Link_BRANCH']))\
                            .withColumn("Link_SUBBU",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000), GLEntry['GlobalDimension14Code'])\
                                        .otherwise(GLEntry['Link_SUBBU']))\
                            .withColumn("Link_OTBRANCH",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000), GLEntry['GlobalDimension12Code'])\
                                        .otherwise(GLEntry['Link_OTBRANCH']))\
                            .withColumn("Link_CUSTOMER",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000), GLEntry['GlobalDimension13Code'])\
                                        .otherwise(GLEntry['Link_CUSTOMER']))

            GLEntry = GLEntry.withColumn("Link_SBUKey",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_SBU']))\
                                                    .otherwise(GLEntry['Link_SBUKey']))\
                            .withColumn("Link_BRANCHKey",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_BRANCH']))\
                                                    .otherwise(GLEntry['Link_BRANCHKey']))\
                            .withColumn("Link_SUBBUKey",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_SUBBU']))\
                                                    .otherwise(GLEntry['Link_SUBBUKey']))\
                            .withColumn("Link_OTBRANCHKey",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_OTBRANCH']))\
                                                    .otherwise(GLEntry['Link_OTBRANCHKey']))\
                            .withColumn("Link_CUSTOMERKey",when((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_CUSTOMER']))\
                                                    .otherwise(GLEntry['Link_CUSTOMERKey']))
            GLEntry.write.jdbc(url=postgresUrl, table="Finance.CashFlow", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
                
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.CashFlow", DBName, EntityName, 0, 0, IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.CashFlow', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_CashFlow completed: ' + str((dt.datetime.now()-st).total_seconds()))


if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:CashFlow")
    finance_CashFlow(sqlCtx, spark)
