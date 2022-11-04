'''
Created on 15 Feb 2021

@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc,round
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

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

def finance_ProfitLoss(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    #Configurl = "jdbc:postgresql://192.10.15.57/Configurator"

    try:

        GL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Entry")
        GLA_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Account")
        
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
            GL_Account_Table = ToDFWitoutPrefix(sqlCtx, hdfspath, GLA_Entity,True)
            
            GL_Account_Table = GL_Account_Table.select("No_","Income_Balance")\
                                        .withColumnRenamed('No_','No').drop('DBName','EntityName')
            GL_Entry_Table = GL_Entry_Table.withColumn('PostingDate',to_date(GL_Entry_Table['PostingDate'])).drop('DBName','EntityName')
            GLEntry = GL_Entry_Table.join(GL_Account_Table,GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No'],'left')
            GLEntry = GLEntry.filter(GLEntry['Income_Balance']==0)\
                        .filter(GLEntry["SourceCode"]!='CLSINCOME')\
                        .filter(year(GLEntry["PostingDate"])!='1753')\
                        .filter(GLEntry["PostingDate"]>=UIStartDate)
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
            Query_COA = "(SELECT * FROM masters.chartofaccounts) AS COA"
            COA = spark.read.format("jdbc").options(url=DBurl, dbtable=Query_COA,\
                                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            COA = COA.select('GLAccount','GLRangeCategory')
            GLEntry = GLEntry.join(COA,'GLAccount','left')
            
            GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.LinkDate))\
                            .withColumn("Link_GLAccount_Key",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.GLAccount))\
                            .withColumn("DBName",lit(DBName))\
                            .withColumn("EntityName",lit(EntityName))
                            
            GLEntry = GLEntry.withColumn('Amount',round('Amount',5))\
                            .withColumn('Cost_Amount',round('Cost_Amount',5))\
                            .withColumn('CreditAmount',round('CreditAmount',5))\
                            .withColumn('DebitAmount',round('DebitAmount',5))
                            
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")    
            GLEntry =  GLEntry.join(DSE,"DimensionSetID",'left')      

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
            
            GLEntry.write.jdbc(url=postgresUrl, table="Finance.ProfitLoss", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.ProfitLoss", DBName, EntityName, GLEntry.count(), len(GLEntry.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.ProfitLoss', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_ProfitLoss completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:ProfitLoss")
    finance_ProfitLoss(sqlCtx, spark)

