'''
Created on 28 Jan 2018
@author: Abhishek-Jaydip
'''

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import re,keyring,os
from pyspark.sql.functions import col,when,length,lit,concat, split, concat_ws
import pandas as pd
import datetime,time,sys
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

def masters_coa(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"
    
    try:

        GL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Account")
        print(GL_Entity)
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            GLAccount = ToDFWitoutPrefix(sqlCtx, hdfspath, GL_Entity,True)
            
            Query_cheker="(SELECT *\
                        FROM "+chr(34)+"ConditionalMapping"+chr(34)+") AS df"
            FlagChecker = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_cheker,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            FlagChecker = FlagChecker.withColumn('BSReportHeader',when(FlagChecker['Particulars'] == 'BalanceSheet',FlagChecker['Alternate Mapping']))
            FlagChecker = FlagChecker.withColumn('PLReportHeader',when(FlagChecker['Particulars'] == 'PL',FlagChecker['Alternate Mapping']))
            FlagChecker = FlagChecker.withColumn('BSReportFlag',when(FlagChecker['Particulars'] == 'BalanceSheet',lit('Y')).otherwise(lit('N')))
            FlagChecker = FlagChecker.withColumn('PLReportFlag',when(FlagChecker['Particulars'] == 'PL',lit('Y')).otherwise(lit('N')))
            FlagChecker = FlagChecker.withColumn('Income_Balance',when(FlagChecker['Particulars'] == 'BalanceSheet',lit(1)).otherwise(lit(0)))
            FlagChecker = FlagChecker.withColumn('Level0',when(FlagChecker['Particulars'] == 'BalanceSheet',lit('Balance Sheet')).otherwise(lit('Profit and Loss Account')))
            NegHead = FlagChecker.select('GLAccount','BSReportHeader','PLReportHeader','BSReportFlag','PLReportFlag','Income_Balance','Level0')
            NegHead = NegHead.withColumn('DBName',lit(DBName))
            NegHead = NegHead.withColumn('EntityName',lit(EntityName))
            
            Inde = [i.Indentation for i in GLAccount.select('Indentation').collect()]
            
            Name = [i.Name for i in GLAccount.select('Name').collect()]
            
            Gl = [i.No_ for i in GLAccount.select('No_').collect()]
            
            IB = [i.Income_Balance for i in GLAccount.select('Income_Balance').collect()]
            
            Acc = [i.AccountType for i in GLAccount.select('AccountType').collect()]
            
          
            size = len(Inde)
            level_range = max(Inde)+1
        
            list1 = []
            list2 = []
            labels = []
            for j in range(0 , level_range):
                list1.insert(0 , "null")
                a ="Level"+str(j)
                labels.append(a)
        
            list2.insert(0,list1)
            
            for i in range(0,size):
               
                if(list2[i][Inde[i]] != Name[i]):
                    
                    list1[Inde[i]] = Name[i]
                    for j in range(Inde[i] + 1 , level_range):
                        list1[j] = "null"
                list2.insert(i + 1 , list1)
                list1 = []
                for k in range(0,level_range):
                    list1.insert(k , list2[i+1][k])
        
            list2 = list2[1:]
            
           
            #sys.exit()
            for i in range(0,size):
                for j in range(1,level_range):
                    if(list2[i][j] == "null"):
                        list2[i][j] = list2[i][j-1]
        
            coa =pd.DataFrame.from_records(list2, columns=labels)
            d = {"GLAccount":Gl,"DBName":DBName,"EntityName":EntityName,"Acc":Acc,"Income_Balance":IB}
            records = pd.concat([coa,pd.DataFrame(d)],axis=1)
            records = spark.createDataFrame(records)
            records = records.filter(records['Acc'] == 0) .drop(records['Acc'])
            records = records.withColumn("Link_GLAccount_Key",concat(records['DBName'],lit("|"),records['EntityName'],lit("|"),records['GLAccount']))
        
            list=records.select('GLAccount').filter('Income_Balance=0').distinct().collect()
            NoOfRows=len(list)
            data1=[]

            Query_GLMapping="(SELECT *\
                        FROM public."+chr(34)+"tblGLAccountMapping"+chr(34)+") AS df"
            GLMapping = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_GLMapping,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            GLMapping = GLMapping.select([col(x).alias(x.replace(' ', '')) for x in GLMapping.columns])
            table = GLMapping.filter(col('GLRangeCategory').isin(['REVENUE','DE','INDE'])).filter(GLMapping['DBName'] == DBName).filter(GLMapping['EntityName'] == EntityName)
            table = table.withColumnRenamed("FromGLCode", "FromGL")
            table = table.withColumnRenamed("ToGLCode", "ToGL")
            table = table.withColumnRenamed("GLRangeCategory", "GLCategory")
            GLRangeCat = table.select("GLCategory","FromGL","ToGL").collect()

            NoofFields=len(GLRangeCat)
            for i in range(0,NoOfRows):
                n=int(list[i]['GLAccount'])
                for j in range(0, NoofFields):
                    if int(GLRangeCat[j].FromGL) <= n <= int(GLRangeCat[j].ToGL):
                        data1.append({'GLRangeCategory':GLRangeCat[j].GLCategory,'GLAccount1':n})
                        break
            TempGLRangeCategory=spark.createDataFrame(data1).distinct()
            cond = [records.GLAccount == TempGLRangeCategory.GLAccount1]
            records=records.join(TempGLRangeCategory,cond,'left').drop('GLAccount1')
        
            table = GLMapping.filter(col('GLRangeCategory').isin(['EBIDTA','PBT','PAT'])).filter(GLMapping['DBName'] == DBName).filter(GLMapping['EntityName'] == EntityName)
            table = table.withColumnRenamed("FromGLCode", "FromGL")
            table = table.withColumnRenamed("ToGLCode", "ToGL")
            table = table.withColumn('FromGL',table['FromGL'].cast('int'))\
                        .withColumn('ToGL',table['ToGL'].cast('int'))
            table = table.withColumnRenamed("GLRangeCategory", "GLCategory")
            GLRangeCat = table.select("GLCategory","FromGL","ToGL").collect()
        
            Query_COA = "(SELECT * FROM public."+chr(34)+"ChartofAccounts"+chr(34)+") AS COA"
            COA_Table = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_COA,\
                                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            #COA_Table.show()
            #sys.exit()                                
            COA_Table = COA_Table.filter(COA_Table['DBName']==DBName).filter(COA_Table['EntityName']==EntityName)
            Config_COA = COA_Table
            
            PL_Headers = COA_Table.select('GL Account No','PLReportHeader')\
                                .withColumnRenamed('GL Account No','GLAccount')
            PL_Headers = PL_Headers.withColumn('PLReportHeader',when(PL_Headers['GLAccount']=='636200', lit('Other expenses'))\
                                                                .otherwise(PL_Headers['PLReportHeader']))
            PL_Headers = PL_Headers.filter(PL_Headers['PLReportHeader']!='')
            
            BS_Headers = COA_Table.select('GL Account No','BSReportHeader').filter(COA_Table['BSReportHeader']!='')\
                                .withColumnRenamed('GL Account No','GLAccount')
            
            COA_Table = COA_Table.select('GL Account No','CFReportHeader','Account Description')
            COA_Table = COA_Table.withColumnRenamed('GL Account No','GLAccount').withColumnRenamed('CFReportHeader','CFReportFlag')\
                                .withColumnRenamed('Account Description','Description')
            
            GL_List = [int(i.GLAccount) for i in records.select('GLAccount').distinct().collect()]
            Dummy_GL_OPENINGCASH = max(GL_List)*100+10
            Dummy_GL_FAPurchase = max(GL_List)*100+20
            Dummy_GL_FADisposal = max(GL_List)*100+30
            
            '''
            Duplicate GL Finding
            '''                   
            x = COA_Table.withColumn('DuplicateCOA',split(COA_Table['GLAccount'],'_')[1])\
                        .withColumn('MappingGL',split(COA_Table['GLAccount'],'_')[0])
            x = x.filter(~(x['DuplicateCOA'].isNull()))
            x = x.withColumn('Link_GLAccount_Key',concat_ws('|',lit(DBName),lit(EntityName),x['GLAccount']))
            x = x.drop('DuplicateCOA','MappingGL')
            
            DummyGL_dict = [{'GLAccount':Dummy_GL_OPENINGCASH,'CFReportFlag':'Opening cash and cash equivalents','Description':'Opening Cash and Cash Equivalents'},
                        {'GLAccount':Dummy_GL_FAPurchase,'CFReportFlag':'Purchase of fixed assets','Description':'FA Purchase'},
                        {'GLAccount':Dummy_GL_FADisposal,'CFReportFlag':'Proceeds from sale of fixed assets','Description':'FA Disposal'}]
            DummyGL = spark.createDataFrame(DummyGL_dict)
            DummyGL = DummyGL.withColumn('Link_GLAccount_Key',concat_ws('|',lit(DBName),lit(EntityName),DummyGL['GLAccount']))
            
            x = x.unionByName(DummyGL)              
            
            table = Config_COA.withColumn("PLReportFlag",when(length(Config_COA.PLReportHeader)==0,'N').otherwise('Y'))
            table = table.withColumn("BSReportFlag",when(length(table.BSReportHeader)==0,'N').otherwise('Y')).select("GL Account No","PLReportHeader","BSReportHeader","PLReportFlag","BSReportFlag")
            table1 = table
            table = table1.collect()
            data1 = []
            data2 = []
            data3 = []
            
            Gl = GLAccount.select('No_')
            Gl = Gl.withColumn('GLAccountNo',Gl['No_'].cast('int'))
            Gl = Gl.select('GLAccountNo').collect()
            
            NoofFields = len(GLRangeCat)
            Tablevalue = len(Gl)
            for i in range(0,NoofFields):
                a = GLRangeCat[i]['GLCategory']
                for j in range(0,Tablevalue):
                    n = Gl[j]['GLAccountNo']
                    if(a == 'EBIDTA' and GLRangeCat[i]['FromGL'] <= n <= GLRangeCat[i]['ToGL']):
                        data1.append({'PLFlag1':"EBIDTA",'GLAccount1':n})
                    if(a == 'PBT' and GLRangeCat[i]['FromGL'] <= n <= GLRangeCat[i]['ToGL']):
                        data2.append({'PLFlag2':"PBT",'GLAccount2':n})
                    if(a == 'PAT' and GLRangeCat[i]['FromGL'] <= n <= GLRangeCat[i]['ToGL']):
                        data3.append({'PLFlag3':"PAT",'GLAccount3':n})
                    
            data1=spark.createDataFrame(data1)
            data2=spark.createDataFrame(data2)
            data3=spark.createDataFrame(data3)
            
            records = records.join(data1,records['GLAccount'] == data1['GLAccount1'],"left")
            records = records.join(data2,records['GLAccount'] == data2['GLAccount2'],"left")
            records = records.join(data3,records['GLAccount'] == data3['GLAccount3'],"left")
            
            #table1 = table1.drop(table1.GLAccount1).drop(table1.GLAccount2).drop(table1.GLAccount3)
            records = records.drop(records.GLAccount1).drop(records.GLAccount2).drop(records.GLAccount3)
            
            ChartofAccount = table1
            records = records.join(ChartofAccount,records['GLAccount']==ChartofAccount['GL Account No'],'left')
            records = records.drop('GL Account No').drop('PLReportHeader').drop('BSReportHeader')
            records = records.join(PL_Headers,'GLAccount','left')
            records = records.join(BS_Headers,'GLAccount','left')    
            record_level7 = records.select('GLAccount','Level7')
            NegHead = NegHead.join(record_level7, 'GLAccount', how = 'left')
            NegHead = NegHead.withColumn("Level7",concat_ws('_',NegHead['Level7'],lit("(Neg Header)")))
            NegHead = NegHead.withColumn('GLAccount',concat(NegHead['GLAccount'],lit('000')))
            NegHead = NegHead.withColumn('Link_GLAccount_Key',concat_ws('|',NegHead['DBName'],NegHead['EntityName'],NegHead['GLAccount']))
            records = CONCATENATE(records,NegHead,spark)
            
            records = records.withColumn("Link_PLReportHeader" , concat(records['DBName'],lit("|"),records['EntityName'],lit("|"),records['PLReportHeader']))
            records = records.withColumn("Link_BSReportHeader" , concat(records['DBName'],lit("|"),records['EntityName'],lit("|"),records['BSReportHeader']))
            records.cache()
            COAforCFOnly = records.join(COA_Table,'GLAccount','left')
            COAforCFOnly = CONCATENATE(COAforCFOnly,x,spark)
        
            records.write.jdbc(url=postgresUrl, table="masters.ChartofAccounts", mode="overwrite", properties=PostgresDbInfo.props)
            records.show()
            COAforCFOnly.write.jdbc(url=postgresUrl, table="masters.COAforCFOnly", mode="overwrite", properties=PostgresDbInfo.props)
            COAforCFOnly.show()
       
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("COA", DBName, EntityName, records.count(), len(records.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('COA', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_coa completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:COA")
    masters_coa(sqlCtx, spark)