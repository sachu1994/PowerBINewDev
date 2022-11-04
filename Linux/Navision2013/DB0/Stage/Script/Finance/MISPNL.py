'''
Created on 16 Feb 2021

@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,count,desc,round,udf,length,explode,split,regexp_replace,coalesce
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

def finance_MISPNL(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"
    Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
    start_time = datetime.datetime.now()
    start_time_string = start_time.strftime('%H:%M:%S')

    
    try:
        
        Acc_Sche_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Acc_ Schedule Line")
        GLA_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Account")
        
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            DBurl = "jdbc:postgresql://192.10.15.134/"+entityLocation
            ####################### DB Credentials  ###########################

            Query_df="(SELECT *\
                                FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            df = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_df,\
                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            df = df.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)

            #..........................Starting...............................#
            def addColumnIndex(df):
                newSchema = StructType(df.schema.fields +[StructField("id",IntegerType(),False),])
                #print(newSchema)
                df_added = df.rdd.zipWithIndex().map(lambda row:row[0]+(row[1],)).toDF(newSchema)
                return df_added

            def split_dataframe(df,seperate,target_col,new_col):#seperates the elements of the target columns according to seperator
                df = df.withColumn(new_col, split(target_col, seperate))
                return df

            def only_minus(df,seperate,target_col,new_col):
                df_minus = split_dataframe(df, seperate, target_col, new_col)
                flag=[]
                for j in df_minus.select(new_col).collect():
                    s=j.Totaling
                    if len(s)==1:
                        flag.append(1)
                    else:
                        flag.append(1)
                        for i in range(1,len(s)):
                            flag.append(-1)

                flag_df = sqlCtx.createDataFrame(flag, IntegerType())
                flag_df = addColumnIndex(flag_df)
                flag_df = flag_df.withColumnRenamed('value','Neg_Flag')
                df_minus = df_minus.withColumn(new_col, explode(df_minus.Totaling))
                df_minus = addColumnIndex(df_minus)
                df_minus=df_minus.join(flag_df,df_minus.id==flag_df.id)
                df_minus = df_minus.drop('id')
                df_minus = df_minus.sort('LineNo_')
                return df_minus

            def divide(df,seperate,target_col,new_col):
                df_divide = split_dataframe(df, seperate, target_col, new_col)
                divide_flag=[]
                list_col = df_divide.select(new_col).collect()
                for j in list_col:
                    j = j.Totaling
                    if len(j)==1:
                        divide_flag.append("N")
                    else:
                        divide_flag.append("N")
                        for i in range(1,len(j)):
                            divide_flag.append("D")
                flag_df = sqlCtx.createDataFrame(divide_flag, StringType())
                flag_df = addColumnIndex(flag_df)
                flag_df = flag_df.withColumnRenamed('value','Divisor_Flag')
                df_divide = df_divide.withColumn(new_col, explode(df_divide.Totaling))
                df_divide = addColumnIndex(df_divide)
                df_divide = df_divide.join(flag_df,df_divide.id==flag_df.id).drop('id')
                return df_divide

            def plus(df,seperate,target_col,new_col):
                df_plus = split_dataframe(df, seperate, target_col, new_col)
                df_plus = df_plus.withColumn(new_col, explode(df_plus[new_col]))

                return df_plus

            #############################FUNCTIONS#########################################

            #------------------------Data Extraction----------------------#
            
            Acc_Sche = ToDFWitoutPrefix(sqlCtx, hdfspath, Acc_Sche_Entity,True)
            COA = ToDFWitoutPrefix(sqlCtx, hdfspath, GLA_Entity,True)
            COA = COA.select("No_","Name","AccountType","Income_Balance","Totaling")
            Acc_Sche = Acc_Sche.select("ScheduleName","LineNo_","RowNo_","Description","Totaling",
                                    "TotalingType","ShowOppositeSign","RowType","AmountType")
            Acc_Sche = Acc_Sche.filter(Acc_Sche["ScheduleName"]=="P&L_NEW")
            #----#RE Salary GL = 9999999
            #Acc_Sche = Acc_Sche.withColumn("Description",when(condition, value))

            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='RE Salary', lit(9999999))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='Corporate Expenses', lit(8888888))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='SUB sbu sharing', lit(7777777))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='Shared Depreciation', lit(6666666))\
                                        .otherwise(Acc_Sche["Totaling"]))
            delete = udf(lambda s: s.replace("*100", ""), StringType())
            Acc_Sche = Acc_Sche.withColumn("test", delete("Totaling"))
            Acc_Sche = Acc_Sche.withColumn("Percent" , when(Acc_Sche["Totaling"]!=Acc_Sche["test"],"%")\
                                                                    .otherwise("V")).drop("Totaling")\
                                                                    .withColumnRenamed("test","Totaling")
            Acc_Sche = Acc_Sche.withColumn('LineType',when(length(Acc_Sche.Totaling)==0, lit('H'))\
                                                    .when(Acc_Sche.TotalingType==2, lit('F')).otherwise(lit('')))
            #Acc_Sche = Acc_Sche.withColumn("Description",when((~(Acc_Sche["LineType"].isin(['H','F']))), concat(lit("     "),Acc_Sche["Description"]))\
            #                               .otherwise(Acc_Sche["Description"]))
            #Acc_Sche.sort('LineNo_').show(30,False)
            #sys.exit()
            Acc_Sche_null=Acc_Sche.filter(Acc_Sche.Totaling=='')
            Acc_Sche_not_null=Acc_Sche.filter(Acc_Sche.Totaling!='')
            row_COA = [int(i.No_) for i in COA.select('No_').collect()]

            df=Acc_Sche_not_null.withColumn("Totaling", explode(split("Totaling", "[|]")))

            df = divide(df, "/", "Totaling", "Totaling")  #(New Column Should be similar as target column)
            df = df.sort('LineNo_')

            df_minus=only_minus(df,"-","Totaling","Totaling")   #(New Column Should be similar as target column)
            dfm_plus=plus(df_minus,"[+]","Totaling","Totaling")   #(New Column Should be similar as target column)
            dfm_plus=dfm_plus.withColumn('Totaling', regexp_replace('Totaling', '\\(|\\)', ''))
            dfm_plus.sort("LineNo_")

            #------------Removing Brackets--------------#
            dfn_dot=dfm_plus.filter(~dfm_plus.Totaling.like("%..%"))
            df_dot = dfm_plus.filter(dfm_plus.Totaling.like("%..%"))
            df_dot = addColumnIndex(df_dot)
            d = [i.Totaling for i in df_dot.select('Totaling').collect()]
            dot=[]
            for i in range(0,len(d)):
                s=d[i].split("..")
                l=[]
                for i in range(int(s[0]),int(s[1])+1):
                    if i in row_COA:
                        l.append(i)
                q = '|'.join(str(e) for e in l)
                dot.append(q)

            kdot=sqlCtx.createDataFrame(dot,StringType())
            kdot = addColumnIndex(kdot)
            kdot = kdot.withColumnRenamed('value','dot_splitted')
            df_dot=df_dot.join(kdot,["id"]).drop('id')

            #------------Changing .. --------------#

            df_dot = df_dot.withColumn("Totaling", explode(split("dot_splitted","[|]")))
            df_dot=df_dot.drop("dot_splitted")

            df_con=dfn_dot.union(df_dot)

            ##############Final Concatenated Dataframe################
            for i in range(0,10):
                seperated=df_con.select(["RowNo_","Totaling","Neg_Flag","RowType"])
                seperated=seperated.withColumnRenamed("Totaling","join"+str(i))\
                            .withColumnRenamed("RowNo_","join"+str(i)+"_RowNo_")\
                            .withColumnRenamed("Neg_Flag","join"+str(i)+"_Neg_Flag")\
                            .withColumnRenamed("RowType","join"+str(i)+"_RowType")

                con = df_con["Totaling"] == seperated["join"+str(i)+"_RowNo_"]
                df_con = df_con.join(seperated,con,'left')
                df_con = df_con.withColumn("join"+str(i),coalesce(df_con["join"+str(i)],df_con["Totaling"]))
                df_con = df_con.na.fill({"join"+str(i)+"_Neg_Flag":1})
                df_con = df_con.withColumn("Flag_"+str(i+1),(df_con["join"+str(i)+"_Neg_Flag"]*df_con["Neg_Flag"]).cast("int"))
                df_con = df_con.drop("Neg_Flag").drop("join"+str(i)+"_Neg_Flag")
                df_con = df_con.withColumn("RowType_"+str(i+1),(df_con["join"+str(i)+"_RowType"]+df_con["RowType"]).cast("int"))
                df_con = df_con.withColumn("RowType_"+str(i+1),coalesce(df_con["RowType_"+str(i+1)],df_con["RowType"]))
                df_con = df_con.drop("RowType").drop("join"+str(i)+"_RowType")
                df_con = df_con.drop('Totaling')
                df_con = df_con.withColumnRenamed("Flag_"+str(i+1),"Neg_Flag")\
                            .withColumnRenamed("RowType_"+str(i+1),"RowType")\
                            .withColumnRenamed("join"+str(i),"Totaling")
                df_con = df_con.sort('LineNo_')
                df_con = df_con.na.fill({"join"+str(i)+"_RowNo_":0})
                df_con = df_con.withColumn("join"+str(i)+"_RowNo_",(df_con["join"+str(i)+"_RowNo_"]).cast('int'))

                sum_of_col = sum(df_con.select("join"+str(i)+"_RowNo_").rdd.flatMap(lambda x: x).collect())
                if sum_of_col == 0:
                    df_con = df_con.withColumnRenamed('Totaling','MISKEY')
                    break

            #######Completed Account Schedule########

            MIS = df_con.withColumn('DBName',lit(DBName))\
                        .withColumn('EntityName',lit(EntityName))
            MIS = MIS.withColumn('Link_GL_Key',concat(MIS['DBName'],lit('|'),MIS['EntityName'],lit('|'),MIS['MISKEY']))
            MIS = CONCATENATE(MIS,Acc_Sche_null,spark)

            MIS.cache()
            MIS.write.jdbc(url=postgresUrl, table="Finance.MISPNL_Customized", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
                
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.MISPNL", DBName, EntityName, MIS.count(), len(MIS.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.MISPNL', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_MISPNL_Customized completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:MISPNL_Customized")
    finance_MISPNL(sqlCtx, spark)
    
