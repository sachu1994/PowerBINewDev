from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import concat,lit,when,col,concat_ws,count,expr
from datetime import timedelta, date
import re,os,datetime,time,sys,traceback
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

logger = Logger()
Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

def masters_dimensions(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    try:
        DC_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Dimension Value") 
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            Query_Config="(SELECT *\
                        FROM "+chr(34)+"tblDimensionCode"+chr(34)+") AS df"
            DC_Config = spark.read.format("jdbc").options(url=Configurl, dbtable=Query_Config,\
                            user="postgres",password="admin",driver= "org.postgresql.Driver").load()
            
            # exit()                
        
            df_dimension = DC_Config.filter(DC_Config['DBName'] == DBName ).filter(DC_Config['EntityName'] == EntityName).filter(DC_Config['IsActive'] == 1)
            df_dimension = df_dimension.select("Name")
            # df_dimension.show()
            list_dimen = df_dimension.collect()
            # print(list_dimen)
            print(DC_Entity)
            NoOfRows=df_dimension.count()
            for t in range(0,NoOfRows):
                table = list_dimen[t].Name
                table2 = ToDFWitoutPrefix(sqlCtx, hdfspath, DC_Entity,True)
                
                table2.printSchema()
                
                df = table2.filter(table2["DimensionCode"]==table).filter(expr("length(Totaling)=0")).drop('DBName','EntityName')
                # df.show()
            
                df = df.withColumnRenamed("DimensionCode","Dimension_Code")
                df = df.withColumn("RegionName",when(df["REGION"]==1, lit('EAST'))\
                                                .when(df["REGION"]==2, lit('WEST'))\
                                                .when(df["REGION"]==3, lit('SOUTH'))\
                                                .when(df["REGION"]==4, lit('NORTH'))\
                                                .otherwise(lit('')))
                df = df.collect()
                len_df = len(df)
                Name = []
                DB = []
                Entity = []
                Region = []
                Inde = []
                Code = []
                level_range = 0
                for row in range(0,len_df):
                    Name.append(df[row]['Name'])
                    Inde.append(df[row]['Indentation'])
                    DB.append(DBName)
                    Entity.append(EntityName)
                    Code.append(df[row]['Code'])
                    Region.append(df[row]['RegionName'])
                level_range = max(Inde) + 1
                size = len(Inde)
                list1 = []
                list2 = []
                labels = []
                for j in range(0 , level_range):
                    list1.insert(0 , "null")
                    a =table+"_Level"+str(j)
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
                coa =pd.DataFrame.from_records(list2, columns=labels)
                d = {table+"_Name":Name,"DBName":DBName,"EntityName":EntityName,"Link"+table:Code,"Code":Code,"Region":Region}
                
               
                records = pd.concat([coa,pd.DataFrame(d)],axis=1)
                # print(records)
                # exit()
                records = spark.createDataFrame(records)
                records = records.na.fill({'Code':'NA'})
                records = records.withColumn("Link"+table+"Key",concat_ws('|',records['DBName'],records['EntityName'],records['Code'])).drop('Code')
                table = re.sub('[\s+]','',table)
                records = records.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(records.columns)))
                records = records.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(records.columns)))
                records = records.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(records.columns)))
                
            
                records.cache()
                records.write.jdbc(url=postgresUrl, table="Masters."+table+"_Dimension", mode="overwrite", properties=PostgresDbInfo.props)
                
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Dimensions", DBName, EntityName, records.count(), len(records.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Dimensions', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_dimensions completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Dimensions")
    masters_dimensions(sqlCtx, spark)