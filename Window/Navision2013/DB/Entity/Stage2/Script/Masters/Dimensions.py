
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit,when,col,concat_ws,count,expr
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
import pandas as pd
from os.path import dirname, join, abspath
import re,datetime,time,sys
import datetime as dt 
from builtins import str
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
sys.path.insert(0,'../../../..')
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit

Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName

conf = SparkConf().setMaster("local[*]").setAppName("Dimensions")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            DC_Config= spark.read.parquet("../../../Stage1/ConfiguratorData/tblDimensionCode")
            df_dimension = DC_Config.filter(DC_Config['DBName'] == DBName ).filter(DC_Config['EntityName'] == EntityName).filter(DC_Config['IsActive'] == 1)
            df_dimension = df_dimension.select("Name")
            list_dimen = df_dimension.collect()
            NoOfRows=df_dimension.count()
            for t in range(0,NoOfRows):
                    table = list_dimen[t].Name
                    table2 = spark.read.parquet("../../../Stage1/ParquetData/DimensionValue")
                    df = table2.filter(table2["DimensionCode"]==table).filter(expr("length(Totaling)=0")).drop('DBName','EntityName')
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
                    records = spark.createDataFrame(records)
                    records = records.na.fill({'Code':'NA'})
                    records = records.withColumn("Link"+table+"Key",concat_ws('|',records['DBName'],records['EntityName'],records['Code'])).drop('Code')
                    table = re.sub('[\s+]','',table)
                    records = records.select(*(col(x).alias(re.sub('[\s+]','', x)) for ix,x in enumerate(records.columns)))
                    records = records.select(*(col(x).alias(re.sub('[(+]','', x)) for ix,x in enumerate(records.columns)))
                    records = records.select(*(col(x).alias(re.sub('[)+]','', x)) for ix,x in enumerate(records.columns)))
                    records.cache()
                    records.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Master/"+table+"_Dimension")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Dimensions", DBName, EntityName, records.count(), len(records.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
        
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 Dimensions '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            
            log_dict = logger.getErrorLoggedRecord('Dimensions', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('masters_Dimensions completed: ' + str((dt.datetime.now()-st).total_seconds()))