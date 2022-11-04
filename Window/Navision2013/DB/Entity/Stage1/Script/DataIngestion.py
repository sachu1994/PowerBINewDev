from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year, current_date, to_date,month,date_format
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys,shutil,time
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str

begin_time = dt.datetime.now()
st = dt.datetime.now()
Stage1_Path = abspath(join(join(dirname(__file__),'..')))
helpersDir = abspath(join(join(dirname(__file__), '..'),'..','..'))
sys.path.insert(0, helpersDir)
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..'))
EntityNamepath=abspath(join(join(dirname(__file__), '..'),'..'))
DBName =os.path.split(DBNamepath)
EntityName =os.path.split(EntityNamepath)
DBName = DBName[1]
EntityName=EntityName[1]
entityLocation = DBName+EntityName
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..')))
Abs_Path =abspath(join(join(dirname(__file__),'..'),'..'))

from Configuration import ModifiedTable as mt, AppConfig as ac, ModifiedModule as mm
from Configuration.Constant import *

conf = SparkConf().setMaster("local[*]").setAppName("DataIngestion")\
        .set("spark.rapids.sql.enabled", True)\
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in ac.config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and dbe['Location']==entityLocation:
        select_list = ['Name','Year', 'Month']
        res = [dbe[i] for i in select_list if i in dbe]
        CompanyName = res[0]
        Year = int(res[1])
        Month = int(res[2])
        CompanyNamewos=CompanyName.replace(" ","")
        try:
            logger = Logger()
            sqlurl=ConnectionInfo.SQL_URL.format(ac.config["SourceDBConnection"]['url'], str(ac.config["SourceDBConnection"]["port"]), ac.config["SourceDBConnection"]['databaseName'], ac.config["SourceDBConnection"]['userName'], ac.config["SourceDBConnection"]['password'],ac.config["SourceDBConnection"]['dbtable'])
            IMpath =DBNamepath+"\\"+"Configuration"+"\\"+'IncrementalMonth.txt'
            IMfile=open(IMpath)
            imonth = IMfile.read()
            count = 0
            MTpath =DBNamepath+"\\"+"Configuration"+"\\"+'ModifiedTable.py'
            MMpath =DBNamepath+"\\"+"Configuration"+"\\"+'ModifiedModule.py'
            #------------------data ingestion for Configurator files-------------------------------------
             table_names = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl,dbtable='information_schema.tables',user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load().\
             filter("table_schema = '"+ConfiguratorDbInfo.Schema+"'").select("table_name")
             table_names_list = [row.table_name for row in table_names.collect()]
            
             for i in table_names_list:
                 Query="(SELECT *\
                             FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+i+chr(34)+") AS df"
            
                 Configurator_Data = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
                 Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(" ","")) for col in Configurator_Data.columns])
                 Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace("(","")) for col in Configurator_Data.columns])
                 Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(")","")) for col in Configurator_Data.columns])
                 Configurator_Data.coalesce(1).write.mode("overwrite").parquet(Abs_Path+"\Stage1\ConfiguratorData\\"+i)
             
            # ------------------------------- delete Files present in ModifiedTable file---------------------------------------
            check_file = os.stat(MTpath).st_size
            if(check_file == 0):
                print(" ")
            else:
                for Modifiedtable in mt.config["tables"]: 
                    tables = Modifiedtable["TableName"]
                    count+=1
                if count>=1:
                    for Modifiedtable in mt.config["tables"]: 
                        Modified_table = Modifiedtable['TableName']             
                        Modified_tbl_name = Modified_table.replace(" ","")
                        file = Modified_tbl_name
                        location = Stage1_Path+"\\"+"ParquetData"
                        path = os.path.join(location, file) 
                        if os.path.exists(path):
                            shutil.rmtree(path)
                           
            #-------------------------Delete Modified Module Files-----------------------
            
            # check_Module = os.stat(MMpath).st_size
            # if(check_Module == 0):
            #     print(" ")
            # else:
            #     for ModifiedModule in mm.config["Modules"]: 
            #         count+=1
            #         print(count)
            #      
            #     if count>=1:
            #         try:
            #             for Table in ac.config["TablesToIngest"]:
            #                 for Moduleval in Table['Modules']:
            #                     Mdl_name = Table['Modules']
            #                     tbl_name = Table['Table']
            #                     tbl_name = tbl_name.replace(" ","")
            #                     print(Moduleval)
            #                     print(Mdl_name)
            #                     for ModifiedModule in mm.config["Modules"]: 
            #                         for Module in ModifiedModule:
            #                                 
            #                           
            #                             for Moduleval in Table['Modules']:
            #                                 Mdl_name = Table['Modules']
            #                                 tbl_name = Table['Table']
            #                                 tbl_name = tbl_name.replace(" ","")
            #                                 
            #                                 if Moduleval== Module and ModifiedModule[Module]=='Full':
            #                                     location = Stage1_Path+"\\"+"ParquetData"
            #                                     path = os.path.join(location, tbl_name) 
            #                                     if os.path.exists(path):
            #                                         
            #                                         shutil.rmtree(path)
            #         except Exception as e:
            #             print("")    
            #   
            #---------------------Data Ingestion main part-----------------------------             
            for Table in ac.config["TablesToIngest"]:
                table_name = CompanyName+Table['Table']
                transact_column = Table['TransactionColumn']
                Columns = Table['Columns']
                try:
                    logger = Logger()
                    schemaquery = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+table_name+chr(39)+") AS data"
                    schema = spark.read.format("jdbc").options(url=sqlurl,dbtable=schemaquery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                    schema = schema.filter(schema['COLUMN_NAME'].isin(Columns))
                    schema = schema.collect()
                    temp = ''
                    for i in range(0,len(schema)):
                        col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                        if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                            col_name_temp = col_name_temp
                        elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                            col_name_temp = "," + col_name_temp
                        if(i == 0 and schema[i]['DATA_TYPE'] == 'sql_variant'):
                            temp =temp +"CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                        elif schema[i]['DATA_TYPE']=='sql_variant':
                            temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                        else:
                            temp = temp + col_name_temp
                    tbl_name = Table['Table']        
                    parquet_tbl_name = tbl_name.replace(" ","")        
                    Type = Table["TableType"]    
                    if Type == "Master":
                        tableQuery = "(SELECT "+temp+" FROM ["+table_name+"]) AS data1"
                        schemaDF = spark.read.format("jdbc").options(url=sqlurl,dbtable=tableQuery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                        result_df = schemaDF.select([F.col(col).alias(col.replace(" ","")) for col in schemaDF.columns])
                        result_df = result_df.select([F.col(col).alias(col.replace("(","")) for col in result_df.columns])
                        result_df = result_df.select([F.col(col).alias(col.replace(")","")) for col in result_df.columns]) 
                        result_df = result_df.withColumn('DBName',lit(DBName)) 
                        result_df = result_df.withColumn('EntityName',lit(EntityName))  
                        result_df.coalesce(1).write.mode("overwrite").parquet(Abs_Path+"\Stage1\ParquetData\\"+parquet_tbl_name)
                        logger.endExecution()
                        try:
                            IDEorBatch = sys.argv[1]
                        except Exception as e :
                            IDEorBatch = "IDLE"
                    
                        log_dict = logger.getSuccessLoggedRecord("DataIngestion_"+table_name, DBName, EntityName, result_df.count(), len(result_df.columns), IDEorBatch)
                        log_df = spark.createDataFrame(log_dict, logger.getSchema())
                        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)     
                    elif Type =="Transaction":
                        ParquetData = Stage1_Path+"\\"+"ParquetData"
                        path = Stage1_Path+"\\"+"ParquetData"+"\\"+parquet_tbl_name
                        
                        if os.path.exists(path): 
                            dateadd_month = imonth
                            tblquery = "(SELECT "+temp+" FROM ["+table_name+"] WHERE ["+transact_column+"] >= DATEADD(month, DATEDIFF(month, 0, DATEADD(MONTH, -"+dateadd_month+", GETDATE())), 0) ) AS data4"
                            schemaDF = spark.read.format("jdbc").options(url=sqlurl,dbtable=tblquery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load() 
                            schemaDF= schemaDF.withColumn("YearMonth",year(schemaDF[transact_column])*100+date_format(schemaDF[transact_column],'MM'))
                            schemaDF = schemaDF.withColumn("YearMonth",schemaDF['YearMonth'].cast(IntegerType()))
                            schemaDF = schemaDF.withColumn("YearMonth",schemaDF['YearMonth'].cast(StringType()))
                            schemaDF= schemaDF.select([F.col(col).alias(col.replace(" ","")) for col in schemaDF.columns])
                            schemaDF= schemaDF.select([F.col(col).alias(col.replace("(","")) for col in schemaDF.columns])
                            schemaDF = schemaDF.select([F.col(col).alias(col.replace(")","")) for col in schemaDF.columns]) 
                            schemaDF = schemaDF.withColumn('DBName',lit(DBName)) 
                            schemaDF = schemaDF.withColumn('EntityName',lit(EntityName)) 
                            spark.conf.set(
                                    "spark.sql.sources.partitionOverwriteMode", "dynamic")
                            schemaDF.repartition(1).write.mode("overwrite").partitionBy("YearMonth").parquet(Abs_Path+"\Stage1\ParquetData\\"+parquet_tbl_name)
                        else:
                               
                                current_year=dt.datetime.now().year
                                current_month = dt.datetime.now().month
                                if current_month<Month:#
                                    current_year=current_year
                                else:
                                    current_year=current_year+1
                                FMonth_end = str(Month-1)
                                FMonth_start=str(Month)
                                for year_var in range(Year,current_year):
                                    Next_year = str(year_var+1)
                                    yearvar = str(year_var)
                                    tblquery = "(SELECT "+temp+" FROM ["+table_name+"] WHERE (YEAR(["+transact_column+"] )*100+MONTH(["+transact_column+"] )) between ("+yearvar+"*100+"+FMonth_start+") and ("+Next_year+"*100+ "+FMonth_end+") ) AS data2"
                                    schemaDF = spark.read.format("jdbc").options(url=sqlurl,dbtable=tblquery,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
                                    schemaDF= schemaDF.withColumn("YearMonth",year(schemaDF[transact_column])*100+date_format(schemaDF[transact_column],'MM'))
                                    schemaDF = schemaDF.withColumn("YearMonth",schemaDF['YearMonth'].cast(IntegerType()))
                                    schemaDF = schemaDF.withColumn("YearMonth",schemaDF['YearMonth'].cast(StringType()))
                                    schemaDF= schemaDF.select([F.col(col).alias(col.replace(" ","")) for col in schemaDF.columns])
                                    schemaDF= schemaDF.select([F.col(col).alias(col.replace("(","")) for col in schemaDF.columns])
                                    schemaDF = schemaDF.select([F.col(col).alias(col.replace(")","")) for col in schemaDF.columns]) 
                                    schemaDF = schemaDF.withColumn('DBName',lit(DBName)) 
                                    schemaDF = schemaDF.withColumn('EntityName',lit(EntityName))          
                                    schemaDF.coalesce(1).write.mode("append").partitionBy("YearMonth").parquet(Abs_Path+"\Stage1\ParquetData\\"+parquet_tbl_name)
                                year_var+=1
                        logger.endExecution()
                        try:
                            IDEorBatch = sys.argv[1]
                        except Exception as e :
                            IDEorBatch = "IDLE"
                    
                        log_dict = logger.getSuccessLoggedRecord("DataIngestion_"+table_name, DBName, EntityName, schemaDF.count(), len(schemaDF.columns), IDEorBatch)
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
                        
                    
                    os.system("spark-submit "+Kockpit_Path+"\Email.py 1  'DataIngestion_'"+Table['Table']+" "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"")   
                    log_dict = logger.getErrorLoggedRecord('DataIngestion_'+table_name, '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
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
                
            
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1  DataIngestion "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"")   
            log_dict = logger.getErrorLoggedRecord('DataIngestion','', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)     
print('DataIngestion completed: ' + str((dt.datetime.now()-st).total_seconds()))   