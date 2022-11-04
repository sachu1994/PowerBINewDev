from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, concat,concat_ws,col
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str
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
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster("local[*]").setAppName("Item").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",'*').\
                    set("spark.driver.maxResultSize","0").\
                    set("spark.sql.debug.maxToStringFields", "1000").\
                    set("spark.executor.instances", "20").\
                    set('spark.scheduler.mode', 'FAIR').\
                    set("spark.sql.broadcastTimeout", "36000").\
                    set("spark.network.timeout", 10000000).\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()   
            columns=Kockpit.TableRename("Item")
            Item=spark.read.format("parquet").load(STAGE1_PATH+"/Item" )
            Item =Item.select("No_","ManufacturerCode","Description","ItemCategoryCode","ProductGroupCode","BaseUnitofMeasure")
            Item = Item.withColumn("ProductGroupCode",concat_ws("|",Item.ItemCategoryCode,Item.ProductGroupCode))
             
            ItemCategory=spark.read.format("parquet").load(STAGE1_PATH+"/Item Category")
            ItemCategory =ItemCategory.select("Code","Description")
           
            ItemCategory = ItemCategory.withColumnRenamed("Code","ItemCategoryCode").withColumnRenamed("Description","ItemCategory")
            ItemCategory = ItemCategory.select('ItemCategory','ItemCategoryCode')
            ItemCategory = ItemCategory.withColumnRenamed('ItemCategory','ProductCategoty')
             
            ProductGroup=spark.read.format("parquet").load(STAGE1_PATH+"/Product Group")
            
            ProductGroup=ProductGroup.select("Code","ItemCategoryCode","Description")
            
            ProductGroup = ProductGroup.withColumn("ProductGroupCode",concat_ws("|",ProductGroup.ItemCategoryCode,ProductGroup.Code))
            ProductGroup = ProductGroup.drop('ItemCategoryCode')\
                                    .withColumnRenamed('Code','ProductGroup')\
                                    .withColumnRenamed('Description','ProductGroupDescription')
            
            Item = LJOIN(Item,ItemCategory,'ItemCategoryCode')
            finalDF = LJOIN(Item,ProductGroup,'ProductGroupCode')
            finalDF = RENAME(finalDF,columns)
            finalDF=finalDF.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
            finalDF = finalDF.withColumn('Link Item Key',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["Link Item"]))
            
            finalDF = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace("(","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace(")","")) for col in finalDF.columns])      
            finalDF.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Masters/Item")
         
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                    IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("Item", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Item '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")  
            log_dict = logger.getErrorLoggedRecord('Item', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_Item completed: ' + str((dt.datetime.now()-st).total_seconds()))