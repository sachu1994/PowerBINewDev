
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

conf = SparkConf().setMaster("local[*]").setAppName("Item")
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
            Item=spark.read.parquet("../../../Stage1/ParquetData/Item")
            Item = Item.select("No_","ManufacturerCode","Description","DBName","EntityName","ItemCategoryCode","ProductGroupCode","BaseUnitofMeasure")
        
            Item= Item.drop('DBName','EntityName')
            Item = Item.withColumn("ProductGroupCode",concat_ws("|",Item.ItemCategoryCode,Item.ProductGroupCode))
            ItemCategory=spark.read.parquet("../../../Stage1/ParquetData/ItemCategory")
            ItemCategory =ItemCategory.select("Code","Description")
           
            ItemCategory = ItemCategory.withColumnRenamed("Code","ItemCategoryCode").withColumnRenamed("Description","ItemCategory")
            ItemCategory = ItemCategory.select('ItemCategory','ItemCategoryCode')
           
            ProductGroup=spark.read.parquet("../../../Stage1/ParquetData/ProductGroup" )
            ProductGroup =ProductGroup.select("Code","ItemCategoryCode","DBName","EntityName","Description")
          
            ProductGroup = ProductGroup.withColumn("ProductGroupCode",concat_ws("|",ProductGroup.ItemCategoryCode,ProductGroup.Code))
            ProductGroup = ProductGroup.drop('ItemCategoryCode')\
                                    .withColumnRenamed('Code','ProductGroup')\
                                    .withColumnRenamed('Description','ProductGroupDescription')
            
            Item = Kockpit.LJOIN(Item,ItemCategory,'ItemCategoryCode')
            finalDF = Kockpit.LJOIN(Item,ProductGroup,'ProductGroupCode').drop('ProductGroupCode')
            finalDF = Kockpit.RENAME(finalDF,columns)
            finalDF = finalDF.withColumn('Link Item Key',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["Link Item"]))
            finalDF = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace("(","")) for col in finalDF.columns])
            finalDF = finalDF.select([F.col(col).alias(col.replace(")","")) for col in finalDF.columns])      
            finalDF.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Master/Item")
            print(finalDF.columns)
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
            ex = str(ex)
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 Item '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")   
            log_dict = logger.getErrorLoggedRecord('Item', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_Item completed: ' + str((dt.datetime.now()-st).total_seconds()))