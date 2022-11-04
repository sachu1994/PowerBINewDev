from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit,concat_ws,max as max_,concat
from datetime import timedelta, date
import re,os,datetime,time,sys,traceback
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

def masters_Item(sqlCtx, spark):
    logger = Logger()

    try:
        ITEM_Entity = next(table for table in config["TablesToIngest"] if table["Table"] == "Item")
        print(ITEM_Entity)
        ITEMCAT_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Item Category")
        PRODGROUP_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Product Group") 
        table_rename = next (table for table in config["TablesToRename"] if table["Table"] == "Item")
        columns = table_rename["Columns"][0]
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            Item = ToDFWitoutPrefix(sqlCtx, hdfspath, ITEM_Entity,True)
            Item.show()
            Item = Item.withColumn("ProductGroupCode",concat_ws("|",Item.ItemCategoryCode,Item.ProductGroupCode))
            
            ItemCategory = ToDFWitoutPrefix(sqlCtx, hdfspath, ITEMCAT_Entity,False)
            ItemCategory = ItemCategory.withColumnRenamed("Code","ItemCategoryCode").withColumnRenamed("Description","ItemCategory")
            ItemCategory = ItemCategory.select('ItemCategory','ItemCategoryCode')
            
            ProductGroup = ToDFWitoutPrefix(sqlCtx, hdfspath, PRODGROUP_Entity,True)
            ProductGroup = ProductGroup.withColumn("ProductGroupCode",concat_ws("|",ProductGroup.ItemCategoryCode,ProductGroup.Code))
            ProductGroup = ProductGroup.drop('ItemCategoryCode')\
                                    .withColumnRenamed('Code','ProductGroup')\
                                    .withColumnRenamed('Description','ProductGroupDescription')
            
            Item = LJOIN(Item,ItemCategory,'ItemCategoryCode')
            finalDF = LJOIN(Item,ProductGroup,'ProductGroupCode')
            finalDF.printSchema()
            finalDF = RENAME(finalDF,columns)
            finalDF = finalDF.withColumn('DB',lit(DBName))\
                    .withColumn('Entity',lit(EntityName))
            finalDF = finalDF.withColumn('Link Item Key',concat(finalDF["DB"],lit('|'),finalDF["Entity"],lit('|'),finalDF["Link Item"]))
            finalDF.printSchema()
            
            finalDF.write.jdbc(url=postgresUrl, table="Masters.Item", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Item", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Item', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
            

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Item")
    masters_Item(sqlCtx, spark)
    