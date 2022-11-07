from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import  when,to_date
from pyspark.sql.types import *
from os.path import dirname, join, abspath
import os,sys
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
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("SalesTarget")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryoserializer.buffer.max","512m")\
        .set("spark.cores.max","24")\
        .set("spark.executor.memory","4g")\
        .set("spark.driver.memory","24g")\
        .set("spark.driver.maxResultSize","20g")\
        .set("spark.memory.offHeap.enabled",'true')\
        .set("spark.memory.offHeap.size","100g")\
        .set('spark.scheduler.mode', 'FAIR')\
        .set("spark.sql.broadcastTimeout", "36000")\
        .set("spark.network.timeout", 10000000)\
        .set("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")\
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .set("spark.databricks.delta.vacuum.parallelDelete.enabled",'true')\
        .set("spark.databricks.delta.retentionDurationCheck.enabled",'false')\
        .set('spark.hadoop.mapreduce.output.fileoutputformat.compress', 'false')\
        .set("spark.rapids.sql.enabled", True)\
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
import delta
from delta.tables import *
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            
            logger = Logger()
            GLB = spark.read.format("delta").load(STAGE1_PATH+"/G_L Budget Entry")
            GLMap = spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            DSE =spark.read.format("delta").load(STAGE1_PATH+"/Dimension Set Entry")
            GLB=GLB.withColumn("LinkDate",to_date(GLB.Date))\
                    .withColumn("Amount",GLB.Amount*-1).drop('Date')
            GLB=RENAME(GLB,{"G_LAccountNo_":"GLAccount","BudgetDimension1Code":"RSM_TMC"
                                       ,"BudgetDimension2Code":"Link_TARGETPROD","DimensionSetID":"DimSetID"})
            GLB=GLB.withColumn('LinkSalesPerson',when(GLB['RSM_TMC']=='4485', GLB['RSM_TMC']).otherwise(GLB['BudgetDimension3Code']))
            SUBBU=DSE.filter(DSE['DimensionCode']=='SUBBU')
            GLB = GLB.join(SUBBU, GLB['DimSetId']==SUBBU['DimensionSetId'], 'left')
            GLB = GLB.withColumnRenamed("DimensionValueCode","LINK_SUBBU")
            GLB = GLB.select("LinkDate","Amount","GLAccount","RSM_TMC","DimSetID","Link_TARGETPROD","LINK_SUBBU","BudgetName","LinkSalesPerson")
            SBU=DSE.filter(DSE['DimensionCode']=='SBU')
            GLB = GLB.join(SBU, GLB['DimSetId']==SBU['DimensionSetId'], 'left')
            GLB = GLB.withColumnRenamed("DimensionValueCode","LINK_SBU")
            GLB = GLB.select("LinkDate","Amount","GLAccount","RSM_TMC","DimSetID","Link_TARGETPROD","LINK_SUBBU","LINK_SBU","BudgetName","LinkSalesPerson")
            GLMap = GLMap.withColumnRenamed('GLRangeCategory','GLCategory')\
                                .withColumnRenamed('FromGLCode','FromGL')\
                                .withColumnRenamed('ToGLCode','ToGL')
            GLRange = GLMap.filter(GLMap["GLCategory"] == 'REVENUE').filter(GLMap["DBName"] == DBName)\
                                    .filter(GLMap["EntityName"] == EntityName).select("GLCategory","FromGL","ToGL")
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                        if i==0:
                            Range = (GLB.GLAccount>=GLRange.select('FromGL').collect()[0]['FromGL']) \
                                & (GLB.GLAccount<=GLRange.select('ToGL').collect()[0]['ToGL'])
                
                        else:
                            Range = (Range) | ((GLB.GLAccount>=GLRange.select('FromGL').collect()[i]['FromGL']) \
                                               & (GLB.GLAccount<=GLRange.select('ToGL').collect()[i]['ToGL']))              
            GLB=GLB.filter(GLB['BudgetName'].like('SALESTGT%'))\
                     .filter(Range)
            GLB.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/SalesTarget")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Sales.SalesTarget", DBName, EntityName, GLB.count(), len(GLB.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Kockpit/Email.py 1 SalesTarget '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            
            log_dict = logger.getErrorLoggedRecord('Sales.SalesTarget', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_SalesTarget completed: ' + str((dt.datetime.now()-st).total_seconds()))