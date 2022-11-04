
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year,when,to_date,col
from pyspark.sql.types import *
import re,os,sys,datetime,time,sys,traceback
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str
from datetime import date
begintime =dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
sys.path.insert(0,'../../../..')
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
st = dt.datetime.now()
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
conf = SparkConf().setMaster("local[*]").setAppName("SalesTarget").\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
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
            GLB = spark.read.parquet("../../../Stage1/ParquetData/G_LBudgetEntry").drop("DBName","EntityName")
            GLB=GLB.select("EntryNo_","BudgetDimension1Code",'BudgetDimension2Code',"BudgetDimension3Code","DimensionSetID","BudgetName","Description","G_LAccountNo_","Date","Amount")
            
            GLMap = spark.read.parquet("../../../Stage1/ConfiguratorData\\tblGLAccountMapping")
         
            GLB=GLB.withColumn("LinkDate",to_date(GLB.Date))\
                    .withColumn("Amount",GLB.Amount*-1).drop('Date')
            
            GLB=RENAME(GLB,{"G_LAccountNo_":"GLAccount","BudgetDimension1Code":"RSM_TMC"
                                       ,"BudgetDimension2Code":"Link_TARGETPROD","DimensionSetID":"DimSetID"})
            GLB=GLB.withColumn('LinkSalesPerson',when(GLB['RSM_TMC']=='4485', GLB['RSM_TMC']).otherwise(GLB['BudgetDimension3Code']))
            DSE = spark.read.parquet("../../../Stage1/ParquetData/DimensionSetEntry")
        
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
            GLB.coalesce(1).write.mode("overwrite").parquet("../../../Stage2/ParquetData/Sales/SalesTarget")
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 SalesTarget '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
            
            log_dict = logger.getErrorLoggedRecord('Sales.SalesTarget', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_SalesTarget completed: ' + str((dt.datetime.now()-st).total_seconds()))