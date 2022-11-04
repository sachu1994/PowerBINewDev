from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import count,concat,lit, year, current_date, month
import os,sys
from os.path import dirname, join, abspath
import datetime as dt
from datetime import date

ConnectionDB =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1'))
sys.path.insert(0, ConnectionDB)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1','E1'))
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..')) 
Kockpit_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DB0_Path =abspath(join(join(dirname(__file__), '..'),'..','..'))
DB0 =os.path.split(DB0_Path)
DB0 = DB0[1]
owmode = 'overwrite'
apmode = 'append'                           
st = dt.datetime.now()
conf = SparkConf().setMaster("local[*]").setAppName("SalesTarget")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true':
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS Sales.SalesTarget;')  
            conn.commit()
            conn.close()
        except Exception as ex:
            exc_type,exc_value,exc_traceback=sys.exc_info()
            print("Error:",ex)
            print("type - "+str(exc_type))
            print("File - "+exc_traceback.tb_frame.f_code.co_filename)
            print("Error Line No. - "+str(exc_traceback.tb_lineno))
        
        try:
            logger =Logger()
            Path= Connection+"\Stage1\ConfiguratorData\\CompanyDetail"
            CompanyDetail =spark.read.parquet(Path)
            NoofRows = CompanyDetail.count()   
            for i in range(NoofRows):    
                DBName=(CompanyDetail.collect()[i]['DBName'])
                EntityName =(CompanyDetail.collect()[i]['EntityName'])
                Path = Abs_Path+"/"+DBName+"/"+EntityName+"\\Stage2\\ParquetData\\Sales\SalesTarget"
                if os.path.exists(Path):
                    
                    finalDF=spark.read.parquet(Path)
                    if i==0:
                        finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Sales.SalesTarget", mode=owmode, properties=PostgresDbInfo.props)
                    else:
                        finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Sales.SalesTarget", mode=apmode, properties=PostgresDbInfo.props)
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            
            log_dict = logger.getSuccessLoggedRecord("Sales.SalesTarget", DB0," ", finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 SalesTarget '"+CompanyName+"' "+DB0+" "+str(exc_traceback.tb_lineno)+"")
            log_dict = logger.getErrorLoggedRecord('Sales.SalesTarget', DB0 ," " , str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('Sales_SalesTarget: ' + str((dt.datetime.now()-st).total_seconds()))      