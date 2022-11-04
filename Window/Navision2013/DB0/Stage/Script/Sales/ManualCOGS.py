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
conf = SparkConf().setMaster("local[*]").setAppName("ManualCOGS")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession

IMpath =ConnectionDB+"\\"+"Configuration"+"\\"+'IncrementalMonth.txt'
IMfile=open(IMpath)
imonth = IMfile.read()
imonth=int(imonth)
columns = StructType([])
cy = date.today().year
cm = date.today().month
finalDF = spark.createDataFrame(data = [],schema=columns)
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true':
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger =Logger()
            Path= Connection+"\Stage1\ConfiguratorData\\CompanyDetail"
            CompanyDetail =spark.read.parquet(Path)
            table_names = spark.read.format("jdbc").options(url=PostgresDbInfo.PostgresUrl,dbtable='information_schema.tables',user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load().\
            filter("table_schema = 'sales'").select("table_name")
            table_names_list = [row.table_name for row in table_names.collect()]
            NoofRows = CompanyDetail.count()
            if 'manualcogs' in table_names_list:
                cur = conn.cursor()
                cur.execute('DELETE FROM sales.manualcogs WHERE "PostingDate" >=  date_trunc(\'month\', CURRENT_DATE)- INTERVAL \''+str(imonth)+' months\'')  
                conn.commit()
                conn.close()   
                for i in range(NoofRows):    
                    DBName=(CompanyDetail.collect()[i]['DBName'])
                    EntityName =(CompanyDetail.collect()[i]['EntityName'])
                    for i in range(imonth+1):    
                        mon = cm-i
                        if mon==0:
                            cy = cy-1
                            mon = mon+12
                        if mon<10:
                            mon = '0'+str(mon)
                        mon = str(mon)    
                        ym = str(cy)+mon
                        Path = Abs_Path+"/"+DBName+"/"+EntityName+"\Stage2\ParquetData\Sales\ManualCOGS\\YearMonth="+ym+""
                        if os.path.exists(Path):
                            partitionedDF = spark.read.parquet(Path)
                            partitionedDF =partitionedDF.withColumn('YearMonth',lit(int(ym)))
                            finalDF = partitionedDF.unionByName(finalDF, allowMissingColumns = True)
                    finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Sales.ManualCOGS", mode=apmode, properties=PostgresDbInfo.props)
            else:  
                for i in range(NoofRows):    
                    DBName=(CompanyDetail.collect()[i]['DBName'])
                    EntityName =(CompanyDetail.collect()[i]['EntityName'])
                    Path = Abs_Path+"/"+DBName+"/"+EntityName+"\\Stage2\\ParquetData\\Sales\ManualCOGS"
                    if os.path.exists(Path):
                        finalDF=spark.read.parquet(Path)
                        if i==0:
                            finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Sales.ManualCOGS", mode=owmode, properties=PostgresDbInfo.props)
                        else:
                            finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Sales.ManualCOGS", mode=apmode, properties=PostgresDbInfo.props)
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            
            log_dict = logger.getSuccessLoggedRecord("Sales.ManualCOGS", DB0," ", finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"\Email.py 1 ManualCOGS '"+CompanyName+"' "+DB0+" "+str(exc_traceback.tb_lineno)+"")
            log_dict = logger.getErrorLoggedRecord('Sales.ManualCOGS', DB0 ," " , str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('Sales_ManualCOGS: ' + str((dt.datetime.now()-st).total_seconds()))      