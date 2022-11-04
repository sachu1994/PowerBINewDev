import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit,when,concat,concat_ws,col,to_date,max as max_
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime,time,sys
import traceback,os,re
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

helpersDir = '/root/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()
stime = start_time.strftime('%H:%M:%S')
schema_log = StructType([
                        StructField('Date',StringType(),True),
                        StructField('Start_Time',StringType(),True),
                        StructField('End_Time', StringType(),True),
                        StructField('Run_Time',StringType(),True),
                        StructField('File_Name',StringType(),True),
                        StructField('DB',StringType(),True),
                        StructField('EN', StringType(),True),
                        StructField('Status',StringType(),True),
                        StructField('Log_Status',StringType(),True),
                        StructField('ErrorLineNo.',StringType(),True),
                        StructField('Rows',IntegerType(),True),
                        StructField('Columns',IntegerType(),True),
                        StructField('Source',StringType(),True)
                ])

try:
    config = os.path.dirname(os.path.realpath(__file__))
    DBET = config[config.rfind("/")+1:]
    Etn = DBET[DBET.rfind("E"):]
    DB = DBET[:DBET.rfind("E")]
    config = config[0:config.rfind("Stage")]
    config = pd.read_csv(config+"/conf.csv")
    for i in range(0,len(config)):
        exec(str(config.iloc[i]['Var'])+"="+chr(34)+str(config.iloc[i]['Val'])+chr(34))
    
    conf = SparkConf().setMaster('local[8]').setAppName("SalesCreditMemo")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.executor.memory","30g")
    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("SalesCreditMemo").getOrCreate()
    POSTGREURL = '192.10.15.57'
    PGUSER = 'postgres'
    PGPWD = 'sa@123'
    POSTGREDB = 'DB1E1'
    PGSCHEMA = 'crm'
    Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB

    Postgresprop= {"user":PGUSER,"password":PGPWD,"driver": "org.postgresql.Driver" }
    
    ####################### DB Credentials  ###########################
    csvpath=os.path.dirname(os.path.realpath(__file__))
    crm_dh = sqlctx.read.parquet(hdfspath+"/CRM/crm_documentheader")
    crm_dh=crm_dh.filter(crm_dh.deleted == '0')
    #crm_dh=crm_dh.filter(crm_dh.documentId == '54901')
    #crm_dh.show()
    #crm_dh.printSchema()
    #exit()
    Deals = crm_dh[(crm_dh.documentType=="Deal")]
    
    WindowAgg = Window.partitionBy('documentId')
    
    
    from pyspark.sql.functions import col,sum,min,max,row_number
    Deals = Deals.withColumn("LinkCreateDate",max(col("createDate")).over(WindowAgg))
    #Deals.show(5)
    #exit()
    crm_dh = sqlctx.read.parquet(hdfspath+"/CRM/crm_dealhistory")
    deals_list = Deals.columns
    crm_dh_list = crm_dh.columns
    
    #exit()
    combine_list = [deals_list,crm_dh_list]
    #print(combine_list)
    duplicates = list(set.intersection(*map(set, combine_list)))
    drop_col = []
    for name in duplicates:
        if name=='documentId':
            continue
        else:
            drop_col.append(name)
    print(drop_col)
    crm_dh = crm_dh.drop(*drop_col)
    merge = Deals.join(crm_dh, on="documentId", how="left")
    merge.show()
    merge = merge.drop("uuid_")
    #merge.show()
    #exit()
    
    
    merge = merge.withColumnRenamed("uuid_","uuid").withColumnRenamed("active_","active")
    #merge.printSchema()
    #exit()
    merge = merge.dropDuplicates(["documentId"]) 
    merge.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Deals", mode=owmode, properties=Postgresprop)
    #exit()

    crm_dh = sqlctx.read.parquet(hdfspath+"/CRM/crm_documentheader")
    #crm_dh.printSchema()
    #exit()
    Leads = crm_dh[(crm_dh.documentType=="Lead")]
    WindowAgg = Window.partitionBy('documentId')


    from pyspark.sql.functions import col,sum,min,max,row_number
    Leads = Leads.withColumn("LinkCreateDate",max(col("createDate")).over(WindowAgg))
    crm_dh = sqlctx.read.parquet(hdfspath+"/CRM/crm_leadhistory")
    leads_list = Leads.columns
    crm_dh_list = crm_dh.columns
    combine_list = [leads_list,crm_dh_list]
    print(combine_list)
    duplicates = list(set.intersection(*map(set, combine_list)))
    drop_col = []
    for name in duplicates:
        if name=='documentId':
            continue
        else:
            drop_col.append(name)
    print(drop_col)
    crm_dh = crm_dh.drop(*drop_col)
    merge = Leads.join(crm_dh, on="documentId", how="left")
    #merge.show()
    merge = merge.drop("uuid_")
    #merge.show()
    #exit()


    merge = merge.withColumnRenamed("uuid_","uuid").withColumnRenamed("active_","active")
    #merge.printSchema()
    merge = merge.dropDuplicates(["documentId"])
    merge.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Leads", mode=owmode, properties=Postgresprop)
    
    crm_sbu = sqlctx.read.parquet(hdfspath+"/CRM/crm_sbu")
    #crm_sbu.printSchema()
    #exit()

    crm_sbu.write.jdbc(url=Postgresurl, table=PGSCHEMA+".SBU", mode=owmode, properties=Postgresprop)

    crm_cp = sqlctx.read.parquet(hdfspath+"/CRM/crm_company")
    #crm_cp.printSchema()
    #exit()

    crm_cp.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Company", mode=owmode, properties=Postgresprop)

    crm_st = sqlctx.read.parquet(hdfspath+"/CRM/crm_solution")
    #crm_st.printSchema()
    #exit()

    crm_st.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Solution", mode=owmode, properties=Postgresprop)

    crm_p = sqlctx.read.parquet(hdfspath+"/CRM/crm_product")
    #crm_p.printSchema()
    #exit()

    crm_p.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Product", mode=owmode, properties=Postgresprop)

    crm_c = sqlctx.read.parquet(hdfspath+"/CRM/crm_contact")
    #crm_c.printSchema()
    #exit()

    crm_c.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Contact", mode=owmode, properties=Postgresprop)

    crm_dp = sqlctx.read.parquet(hdfspath+"/CRM/crm_department")
    #crm_dp.printSchema()
    #exit()

    crm_dp.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Department", mode=owmode, properties=Postgresprop)

    crm_dpm = sqlctx.read.parquet(hdfspath+"/CRM/crm_documentproductmap")
    #crm_dpm.printSchema()
    #exit()

    crm_dpm.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Documentproductmap", mode=owmode, properties=Postgresprop)

    crm_in = sqlctx.read.parquet(hdfspath+"/CRM/crm_industry")
    #crm_in.printSchema()
    #exit()

    crm_in.write.jdbc(url=Postgresurl, table=PGSCHEMA+".Industry", mode=owmode, properties=Postgresprop)
    
    crm_lh = sqlctx.read.parquet(hdfspath+"/CRM/crm_leadhistory")
    #crm_lh.printSchema()
    #exit()

    crm_lh.write.jdbc(url=Postgresurl, table=PGSCHEMA+".leadhistory", mode=owmode, properties=Postgresprop)
    
    user=sqlctx.read.parquet(hdfspath+"/CRM/user_")
    user.write.jdbc(url=Postgresurl, table=PGSCHEMA+".user", mode=owmode, properties=Postgresprop)

    crm_cpromap = sqlctx.read.parquet(hdfspath+"/CRM/crm_companyprojectmap")
    crm_cpromap.write.jdbc(url = Postgresurl, table=PGSCHEMA+".Company_Project_Map", mode = owmode ,properties = Postgresprop)

    crm_campaigns = sqlctx.read.parquet(hdfspath+"/CRM/crm_campaigns")
    crm_campaigns.write.jdbc(url = Postgresurl,table = PGSCHEMA+".Campaigns",mode = owmode, properties = Postgresprop)

    crm_techmap = sqlctx.read.parquet(hdfspath+"/CRM/crm_companytechnologymap")
    crm_techmap.write.jdbc(url = Postgresurl,table = PGSCHEMA+".Company_Technology_Map",mode = owmode, properties = Postgresprop)

    crm_event = sqlctx.read.parquet(hdfspath+"/CRM/crm_event")
    crm_event.write.jdbc(url = Postgresurl, table = PGSCHEMA+".Events",mode =owmode, properties = Postgresprop)

    crm_salerepre = sqlctx.read.parquet(hdfspath+"/CRM/crm_insidesalesrepresentative")
    crm_salerepre.write.jdbc(url = Postgresurl,table = PGSCHEMA+".Inside_Sales_Representative",mode = owmode, properties = Postgresprop)

    data_req = sqlctx.read.parquet(hdfspath+"/CRM/crm_invaliddatarequest")
    data_req.write.jdbc(url = Postgresurl,table = PGSCHEMA+".Invalid_Data_Request",mode = owmode, properties = Postgresprop)

    crm_node = sqlctx.read.parquet(hdfspath+"/CRM/crm_node")
    crm_node.write.jdbc(url = Postgresurl,table = PGSCHEMA+".Node", mode = owmode, properties = Postgresprop)

    crm_task = sqlctx.read.parquet(hdfspath+"/CRM/crm_task")
    crm_task.write.jdbc(url = Postgresurl, table = PGSCHEMA+".Task",mode = owmode,properties = Postgresprop)

    crm_technology = sqlctx.read.parquet(hdfspath+"/CRM/crm_technology")
    crm_technology.write.jdbc(url = Postgresurl, table = PGSCHEMA+".Technology", mode = owmode,properties = Postgresprop)

    crm_tech_cat = sqlctx.read.parquet(hdfspath+"/CRM/crm_technologycategory")
    crm_tech_cat.write.jdbc(url = Postgresurl, table = PGSCHEMA+".Technology_Category",mode = owmode, properties = Postgresprop)

    crm_workflow = sqlctx.read.parquet(hdfspath+"/CRM/crm_workflowactivity")
    crm_workflow.write.jdbc(url = Postgresurl, table = PGSCHEMA+".Workflow_Activity",mode = owmode, properties = Postgresprop)


    exit()















    

    
    Calendar_StartDate = CmpDt.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
    if datetime.date.today().month>int(MnSt)-1:
            UIStartYr=datetime.date.today().year-int(yr)+1
    else:
            UIStartYr=datetime.date.today().year-int(yr)
    UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
    UIStartDate=max(Calendar_StartDate,UIStartDate)
    
    ################################ Start Date #################################
    scml = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/SalesCr_MemoLine")
    sih = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/SalesCr_MemoHeader")
    gps = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/GeneralPostingSetup")
    pld = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/PostedStrOrderLineDetails")
    VE = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/ValueEntry")
    col_names = sqlctx.read.parquet(hdfspath+"/Stage2/Stage2").filter(col("Script_Name")=="SalesCreditMemo")
    
    col_scml = col_names.filter(col("Table_Name")=="SalesCr_MemoLine").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    col_sih = col_names.filter(col("Table_Name")=="SalesCr_MemoHeader").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    col_gps = col_names.filter(col("Table_Name")=="GeneralPostingSetup").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    col_pld = col_names.filter(col("Table_Name")=="PostedStrOrderLineDetails").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    col_dse = col_names.filter(col("Table_Name")=="DimensionSetEntry").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    col_VE = col_names.filter(col("Table_Name")=="ValueEntry").select("Col_Name").rdd.flatMap(lambda x: x).collect()
    
    scml = scml.select(col_scml)
    scml = scml.filter((year(col("PostingDate"))!='1753')&(col("Quantity")!=0)&(col("Type")!=4)&(to_date(col("PostingDate"))>=str(UIStartDate)))
    Line = scml.withColumn("LinkItem",when(scml["Type"]==2,when(scml["No_"]=='',lit("NA")).otherwise(scml["No_"])))\
           .withColumn("No",when(col("Type")==1,col("No_").cast('int')))\
           .withColumn("GL_Link",concat_ws("|",scml.Gen_Bus_PostingGroup,scml.Gen_Prod_PostingGroup))\
           .withColumn("LinkValueEntry",concat_ws("|",scml.DocumentNo_.cast('string'),scml.LineNo_.cast('string'),to_date(scml.PostingDate).cast('string')))\
           .withColumn("Link_GD",concat_ws("-",scml.DocumentNo_,scml.LineNo_))\
           .withColumn("LinkLocationCode",when(col("LocationCode")=='',"NA").otherwise(col("LocationCode")))\
           .withColumn("ShipmentDate",to_date(col("ShipmentDate")))\
           .withColumn("InvoicedQuantity",when(col("Type")==2,col("Quantity")*(-1)))\
           .withColumn("ServiceTaxAmount",(scml.ServiceTaxeCessAmount+scml.ServiceTaxSHECessAmount+scml.ServiceTaxAmount)*(-1))\
           .withColumnRenamed("DimensionSetID","DimSetID").withColumnRenamed("DocumentNo_","Document_No").withColumnRenamed("LineNo_","LineNo")\
           .withColumnRenamed("Description","LineDescription").withColumnRenamed("Quantity","Trn_Quantity")\
           .withColumn("CrMemoAmount",col("Amount")*(-1))\
           .withColumn("TaxAmount",col("TaxAmount")*(-1))\
           .withColumn("ExciseAmount",col("ExciseAmount")*(-1))\
           .withColumnRenamed("Inv_DiscountAmount","InvDiscountAmount")\
           .withColumnRenamed("Sell-toCustomerNo_","SellToCustomerNumber").drop("PostingDate")
    
    
    sih = sih.select(col_sih)
    sih = sih.filter((year(sih.PostingDate)!='1753') & (to_date(sih.PostingDate)>=str(UIStartDate)))
    Header = sih.withColumn("LinkCustomer",when(sih['Bill-toCustomerNo_']=='','NA').otherwise(sih["Bill-toCustomerNo_"])).drop("Bill-toCustomerNo_","DBName","EntityName")\
            .withColumn("BilltoName",sih['Bill-toName']+" "+sih['Bill-toName2']).drop('Bill-toName','Bill-toName2')\
            .withColumn("BilltoAddress",sih['Bill-toAddress']+" "+sih['Bill-toAddress2']).drop('Bill-toAddress','Bill-toAddress2')\
            .withColumn("ShiptoName",sih['Ship-toName']+" "+sih['Ship-toName2']).drop('Ship-toName','Ship-toName2')\
            .withColumn("ShiptoAddress",sih['Ship-toAddress']+" "+sih['Ship-toAddress2']).drop('Ship-toAddress','Ship-toAddress2')\
            .withColumn("LinkDate",to_date(sih.PostingDate))\
            .withColumn("LinkSalesRep",when(sih.SalespersonCode=='','NA').otherwise(sih.SalespersonCode)).drop("SalespersonCode")\
            .withColumn("SellToCustomerName",sih["Sell-toCustomerName"]+" "+sih["Sell-toCustomerName2"]).drop("Sell-toCustomerName","Sell-toCustome")\
            .withColumn("Years",when(month(sih.PostingDate)>=MnSt,year(sih.PostingDate)).otherwise(year(sih.PostingDate)-1))\
            .withColumn("Quarters",when(month(sih.PostingDate)>=MnSt,concat(lit('Q'),(quarter(sih.PostingDate)-1))).otherwise(lit('Q4')))\
            .withColumn("MonthNum", month(sih.PostingDate))\
            .withColumnRenamed("No_","Sales_No").withColumnRenamed("Bill-toCity","BilltoCity").withColumnRenamed("Ship-toCode","ShiptoCode")\
            .withColumnRenamed("Ship-toCity","ShiptoCity").withColumnRenamed("OnHold","HoldStatus")\
            .withColumnRenamed("Due_Date","DueDate").withColumnRenamed("LocationCode","HeaderLocationCode")\
            .withColumnRenamed("Sell-toCity","SelltoCity").withColumnRenamed("Bill-toPostCode","BilltoPostCode")\
            .withColumnRenamed("Ship-toPostCode","ShiptoPostCode").withColumnRenamed("ExternalDocumentNo_","ExternalDocumentNo")
    
    Monthsdf = udf.MONTHSDF(sqlctx)
    mcond = [Header.MonthNum==Monthsdf.MonthNum1]
    Header = udf.LJOIN(Header,Monthsdf,mcond)
    Header = Header.drop('MonthNum','MonthNum1')
    
    gps = gps.select('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','SalesCreditMemoAccount','DBName','EntityName','COGSAccount','InventoryAdjmt_Account')
    gps = gps.withColumn('COGSAccount',when(gps['Gen_Bus_PostingGroup']=='VALUATION', gps['InventoryAdjmt_Account']).otherwise(gps['COGSAccount']))
    GL_Master = gps.withColumn("GL_LinkDrop",concat_ws('|',gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
               .withColumn("GLAccount",when(gps.SalesCreditMemoAccount=='',0).otherwise(gps.SalesCreditMemoAccount)).drop('SalesCreditMemoAccount','DBName','EntityName')
    
    GL_Master_COGS = gps.withColumn("GL_Link",concat_ws('|',gps.DBName,gps.EntityName,gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
                .withColumn("GLAccount",when(gps.COGSAccount=='',0).otherwise(gps.COGSAccount)).drop('COGSAccount','DBName','EntityName','SalesCreditMemoAccount')
    
    
    GLMap = sqlctx.read.parquet(hdfspath+"/Data/GLAccountMapping")
    GLMap = GLMap.withColumnRenamed("GLRangeCategory","GLCategory")
    GLMap = GLMap.withColumnRenamed("FromGLCode","FromGL")
    GLMap = GLMap.withColumnRenamed("ToGLCode","ToGL")
    
    GLRange = GLMap.filter(GLMap["GLCategory"] == 'REVENUE').filter(GLMap["DBName"] == DB)\
                              .select("GLCategory","FromGL","ToGL")
    
    SalesRange = GLMap.filter(GLMap["GLCategory"] == 'Sales Revenue').filter(GLMap["DBName"] == DB)\
                                     .select("GLCategory","FromGL","ToGL")
    
    ServRange = GLMap.filter(GLMap["GLCategory"] == 'Service Revenue').filter(GLMap["DBName"] == DB)\
                                    .select("GLCategory","FromGL","ToGL")
    
    cond = [Line.Document_No == Header.Sales_No]
    Sales = udf.LJOIN(Line,Header,cond)
    
    cond1 = [Sales.GL_Link == GL_Master.GL_LinkDrop]
    Sales = udf.LJOIN(Sales,GL_Master,cond1)
    Sales = Sales.drop("GL_LinkDrop","GL_Link")
    
    Sales = Sales.withColumn('GLs',when(Sales['GLAccount']==0, Sales['No']).otherwise(Sales['GLAccount']))
    
    Range='1=1'
    Range1='1=1'
    NoOfRows=GLRange.count()
    for i in range(0,NoOfRows):
        if i==0:
            Range = (Sales.GLAccount>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                & (Sales.GLAccount<=GLRange.select('ToGL').collect()[0]["ToGL"])
    
            Range1 = (Sales.No>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                & (Sales.No<=GLRange.select('ToGL').collect()[0]["ToGL"])
        else:
            Range = (Range) | (Sales.GLAccount>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                & (Sales.GLAccount<=GLRange.select('ToGL').collect()[i]["ToGL"])
    
            Range1 = (Range1) | (Sales.No>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                & (Sales.No<=GLRange.select('ToGL').collect()[i]["ToGL"])
    Sales = Sales.filter(((Sales.Type!=2) | ((Sales.Type==2) & (Range))) & ((Sales.Type!=1) | ((Sales.Type==1) & (Range1))))
    Sales = Sales.withColumn("SalesServAccount",when(Sales.Type==2,Sales.GLAccount).otherwise(when(Sales.Type==1,Sales.No).otherwise(lit(1))))
    
    vSalesRange=Sales['SalesServAccount']!=0
    NoOfRows=SalesRange.count()
    for i in range(0,NoOfRows):
        if i==0:
            FromGL = "%s"%SalesRange.select(SalesRange.FromGL).collect()[0]["FromGL"]
            ToGL = "%s"%SalesRange.select(SalesRange.ToGL).collect()[0]["ToGL"]
            vSalesRange = (Sales['SalesServAccount']>=FromGL) & (Sales['SalesServAccount']<=ToGL)
        else:
            FromGL = "%s"%SalesRange.select(SalesRange.FromGL).collect()[i]["FromGL"]
            ToGL = "%s"%SalesRange.select(SalesRange.ToGL).collect()[i]["ToGL"]
            vSalesRange = (Sales['SalesServAccount']>=FromGL) & (Sales['SalesServAccount']<=ToGL)
    vServRange=Sales['SalesServAccount']!=0
    NoOfRows=ServRange.count()
    for i in range(0,NoOfRows):
        if i==0:
            FromGL = "%s"%ServRange.select(ServRange.FromGL).collect()[0]["FromGL"]
            ToGL = "%s"%ServRange.select(ServRange.ToGL).collect()[0]["ToGL"]
            vServRange = (Sales['SalesServAccount']>=FromGL) & (Sales['SalesServAccount']<=ToGL)
        else:
            FromGL = "%s"%ServRange.select(ServRange.FromGL).collect()[i]["FromGL"]
            ToGL = "%s"%ServRange.select(ServRange.ToGL).collect()[i]["ToGL"]
            vServRange = (Sales['SalesServAccount']>=FromGL) & (Sales['SalesServAccount']<=ToGL)
    
    pld = pld.select(col_pld)
    pld = pld.filter((pld.Type == 2) & (pld.DocumentType==2) & (pld.Tax_ChargeType==0))
    LineDetails = pld.withColumn("Link_GDDrop",concat_ws('-',pld.InvoiceNo_.cast('string'),pld.LineNo_.cast('string'))).drop("InvoiceNo_","LineNo_")\
                                            .withColumnRenamed("AccountNo_","AccountNo")
    
    GLExclusion = sqlctx.read.parquet(hdfspath+"/Data/GLCodeExclusion")
    GLExclusion = GLExclusion.withColumn('GLExclusionFlag',lit(1))
    GLExclusion = GLExclusion.filter(GLExclusion['DBName'] == DB).filter(GLExclusion['EntityName'] == Etn).filter(GLExclusion['IsExcluded'] == 1)
    GLExclusion = GLExclusion.select("GLCode","GLExclusionFlag")
    
    ExclusionCond = [Sales.SalesServAccount == GLExclusion.GLCode]
    Sales = udf.LJOIN(Sales,GLExclusion,ExclusionCond)
    Sales=Sales.filter("GLExclusionFlag IS NULL").drop('GLExclusionFlag')
    
    Range2 = LineDetails['AccountNo']!=0
    NoOfRows = GLRange.count()
    for j in range(0,NoOfRows):
        if i==0:
            FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[0]['FromGL']
            ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[0]['ToGL']
            Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
        else:
            FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[i]['FromGL']
            ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[i]['ToGL']
            Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
    
    LineDetails = LineDetails[Range2]
    LineDetails=LineDetails.groupby('Link_GDDrop').sum('Amount').withColumnRenamed('sum(Amount)','ChargesToCustomer')
    cond2 = [Sales.Link_GD == LineDetails.Link_GDDrop]
    Sales = udf.LJOIN(Sales,LineDetails,cond2)
    Sales = Sales.drop('Link_GDDrop')
    
    Sales = Sales.withColumn('TransactionType',lit('CrMemo'))\
                        .withColumn('GLAccountNo',when(Sales.GLAccount.isNull(), Sales.No).otherwise(Sales.GLAccount))\
                        .withColumn('RevenueAmount',when((Sales.CurrencyFactor==0) | (Sales.CurrencyFactor.isNull()),when(Sales.ChargesToCustomer.isNull(),Sales.CrMemoAmount)\
                        .otherwise(Sales.CrMemoAmount+Sales.ChargesToCustomer)).otherwise(when(Sales.ChargesToCustomer.isNull(),(Sales.CrMemoAmount/Sales.CurrencyFactor))\
                        .otherwise(((Sales.CrMemoAmount+Sales.ChargesToCustomer)/(Sales.CurrencyFactor)))))\
                        .withColumn('RevenueType',when(vSalesRange,lit('Sales')).otherwise(when(vServRange,lit('Service')).otherwise(lit('Other'))))\
                        .withColumn('LinkLocation',when(Sales.LinkLocationCode.isNull(),Sales.HeaderLocationCode).otherwise(Sales.LinkLocationCode))\
                        .withColumn('InvoiceAmount',when((Sales.CurrencyFactor==0) | (Sales.CurrencyFactor.isNull()),when(Sales.ChargesToCustomer.isNull(),Sales.AmountIncludingTax)\
                        .otherwise(Sales.AmountIncludingTax+Sales.ChargesToCustomer)).otherwise(when(Sales.ChargesToCustomer.isNull(),(Sales.AmountIncludingTax/Sales.CurrencyFactor))\
                        .otherwise(((Sales.AmountIncludingTax+Sales.ChargesToCustomer)/(Sales.CurrencyFactor)))))\
                        .drop('SalesServAccount')
    
    Field = sqlctx.read.parquet(hdfspath+"/Data/FieldSelection")
    Field = Field.filter(Field['Flag'] == 1).filter(Field['TableName'] == 'VE FieldSelection')
    Field = Field.select("FieldType","Flag","TableName")
    FieldName = Field.select('FieldType').collect()[0]["FieldType"]
    FieldName = re.sub('[\s+]', '', FieldName)
    
    VE = VE.select(col_VE)
    VE = VE.filter(to_date(VE.PostingDate)>=str(UIStartDate))
    ValueEntry = VE.withColumn("LinkDate",to_date(VE.PostingDate))\
               .withColumn("LinkValueEntry1",concat_ws("|",VE.DocumentNo_.cast('string'),VE.DocumentLineNo_.cast('string'),to_date(VE.PostingDate).cast('string')))\
               .withColumnRenamed("DimensionSetID","DimSetID").withColumnRenamed("ItemNo_","LinkItem").withColumnRenamed("DocumentNo_","DocumentNo")\
               .withColumnRenamed("ItemLedgerEntryNo_","LinkILENo")
    
    if FieldName=='CostAmount(Expected)':
        ValueEntry = ValueEntry.withColumn("CostAmount",when(ValueEntry.CostAmountActual==0,ValueEntry.CostAmountExpected*(-1)).otherwise(ValueEntry.CostAmountActual*(-1)))
    else:
        ValueEntry = ValueEntry.withColumn("CostAmount",ValueEntry[FieldName]*(-1))
    
    ################################# Join to include COGS #########################
    ValueEntry1 = ValueEntry.select('LinkValueEntry1','CostAmount').groupby('LinkValueEntry1').agg({'CostAmount':'sum'})\
                        .withColumnRenamed('sum(CostAmount)','Cost_Amount').drop('CostAmount')
    
    ValueEntry1 = ValueEntry1.withColumnRenamed('Cost_Amount','CostAmount')
    
    JoinCOGS = [Sales.LinkValueEntry == ValueEntry1.LinkValueEntry1]
    Sales = udf.LJOIN(Sales,ValueEntry1,JoinCOGS)
    Sales = Sales.drop('LinkValueEntry1','CrMemoAmount')
    
    Documents = Sales.select('Document_No').distinct()
    Documents = Documents.withColumn('SysDocFlag',lit(1)).withColumnRenamed('Document_No','Document_NoDrop')
    SalesInvQuery="(SELECT * FROM "+DBET+".salesinvoice_linux) AS SalesInv"
    SalesInvoice = sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=SalesInvQuery,user=PGUSER,password=PGPWD,driver= "org.postgresql.Driver").load()
    
    DocumentsCrMemo = SalesInvoice.select("Document_No").distinct()
    DocumentsCrMemo = DocumentsCrMemo.withColumnRenamed("Document_No","Document_NoDrop").withColumn("SysDocFlag",lit(2))
    Documents=Documents.unionAll(DocumentsCrMemo)
    
    JoinDocs = [ValueEntry.DocumentNo == Documents.Document_NoDrop]
    ValueEntry= ValueEntry.join(Documents,JoinDocs,'left').drop("Document_NoDrop")
    
    SysEntries = Sales.select('LinkValueEntry').distinct()
    SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
    JoinSysEntry = [ValueEntry.LinkValueEntry1 == SysEntries.LinkValueEntry]
    ValueEntry= ValueEntry.join(SysEntries,JoinSysEntry,'left').drop("LinkValueEntry")\
                .withColumnRenamed('LinkValueEntry1','LinkValueEntry')
    
    ValueEntry = ValueEntry.withColumn("GL_Link",concat_ws('|',lit(DB),lit(Etn),ValueEntry.Gen_Bus_PostingGroup, ValueEntry.Gen_Prod_PostingGroup))
    ValueEntry = ValueEntry.join(GL_Master_COGS,'GL_Link','left').withColumnRenamed('GLAccount','GLs')
        
    ValueEntry = ValueEntry.filter(((ValueEntry.SysDocFlag==1)&(ValueEntry.SysValueEntryFlag.isNull()))|((ValueEntry.SysDocFlag.isNull())&(ValueEntry.ItemLedgerEntryType==1)))
    ValueEntry = ValueEntry.withColumnRenamed('DocumentNo','Document_No')\
                        .withColumnRenamed('DocumentLineNo_','LineNo')\
            .withColumnRenamed('EntryNo_','Link_GD')\
                        .withColumn('Type',lit(''))\
            .withColumn('No_',lit(''))\
                        .withColumn('No',lit(0))\
                        .withColumn('LinkLocationCode',lit(''))\
                        .withColumn('ShipmentDate',lit(''))\
                        .withColumn('LineDescription',lit(''))\
                        .withColumn('Trn_Quantity',ValueEntry['InvoicedQuantity'])\
                        .withColumn('UnitPrice',lit(0))\
                        .withColumn('UnitCostLCY',lit(0))\
                        .withColumn('LineDiscount_',lit(0))\
                        .withColumn('LineDiscountAmount',lit(0))\
                        .withColumn('InvDiscountAmount',lit(0))\
                        .withColumn('Tax_',lit(0))\
                        .withColumn('AmountIncludingTax',lit(0))\
                        .withColumn('TaxBaseAmount',lit(0))\
                        .withColumn('TaxAreaCode',lit(''))\
            .withColumn('TaxLiable',lit(0))\
                        .withColumn('TaxGroupCode',lit(''))\
            .withColumn('ServiceTaxGroup',lit(''))\
                        .withColumn('ServiceTaxBase',lit(''))\
            .withColumn('StandardDeduction_',lit(0))\
                        .withColumn('StandardDeductionAmount',lit(0))\
            .withColumn('MRPPrice',lit(0))\
                        .withColumn('MRP',lit(0))\
            .withColumn('Abatement_',lit(0))\
                        .withColumn('ServiceTaxAmount',lit(0))\
            .withColumn('TaxAmount',lit(0))\
                        .withColumn('ExciseAmount',lit(0))\
            .withColumn('SellToCustomerNumber',lit(''))\
                        .withColumn('Sales_No',lit(''))\
            .withColumn('LinkCustomer',lit(''))\
                        .withColumn('BilltoName',lit(''))\
            .withColumn('BilltoAddress',lit(''))\
                        .withColumn('BilltoCity',lit(''))\
            .withColumn('ShiptoCode',lit(''))\
                        .withColumn('ShiptoName',lit(''))\
            .withColumn('ShiptoAddress',lit(''))\
                        .withColumn('ShiptoCity',lit(''))\
            .withColumn('PaymentTermsCode',lit(''))\
                        .withColumn('DueDate',lit(''))\
            .withColumn('ShipmentMethodCode',lit(''))\
                        .withColumn('HeaderLocationCode',lit(''))\
            .withColumn('CurrencyCode',lit(''))\
                        .withColumn('CurrencyFactor',lit(''))\
            .withColumn('LinkSalesRep',lit(''))\
                        .withColumn('HoldStatus',lit(''))\
            .withColumn('SellToCustomerName',lit(''))\
                        .withColumn('SelltoCity',lit(''))\
            .withColumn('BilltoPostCode',lit(''))\
                        .withColumn('ExternalDocumentNo',lit(''))\
            .withColumn('ShiptoPostCode',lit(''))\
                        .withColumn('FormCode',lit(''))\
            .withColumn('Years',lit(0))\
                        .withColumn('Quarters',lit(''))\
            .withColumn('Months',lit(''))\
                        .withColumn('Structure',lit(''))\
            .withColumn('GLAccount',lit(0))\
                        .withColumn('ChargesTocustomer',lit(0))\
            .withColumn('GLAccountNo',lit(0))\
                        .withColumn('RevenueAmount',lit(0))\
            .withColumn('RevenueType',lit('OTHER'))\
                        .withColumn('TransactionType',lit('RevaluationandRemainingEntries'))\
                        .withColumnRenamed('LocationCode','LinkLocation')
    
    dse = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/DSE").drop('DBName','EntityName')
    
    scond = [Sales.DimSetID==dse.DimSetID1]
    Sales = udf.LJOIN(Sales,dse,scond)
    Sales = Sales.drop('DimSetID1')
    
    vcond = [ValueEntry.DimSetID==dse.DimSetID1]
    ValueEntry = udf.LJOIN(ValueEntry,dse,vcond)
    ValueEntry = ValueEntry.withColumn("PostingDate",col("PostingDate").cast('string'))\
            .withColumn("LinkDate",col("LinkDate").cast('string')).drop('DimSetID1')
    
    Sales = Sales.withColumn("PostingDate",col("PostingDate").cast('string'))\
            .withColumn("ShipmentDate",col("ShipmentDate").cast('string'))\
            .drop('LocationCode','Amount','ServiceTaxSHECessAmount','Sell-toCustomerName2','GLCode','Link_GDDrop')
    
    Sales = udf.UNALL(Sales,ValueEntry)
    Sales = Sales.withColumnRenamed('LinkSalesRep','LinkSalesPerson')
    
    ############################## Sales Person Field Selection #################################
    
    Field = sqlctx.read.parquet(hdfspath+"/Data/FieldSelection")
    
    FieldName = Field.filter(Field['Flag'] == 1).filter(Field['TableName'] == 'Sales Transaction')
    FieldName = FieldName.select("FieldType","Flag","TableName")
    
    vFieldSalesRep=FieldName.filter("TableName=='Sales Transaction'").select('FieldType').collect()[0]["FieldType"]
    
    if vFieldSalesRep=='Customer':
            Sales=Sales.drop('LinkSalesPerson')
            cust = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/Customer")
            col_cust = col_names.filter(col_names.Table_Name=="Customer").select(col_names.Col_Name).rdd.flatMap(lambda x: x).collect()
            cust = cust.select(col_cust)
            Customer = cust.filter(cust['No_']>0).withColumnRenamed("No_","Link1").withColumnRenamed("SalespersonCode","LinkSalesPerson").drop("DBName","EntityName")
            cond = [Sales.LinkCustomer == Customer.Link1]
            Sales = udf.LJOIN(Sales,Customer,cond)
            Sales = Sales.drop("Link1")
    
    qBudFields = sqlctx.read.parquet(hdfspath+"/Data/BudgetFieldNames")
    BudFields = qBudFields.distinct()
    
    qBudgetGD = sqlctx.read.parquet(hdfspath+"/Data/BudgetDimension")
    BudgetGD = qBudgetGD.distinct()
    
    if BudFields.count()+BudgetGD.count()>0:
        NoOfFields=BudFields.count()
        for i in range(0,NoOfFields):
            MasterTab = BudFields.select(BudFields['TableName']).collect()[i]["TableName"]
            MasterField = BudFields.select(BudFields['ColumnName']).collect()[i]["ColumnName"]
            MasterField = re.sub('[\s+]', '',MasterField)
            MasterCol = "Link"+MasterTab
            MasterFieldCol = MasterField
            MasterField = sqlctx.read.parquet(hdfspath+"/Stage2/"+DB+"/"+MasterTab)
            MasterField = MasterField.select(MasterCol,MasterFieldCol).withColumnRenamed(MasterCol,"Code")
            Sales=Sales.withColumnRenamed('Link'+MasterTab,'Codes')
            cond = [Sales.Codes == MasterField.Code]
            Sales=Sales.join(MasterField,cond,'left').drop('Code').withColumnRenamed('Codes','Link'+MasterTab)
    
        NoOfFields=BudgetGD.count()
        for i in range(0,NoOfFields):
            GDTab = BudgetGD.select(BudgetGD['DimensionCode']).collect()[i]["DimensionCode"]
            GDTab_level = BudgetGD.select(BudgetGD['LevelName']).collect()[i]["LevelName"]
            GDTab = re.sub('[\s+]', '',GDTab)
            GDTab_level = re.sub('[\s+]', '',GDTab_level)
            vVar=re.sub('[\s+]', '', GDTab)
            GDTabCol = "Link"+GDTab
            GDTab_levelCol = GDTab_level
            GD = sqlctx.read.parquet(hdfspath+"/Stage2/"+DB+"/"+vVar)
            GD = GD.select(GDTabCol,GDTab_levelCol).withColumnRenamed(GDTabCol,'Code')
            Sales=Sales.withColumnRenamed('Link_'+vVar,'Codes')
            cond = [Sales.Codes == GD.Code]
            Sales=Sales.join(GD,cond,'left').drop('Code').withColumnRenamed('Codes','Link_'+vVar)
    
        BudgetGD = BudgetGD.select('LevelName').withColumnRenamed('LevelName','BudColumn')
        BudField = BudFields.select('ColumnName').withColumnRenamed('ColumnName','BudColumn').unionAll(BudgetGD)
        var1_list = BudField.select('BudColumn').collect()
        NoOfFields = BudField.count()
        for i in range(0,NoOfFields):
            var1 = var1_list[i].BudColumn
            var1 = re.sub('[\s+]', '',var1)
            if i==0:
                Sales = Sales.withColumn('BudColumn',concat(Sales[var1],lit('|')))
                Sales = Sales.drop(Sales[var1])
            else:
                Sales = Sales.withColumn('BudColumn1',concat(Sales[var1],lit('|')))
                Sales = Sales.drop(Sales[var1])
    
        Sales = Sales.withColumn('BudColumnNew',concat(Sales.BudColumn,Sales.BudColumn1))\
                                .drop('BudColumn','BudColumn1').withColumnRenamed('BudColumnNew','BudColumn')
        Target = sqlctx.read.parquet(hdfspath+"/Data/DimensionBudget")
        vOutPeriod = Target.select('OutputPeriod').collect()[0]['OutputPeriod']
        if vOutPeriod=='Yearly':
            Sales = Sales.withColumn('BudField',concat(Sales.BudColumn,Sales.Years))
        elif vOutPeriod=='Quarterly':
            Sales = Sales.withColumn('BudField',concat(Sales.BudColumn,Sales.Quarters,Sales.Years))
        elif vOutPeriod=='Monthly':
            Sales = Sales.withColumn('BudField',concat(Sales.BudColumn,Sales.Months,Sales.Years))
    Sales = Sales.drop('BudColumn')
    
    #//////////////////////////Writing data//////////////////////////
    
    Sales = Sales.withColumn('LinkCustomerKey',concat_ws('|',Sales.DBName,Sales.EntityName,Sales.LinkCustomer))\
                        .withColumn('LinkDateKey',concat_ws('|',Sales.DBName,Sales.EntityName,Sales.LinkDate))\
                        .withColumn('LinkSalesPersonKey',concat_ws('|',Sales.DBName,Sales.EntityName,Sales.LinkSalesPerson))\
                        .withColumn('LinkItemKey',concat_ws('|',Sales.DBName,Sales.EntityName,Sales.LinkItem))\
                        .withColumn('LinkLocationKey',concat_ws('|',Sales.DBName,Sales.EntityName,Sales.LinkLocation))
    
    Sales = Sales.withColumn('BookingDate',Sales["LinkDate"]).withColumn('ServiceCost',lit(0))
    
    SalesInv = udf.UNALL(Sales,SalesInvoice)
    
    ''' EXPLICIT
        MAPPING (TeamComputers Only)'''
    GLEntry = SalesInv
    
    GLEntry = GLEntry.withColumn("Link_SUBBU",when((GLEntry["Link_SUBBU"]>=400) & (GLEntry["Link_SUBBU"]<=499)
                                                   , when(GLEntry["Link_SBU"]==1601, lit(415))\
                                                     .when(GLEntry["Link_SBU"]==1602, lit(425))\
                                                     .when(GLEntry["Link_SBU"]==1603, lit(420))\
                                                     .when(GLEntry["Link_SBU"]==1604, lit(445))\
                                                     .when(GLEntry["Link_SBU"]==1605, lit(430))\
                                                     .when(GLEntry["Link_SBU"]==1607, lit(495))\
                                                     .when(GLEntry["Link_SBU"]==1609, lit(486))\
                                                     .when(GLEntry["Link_SBU"]==1611, lit(487))\
                                                     .when(GLEntry["Link_SBU"]==1612, lit(488))\
                                                     .when(GLEntry["Link_SBU"]==1613, lit(489))\
                                                     .when(GLEntry["Link_SBU"]==1610, lit(435))\
                                                     .when(GLEntry["Link_SBU"]==1615, lit(440))\
                                                     .when(GLEntry["Link_SBU"]==1620, lit(490))\
                                                     .when(GLEntry["Link_SBU"]==1625, lit(485))\
                                                     .when(GLEntry["Link_SBU"].isin([4551,4560,4670,4680,4799]), lit(455))\
                                                     .when(GLEntry["Link_SBU"].isin([1630]), lit(405))\
                                                     .when((GLEntry["Link_SBU"]>=4250) & (GLEntry["Link_SBU"]<=4499), lit(465))\
                                                     .when((GLEntry["Link_SBU"]>=8000) & (GLEntry["Link_SBU"]<=8100), lit(475))\
                                                     .otherwise(lit(490)))\
                                                .when((GLEntry["Link_SUBBU"]>=500) & (GLEntry["Link_SUBBU"]<=599),lit(500))\
                                                .when((GLEntry["Link_SUBBU"]>=10) & (GLEntry["Link_SUBBU"]<=99)
                                                      , when(GLEntry["Link_TARGETPROD"]==70, lit(20))\
                                                      .when(GLEntry["Link_TARGETPROD"].isin([90,95]), lit(30))\
                                                      .when(GLEntry["Link_TARGETPROD"]==100, lit(25))\
                                                      .when(GLEntry["Link_TARGETPROD"]==140, lit(35))\
                                                      .when(GLEntry["Link_TARGETPROD"]==30, lit(40))\
                                                      .when(GLEntry["Link_TARGETPROD"]==25, lit(45))\
                                                      .when(GLEntry["Link_TARGETPROD"]==40, lit(50))\
                                                      .when(GLEntry["Link_TARGETPROD"].isin([170]), lit(55))\
                                                      .when(GLEntry["Link_TARGETPROD"]==80, lit(60))\
                                                      .when(GLEntry["Link_TARGETPROD"].isin([15,20,35,45,50,55,60,75,98,110,120,130,141,145,146,175]), lit(65))\
                                                      .otherwise(lit(96)))\
                                                .when((GLEntry["Link_SUBBU"]>=101) & (GLEntry["Link_SUBBU"]<=200)
                                                      , when(GLEntry["Link_TARGETPROD"].isin([15,60,98,110,120,130,141,145]), lit(110))\
                                                      .when(GLEntry["Link_TARGETPROD"].isin([10,35,45]), lit(105))\
                                                      .when(GLEntry["Link_TARGETPROD"].isin([20,135]), lit(115))\
                                                      .when((GLEntry["Link_SBU"]>=4551) & (GLEntry["Link_SBU"]<=4799), lit(130))\
                                                      .otherwise(lit(120)))\
                                                .when((GLEntry["Link_SUBBU"]>=300) & (GLEntry["Link_SUBBU"]<=399)
                                                    , when(GLEntry["Link_SBU"]==710, GLEntry["Link_SUBBU"])\
                                                    .when(GLEntry["Link_SBU"]==520, lit(315))\
                                                    .when(GLEntry["Link_SBU"]==530, lit(320))\
                                                    .when(GLEntry["Link_SBU"]==545, lit(325))\
                                                    .when(GLEntry["Link_SBU"]==550, lit(330))\
                                                    .when(GLEntry["Link_SBU"]==555, lit(335))\
                                                    .when(GLEntry["Link_SBU"]==560, lit(340))\
                                                    .when(GLEntry["Link_SBU"]==510, lit(345))\
                                                    .when(GLEntry["Link_SBU"]==630, lit(350))\
                                                    .when(GLEntry["Link_SBU"].isin([601,610,615,625,635,640,645,650]), lit(350))\
                                                    .when(GLEntry["Link_SBU"]==565, lit(360))\
                                                    .otherwise(lit(305)))\
                                                .otherwise(GLEntry["Link_SUBBU"]))
    GLEntry = GLEntry.withColumn('Link_SUBBU',when(GLEntry['Link_SUBBU']==310, lit(305)).otherwise(GLEntry["Link_SUBBU"])) #EXTRA ADDED ON 12 APR 2020
    GLEntry = GLEntry.withColumn("Link_SUBBUKey",concat_ws('|',GLEntry["DBName"],GLEntry["EntityName"],GLEntry["Link_SUBBU"]))
    SalesInv = GLEntry
    
    SalesInv.cache()
    
    SalesInv.write.jdbc(url=Postgresurl, table=DBET+".revenue_Linux", mode=owmode, properties=Postgresprop)
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    try:
            IDEorBatch = sys.argv[1]
    except Exception as e :
            IDEorBatch = "IDLE"
    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SalesCreditMemo','DB':DB,'EN':DB,'Status':'Completed',\
                        'Log_Status':'Completed','ErrorLineNo.':'NA','Rows':SalesInv.count(),'Columns':len(SalesInv.columns),'Source':IDEorBatch}]
    
    log_df = spark.createDataFrame(log_dict,schema_log)
    log_df.write.mode(apmode).save(hdfspath+"/Logs")
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    end_time = datetime.datetime.now()
    endtime = end_time.strftime('%H:%M:%S')
    etime = str(end_time-start_time)
    etime = etime.split('.')[0]
    try:
            IDEorBatch = sys.argv[1]
    except Exception as e :
            IDEorBatch = "IDLE"

    log_dict = [{'Date':Datelog,'Start_Time':stime,'End_Time':endtime,'Run_Time':etime,'File_Name':'SalesCreditMemo','DB':DB,'EN':DB,'Status':'Failed',\
                        'Log_Status':ex,'ErrorLineNo.':str(exc_traceback.tb_lineno),'Rows':0,'Columns':0,'Source':IDEorBatch}]

    log_df = spark.createDataFrame(log_dict,schema_log)
    log_df.write.mode(apmode).save(hdfspath+"/Logs")










