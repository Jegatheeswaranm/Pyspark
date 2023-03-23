import sys
def main():
   # VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW , WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
   print("*************** Data Munging -> Validation, Cleansing, Scrubbing, De Duplication and Replacement of Data to make it in a usable format *********************")
   print("*************** Data Enrichment -> Rename, Add, Concat, Casting of Fields *********************")
   print("*************** Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")
   print("*************** Data Processing, Analysis & Summarization -> filter, transformation, Grouping, Aggregation *********************")
   print("*************** Data Wrangling -> Lookup, Join, Enrichment *********************")
   print("*************** Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")
   from pyspark.sql import SparkSession
   jdbc_lib = "file:///usr/local/hive/lib/mysql-connector-java.jar"
   print("driver for DB connectivity")
   print(jdbc_lib)
   # define spark configuration object
   spark = SparkSession.builder\
      .master("local[1]").appName("Very Important SQL End to End App 1").config("spark.jars", jdbc_lib).enableHiveSupport()\
      .getOrCreate()
   #spark.conf.set("hive.metastore.uris","thrift://127.0.0.1:9083")
   # Set the logger level to error
   spark.sparkContext.setLogLevel("ERROR")

   print("UseCase1 : Understanding Creation of DF from a Schema RDD")

   # 1.Create dataframe using toDF and createdataframe functions (Reflection)
   print("1. Create dataframe using Reflection")
   # step 1: create rdd
   filerdd = spark.sparkContext.textFile("file:/home/hduser/hive/data/custs")
   # step 2: create Row object based on the structure of the data
   from pyspark.sql import Row
   row_rdd1 = filerdd.map(lambda x: x.split(",")).filter(lambda x: len(x) == 5).map(lambda x: Row(id=int(x[0]), fname=x[1], lname=x[2], age=int(x[3]), profession=x[4]))
   rddreject1 = filerdd.map(lambda x: x.split(",")).filter(lambda x: len(x) != 5)
   print("rejected data count {}".format(rddreject1.count()))

   print("Creating Dataframe")
   filedf = row_rdd1.toDF();
   rowrdd = filedf.rdd
   filedf1 = spark.createDataFrame(row_rdd1)
   filedf.printSchema()
   filedf.show(5,truncate=False,vertical=True)
   print("DSL Queries on DF")
   filedf.select("*").filter("profession='Pilot' and age>35").show(10)
   #or
   filedf.filter("profession='Pilot' and age>35").show(10)
   filedf.select("*").where("profession='Pilot' and age>35").show(10) #where and filter are same

   print("Registering Dataframe as a temp view and writing sql queries")
   #It can be accessed in other spark sessions but not on the other spark application right ? In that case there can be multiple sessions for single application?

   filedf.createOrReplaceTempView("datasettempview")
   #filedf.createGlobalTempView("globaldatasettempview")

   print("number of partitions in the session1 = "+str(filerdd.getNumPartitions()))
#   spark.stop()
#   spark=SparkSession.builder.master("local[2]").getOrCreate()
#   filerdd = spark.sparkContext.textFile("file:/home/hduser/hive/data/custs")
#   print("number of partitions in the session1 = " + str(filerdd.getNumPartitions()))
   #spark.sql("select * from globaldatasettempview where profession='Pilot' and age>35").show(5, truncate=False)

   print("2. Reading from a paths contains multiple pattern of files")
   dfmultiple = spark.read.option("header", "True").option("inferSchema", True).csv(
      "file:///home/hduser/sparkdir*/*.csv", sep="~")
   print("Reading from a multiple different paths contains multiple pattern of files")
   dfmultiple1 = spark.read.option("header", "True").option("inferSchema", True).option("delimiter", "~")\
      .csv(["file:///home/hduser/sparkdir1/*.csv", "file:///home/hduser/raavandir2/*.csv"])
   print(" Four files with 500 lines with each 1 header, after romoved 499*4=1996 ")
   print(dfmultiple.count())
   print(dfmultiple1.count())

   print("3.create dataframe directly using multiple methodologies like toDF(), schema etc.,")
   '''
   cd ~
   mkdir sparkdir1
   mkdir sparkdir2
   cp ~/sparkdata/usdata.csv ~/sparkdir1/
   cp ~/sparkdata/usdata.csv ~/sparkdir1/usdata1.csv
   cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata2.csv
   cp ~/sparkdata/usdata.csv ~/sparkdir2/usdata3.csv
   wc -l ~/sparkdir1/usdata1.csv
   mkdir raavandir2
   cp sparkdir2/* raavandir2/ 
   cd ~
   mkdir useast
   mkdir ussouth
   cp ~/sparkdata/usdata.csv ~/useast/usdata_ny.csv
   cp ~/sparkdata/usdata.csv ~/useast/usdata_nj.csv
   cp ~/sparkdata/usdata.csv ~/south/usdata_ca.csv
   cp ~/sparkdata/usdata.csv ~/south/usdata_mi.csv
   wc -l ~/sparkdir1/usdata1.csv     
   '''

   print("3.A. least performant one - Create dataframe inferring schema dynamically and defining column names manually")
   dfcsv1 = spark.read.format("csv").option("inferSchema", True).load("file:/home/hduser/hive/data/custsmodified")\
      .toDF("cid", "fname", "lname", "ag", "prof")

   dfcsv1.show(10)
   dfcsv1.printSchema()
   print("3.B. best performant one - create dataframe using the custom defined schema (without schema inference) from collections type such as structtype and fields")
   uschema = StructType([
      StructField("first_name", StringType(), True),
      StructField("last_name", StringType(), False),
      StructField("company_name", StringType(), True),
      StructField("address", StringType(), True),
      StructField("city", StringType(), True),
      StructField("country", StringType(), True),
      StructField("state", StringType(), True),
      StructField("zip", StringType(), True),
      StructField("age", IntegerType(), True),
      StructField("phone1", StringType(), True),
      StructField("phone2", StringType(), True),
      StructField("email", StringType(), True),
      StructField("website", StringType(), True)])
   udf1 = spark.read.csv("file:/home/hduser/sparkdata/usdata.csv", header="True", schema=uschema)
   #or
   udf1 = spark.read.option("header", True).schema(uschema).csv("file:/home/hduser/sparkdata/usdata.csv")
   udf1.printSchema()
   print("Writing a sample DSL on the above udf1 dataframe using select expr or using withColumn")

   #mix and match both DSL and SQL in a single statement select(col("*")) is a DSL where as expr("sql syntax") is a SQL
   udf1.select(col("*"), expr("case when age <= 10 then 'childrens' when age > 10 and age < 20 then 'teen' else 'others' end agecat"))\
      .distinct().orderBy(col("agecat").desc()).show()
   udf1.selectExpr("case when age <= 10 then 'childrens' when age > 10 and age < 20 then 'teen' else 'others' end as agecat").show()

   #below is a pure DSL (not much familiar)
   udf1.withColumn("agecat",when(col("age") <= 10, "childrens").when((col("age") > 10) & (col("age") < 20), "teen").otherwise("others")).show()

   # or
   print("3.C. Converting the DF to Tempview to write equivalent pure SQL Queries")
   udf1.createOrReplaceTempView("custinfo")
   agecat = spark.sql("""select a.*,
   case when age <=10 then 'childrens' 
   when age >10 and age < 20 then  'teen' 
   else 'others' end as agecat 
   from custinfo a """)
   agecat.show(2,truncate=False,vertical=False)

   #E2E data management (Source systems -> pushed/pulled -> Ingestion Team -> Raw zone (DataLake) -> Data Engg Team-> Munging -> Curated zone (Hive/Nosql/Cloud stores)
   # -> DataEngg Team/DataScience Team (Munging/Wrangling) -> UI Team -> Visualization/Dashboard (democritize data)-> Analytics (Business team))
   #Data wrangling, sometimes referred to as data munging, is the process of transforming and mapping data from one "raw" data form into another format with the intent of making it more appropriate and valuable for a variety of downstream purposes such as analytics
   print("4. IMPORTANT - STARTING OF DATA ENGINEERING USE CASES - Munging (usable data preparation for DE & DA) & Wrangling (usable data preparation for DA & DS)")
   '''   
   cp /home/hduser/hive/data/custs /home/hduser/hive/data/custsmodified
   vi /home/hduser/hive/data/custsmodified
   4000000,Apache,Spark,11,
   4000001,Kristina,Chung,55,Pilot
   4000001,Kristina,Chung,55,Pilot
   4000002,Paige,Chen,77,Actor
   4000003,Sherri,Melton,34,Reporter
   4000004,Gretchen,Hill,66,Musician
   ,Karen,Puckett,74,Lawyer
   echo "trailer_data:end of file" >> /home/hduser/hive/data/custsmodified
   wc -l /home/hduser/hive/data/custsmodified
   10002 '''
   print("4.A : Header & Footer management using data ingestion modes for initial cleansing of data")
   print("Lets learn how to apply Munging by CREATING DF'S USING MODE(PERMISSIVE, DROPMALFORMED, FAILFAST) WITH INFERSCHEMA & CUSTOM SCHEMA")
   dfcsvwithfooter1 = spark.read.option("mode", "permissive").option("inferschema", "true").csv("file:/home/hduser/hive/data/custsmodified")
   print("malformed data is permitted, try the failure case also!!!")
   #dfcsvwithfooter1 = spark.read.option("mode", "failfast").option("inferschema", "true").csv("file:/home/hduser/hive/data/custsmodified")
   dfcsvwithfooter1.filter("_c0 like 'trailer%'").show()
   footercnt = dfcsvwithfooter1.filter("_c0 like 'trailer%'").count()
   if (footercnt == 1):
    print("first permit all data to ensure footer is there, once footer is identified remove it using dropmalformed")
    # after DF creation, remove the footer using filter (conditional)
    dfcsvwithoutfooter = dfcsvwithfooter1.filter("_c0 not like 'trailer%'")
    #or
    #at the time of DF creation, remove the footer (un conditionally works without huge awareness on the data)
    dfcsvwithoutfooter1 = spark.read.option("mode", "dropmalformed").option("inferschema", "true").csv("file:/home/hduser/hive/data/custsmodified")
    print("malformed data is dropped")
    dfcsvwithoutfooter1.filter("_c0 like 'trailer%'").show()
   else:
    print(" Data seems to be having issue when it was generated by the source system itself, because trailer is missing")
    sys.exit(1)

   #Example syntax for structure struct = StructType([(StructField(colname,dtype,nullable))])
   custstructtype1 = StructType([StructField("id", IntegerType(), False), StructField("custfname", StringType(), False)
                                      , StructField("custlname", StringType(), True)
                                      , StructField("custage", ShortType(), True),
                                      StructField("custprofession", StringType(), True)]);

   dfcsvstruct1 = spark.read.format("csv").option("mode", "permissive").schema(custstructtype1)\
      .load("file:/home/hduser/hive/data/custsmodified")
#  Good in performance due to it does not check all the records to define schema as like inferschema, also the custom data type can be defined
   dfcsvstruct1.filter("id is null").show()

   dfcsvstruct1_permissive = spark.read.format("csv").option("mode", "permissive").schema(custstructtype1)\
      .load("file:/home/hduser/hive/data/custsmodified_malformed")

   '''4000004, Gretchen, Hill, 66, Computer hardware engineer
   , Karen, Puckett, 74, Lawyer
   4000006, Patrick, Song, 42, Veterinarian, additional column // more fields are dropped  
   4000007, Elsie, Hamilton, 43 // less fields are dropped
   fourtylakhsseven, mohd, irfan, 39, IT // string in the place of int dropped
   '''
   print(" Final data After allowing the trailer record, with less/more field rows of the original file ")
   dfcsvstruct1_permissive.where("id is null or id in (4000004,4000005,4000006,4000007,4000008)").show()
   # dfcsvstruct1.show(100001)

   '''/ *Permissive + -------+---------+---------+-------+--------------------+
   +-------+---------+-----------------+-------+--------------------+
   |     id|custfname|        custlname|custage|      custprofession|
   +-------+---------+-----------------+-------+--------------------+
   |4000004| Gretchen|             Hill|     66|Computer hardware...|
   |   null|    Karen|          Puckett|     74|              Lawyer|
   |4000006|  Patrick|             more cols|     42|        Veterinarian|
   |4000007|    Elsie|     less columns|     43|                null|
   |   null|     mohd|string in integer|     39|                  IT|
   |   null|     null|             null|   null|                null|
   +-------+---------+-----------------+-------+--------------------+
   '''
   dfcsvstruct1_malformed = spark.read.format("csv").option("mode","dropmalformed").schema(custstructtype1) \
      .load("file:/home/hduser/hive/data/custsmodified_malformed")
   print(" Final data After removing the trailer record, malformed less/more field rows of the original file ")
   dfcsvstruct1_malformed.where("id is null or id in (4000004,4000005,4000006,4000007,4000008)").show()
   # dfcsvstruct1.show(100001)

   print("display the dropped data")
   dfcsvstruct1_permissive.subtract(dfcsvstruct1_malformed).show()
   ''' *Drop malformed (Munged Data) +-------+---------+---------+-------+--------------------+
   | id | custfname | custlname | custage | custprofession |
   +-------+---------+---------+-------+--------------------+
   | 4000004 | Gretchen | Hill | 66 | Computer
   hardware... |
   | null | Karen | Puckett | 74 | Lawyer |
   | 4000008 | Hazel | Bender | 63 | Carpenter |
   +-------+---------+---------+-------+--------------------+
   '''

   # Option 2 using inferschema
   dfcsv1 = spark.read.format("csv").option("mode", "permissive").option("inferSchema", True)\
      .load("file:/home/hduser/hive/data/custsmodified_malformed")
      #.toDF("cid", "fname", "lname", "ag", "prof")
   '''   not a good way to do, because inferschema evaluate all rows to apply schema, but we can't define custom schema here inorder to read all data... 
   if i  need column name explicitly, let's use toDF(colnames)
   '''
   print(" Final data After allowing the trailer record, malformed less/more field rows of the original file ")
   dfcsv1.where("_c0 is null or _c0='fourtylakhsseven' or _c0 in (4000004,4000005,4000006,4000007,4000008)").show()
   print("display the raw values of the dropped data")
   dfcsv1.subtract(dfcsvstruct1_malformed).show()

   '''/ *+----------------+--------+-------+-----+--------------------+
   | _c0 | _c1 | _c2 | _c3 | _c4 |
   +----------------+--------+-------+-----+--------------------+
   | 4000004 | Gretchen | Hill | 66 | Computer
   hardware... |
   | null | Karen | Puckett | 74 | Lawyer |
   | fourtylakhsseven | mohd | irfan | 39 | IT |
   | 4000007 | Hazel | Bender | sixty | Carpenter |
   | 4000008 | Hazel | Bender | 63 | Carpenter |
   +----------------+--------+-------+-----+--------------------+
   '''
   print("Do some basic performance improvements, we will do more later in a seperate discussion ")
   print("Number of partitions in the given DF {}".format(dfcsv1.rdd.getNumPartitions()))
   dfcsv = dfcsv1.repartition(4)
   # after the shuffle happens as a part of wide transformation(groupby, join) the partition will be increased to 200
   dfcsv2 = dfcsv.coalesce(1)
   dfcsv.cache()
   # default parttions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
   spark.conf.set("spark.sql.shuffle.partitions", "4")
   #select (10 partitions) -> shuffling (copy data from mapper container to the groupBy container)->
   # in spark sql after shuffle completed the number of partitions will become 200 by default-> groupBy("city").agg(sum("amt"))
   print("Initial count of the dataframe ")
   print(dfcsv.count())

   print("MAJOR PART OF THIS PROGRAM - ETL PIPELINE")
   dfcsv1 = spark.read.format("csv").option("mode", "permissive").option("inferSchema", True)\
      .load("file:/home/hduser/hive/data/custsmodified_malformed")
      #.toDF("cid", "fname", "lname", "ag", "prof")
   # Transformations / Curation/ processing / ETL / ELT / EDA / SCD / CDC / Wrangling / Munging /conversion /migration ....
   print("""*************** 1. Data Munging (preparing TIDY data from the RAW data for DE team to curate further)-> 
   Preprocessing, Preparation, Validation, Cleansing (removal of unwanted datasets), Scrubbing(convert of raw to tidy), 
   De Duplication and Replacement of fields to convert Data in a usable format *********************""")

   #MUNGING comprise of all these functionalities applied on the raw data ->
   # dropmalformed, drop null (all,any,subset), fill null with something, replace something with somethingelse, drop duplicates, distinct, case
   # Writing DSL
   dfcsv.printSchema()
   print("Dropping the null records with all of the columns is null ")
   dfcsva = dfcsv.na.drop("all") # good to use DSL rather than using SQL
   print("Dropping the null records with any one of the column is null ")
   #dfcsva = dfcsva.na.drop("any") # good to use DSL rather than using SQL
   #dfcsv.where("_c4 is null").na.drop("any").show()
   #dfcsv.na.drop("all").where("_c0 is null").show()
   print("Dropping the null records with custid is null ")
   dfcsva = dfcsva.na.drop("any",subset=["_c0"])
   print(dfcsva.count())
   #set threshold as a second argument to drop the records having a minimum threshold of 2 rows with any one the column contain null
   dfcsva = dfcsva.na.drop("any",2,subset=["_c0","_c1"])  # good to use DSL rather than using SQL
   print(dfcsva.count())
   #Parameter:
   #how: This parameter is used to determine if the row or column has to remove or not.
   #‘any’ – If any of the column value in Dataframe is NULL then drop that row.
   #‘all’ – If all the values of particular row or columns is NULL then drop.
   #thresh: If non NULL values of particular row or column is less than thresh value then drop that row or column.
   #subset: If the given subset column contains any of the null value then dop that row or column.

   print("If need to write the na.drop functionalities in SQL, is it easy or hard? ")
   dfcsv.createOrReplaceTempView("tmpview")
   #below is equivalent to dfcsv.na.drop("any")
   dfcsva_sql = spark.sql("select * from tmpview "
                          "where _c0 is not null and _c1 is not null and _c2 is not null and _c3 is not null and _c4 is not null")
   # not good to use SQL as it is costly to write in the above usecase
   # SQL or DSL- DSL wins in terms of simplicity and the features of using it

   print("Customers didn't mention their profession {}".format(dfcsv.where("_c4 is null").count()))
   print("fill the null with default values ")
   dfcsvb = dfcsva.na.fill("UNKNOWN", subset=["_c4"])
   #dfcsvb=dfcsva.fillna("UNKNOWN",subset=["_c4"])
   dfcsvb.where("_c4 ='UNKNOWN'").show()
   print("Replace the key with the respective values in the columns (another way of writing Case statement)")
   dict1 = {"Actor":"Stars", "Reporter":"Media person", "Musician":"Instrumentist"}
   dfcsvb.filter("_c4 in ('Actor','Reporter')").count()
   dfcsvc = dfcsvb.na.replace(dict1,subset=["_c4"])
   dfcsvc.filter("_c4 in ('Stars','Media person')").count()

   print("Dropping Duplicate records ")
   dfcsvd = dfcsvc.distinct() #.dropDuplicates()
   dfcsvd=dfcsvc.dropDuplicates()
   print("De Duplicated count")
   print(dfcsvd.count())
   dfcsvd.orderBy(asc("_c0")).show(10)
   dfcsvd.orderBy("_c0",ascending=True).show(10)
   dfcsvd.orderBy(col("_c0").asc()).show(10)
   #len("irfan").__len__()
   #Writing equivalent SQL
   dfcsv.createOrReplaceTempView("dfcsvview")
   dfcsvcleanse = spark.sql("""select distinct _c0,_c1,_c2,_c3,case when _c4 is null then "UNKNOWN" 
                                  when _c4="Actor" then "Stars" when _c4="Reporter" then "Media person" 
                                  when _c4="Musician" then "Instrumentist"
                                  else _c4 end as _c4 
                                  from  dfcsvview 
                                  where _c0 is not null
                                  order by _c0 asc""")

   print(dfcsvcleanse.count())
   dfcsvcleanse.sort("_c0").show(10)

   print("Current Schema of the DF")
   dfcsvd.printSchema()
   print("Current Schema of the tempview DF")
   dfcsvcleanse.printSchema()

   print("*************** 2. Data Curation/Transformation -> Enrichment -> Rename, Derive/Add, Remove, Merging/Split, Casting of Fields *********************")
   # 4005741, Wayne, Marsh, 23, Pilot
   print("Passing munged data to Enrichment stage")
   # rename the column
   # Deriving a boolean value from another value .contains("Pilot")
   # adding new column with a literal of CustInfo
   # Merging of multiple columns into one
   # Conversion of type from int to string on the age column
   # Dropping on unwanted columns or the columns that are addressed already
   # Generating sequence number in a new column called surrogatekey

   # Only using pure DSL
   dfcsvd1=dfcsvd\
      .withColumnRenamed("_c0", "custid")\
      .withColumn("IsAPilot", col("_c4").contains("Pilot"))\
      .withColumnRenamed("_c4","profession")\
      .withColumn("Typeofdata", lit("CustInfo"))\
      .withColumn("fullname", concat("_c1",lit(" "),"_c2"))\
      .withColumn("age",col("_c3").cast("String"))\
      .drop("_c1", "_c2", "_c3","_c4")\
      .withColumn("surrogatekey", monotonically_increasing_id())
   dfcsvd1.printSchema()
   dfcsvd1.sort("custid").show(5, False)

   #DSL + SQL
   dfcsvdexpr1=dfcsvd.selectExpr("_c0 as custid", "_c4 as profession", "case when _c4 like ('%Pilot%') then true else false end as IsAPilot",
   "'CustInfo' as Typeofdata", """concat(_c1," ",_c2) as fullname""", "cast(_c3 as string) as age","row_number() over(order by _c0) as surrogatekey")

   dfcsvdexpr1.printSchema()
   dfcsvdexpr1.show(5, False)
   dfcsvd.createOrReplaceTempView("customer")
   print("Converting DSL Equivalent SQL queries")

   print("SQL EQUIVALENT")

   spark.sql(""" select _c0 as custid,_c4 as profession, case when _c4 like ('%Pilot%') then true else false end as IsAPilot,
                         'CustInfo' as Typeofdata,concat(_c1," ",_c2) as fullname,
                          cast(_c3 as string) as age,row_number() over(order by _c0) as surrogatekey
                          from customer """).sort("custid").show(5, False)


   print("Another way of define/renaming column names for more number of columns to just renamed")
   #define or change column names and datatype toDF(), structtype(Array(structfields)) -> schema(), sql as,
   # col names -> toDF(all columns), withcolumnrenamed, option("header", true)
   dfcsvd1renamed = dfcsvd1.toDF("custid", "profession", "IsAPilot", "TypeofData", "fullname", "age","key")
   print("Interiem dataframe with proper names to understand further steps easily ")
   #dfcsvdexpr1.printSchema()
   #dfcsvd1renamed.printSchema

   print("*************** 3. Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")
   print("Applying UDF in DSL")
   print("create an object reusableobj to Instantiate the cleanup class present in org.raavan.spark.sql.reusable pkg")
   # how to make use of  an existing python method in a DSL -> once object is created
   # convert the method into udf -> apply the udf to your df  columns
   from sparkapp.pyspark.utils.common_functions import cls_common_udf
   ageobj=cls_common_udf()
   from pyspark.sql.functions import udf
   # Convert the normal python function into Spark DF DSL supportive UDF
   udf_agerange = udf(ageobj.agerange) #this will be converting the normal func to udf
   print("Convert the python function to UDF to use in Dataframe")
   # More than 1 argument
   dfudfapplied1 = dfcsvd1renamed.fillna({'age':'0'})\
      .select("custid","profession","IsAPilot","TypeofData","fullname",col("age").cast("int").alias("age"),"key")\
      .withColumn("agerange", udf_agerange(col("age").cast("int")))
   dfudfapplied1.printSchema()
   dfudfapplied1.show(5, False)

   print("Applying UDF in SQL")
   print("register the python function to UDF to use in SQL")
   spark.udf.register("sqludf",ageobj.agerange)
   #used in SQL, this will be converting the normal func to udf and it will add this function into the metastore

   dfcsvd1renamed.createOrReplaceTempView("custinfo")
   dfsqltransformed=spark.sql("""select custid,profession,isapilot,typeofdata,fullname,
   cast(age as int) as age,sqludf(age) as agerange 
   from custinfo""")

   print("*************** 4. Data Processing, Analytics (EDA - Exploratory Data Analytics) & Summarization -> "
         "transformation (sampling, filter, Grouping, Aggregation, de duplication, ranking, EDA) *********************")
   dfgrouping = dfudfapplied1.filter("age>25").groupBy("profession")\
      .agg(max("age").alias("maxage"), min("age").alias("minage"),count("age").alias("prof_cnt"),countDistinct("age").alias("prof_distinct_cnt"))
   dfgrouping.show(5, False)

   print("Converting DSL Equivalent SQL queries")
   dfudfapplied1.createOrReplaceTempView("customer")
   dfsqltransformedaggr = spark.sql("""select profession,max(age) as maxage,min(age) as minage ,count(age) as prof_cnt,count(distinct age) as prof_distinct_cnt
              from customer 
              where age>5
              group by profession""")
   dfsqltransformedaggr.show(5, False)
   print("Analytical - EDA - Frequent Items of age and profession")
   dfudfapplied1.freqItems(["age","profession"]).show(5, False)
   print("Sample DF")
   dfudfapplied1.sample(.1,20).show()
   print("EDA correlation of age" + str(dfudfapplied1.corr("age","age")))

   print("EDA - Summary of age")
   dfudfapplied1.select("age").summary().show()
   print("""*************** 5. Data Wrangling (preparing the data for DS/DA/BU/Consumers to generate report,strategic initiatives, analysis)
                         -> Lookup, Join, Enrichment,windowing *********************""")

   txns = spark.read.option("inferschema", True)\
   .option("header", False).option("delimiter", ",")\
   .csv("file:///home/hduser/hive/data/txns")\
   .toDF("txnid", "dt", "custid", "amt", "category", "product", "city", "state", "transtype")

   print("Data Enrichment1")
   txns = txns.withColumn("dt", to_date("dt", "MM-dd-yyyy")).select("*",year("dt").alias("year"),month("dt").alias("month")
         ,last_day("dt").alias("last_day"),add_months("dt",1).alias("one_month_added"),date_add("dt",2).alias("two_days_added")
         ,dayofweek("dt").alias("day_of_the_week"))

   txns.show(10);
   #Transaction data of last 1 month I am considering above
   print("Lookup and Enrichment Scenario using DSL")
   #df1leftjoined = dfudfapplied1.alias("a").join(txns.alias("b"), dfudfapplied1("custid") == txns("custid"), "left").select("a.custid").show()
   df1leftjoined = dfudfapplied1.alias("a").join(txns.alias("b"), "custid","left").select("a.custid","b.amt","transtype")
   df1leftjoined.show()
   #df1leftjoinedout=df1leftjoined.selectExpr("custid,amt,transtype,case when b.transtype is null then 'old customer' else 'repeating customer' end as TransactionorNot")
   #df1leftjoinedout.show()

   print("Lookup and Enrichment Scenario using SQL")
   dfudfapplied1.createOrReplaceTempView("customertransformed")
   txns.createOrReplaceTempView("trans");

   dflookupsql = spark.sql("""select a.custid,b.amt,b.transtype,case when b.transtype is null then "old customer" 
   else "repeating customer" end as TransactionorNot
   from customertransformed a left join trans b
   on a.custid=b.custid  """)

   print("Lookup dataset")
   dflookupsql.show(10, False)

   print("Join Scenario in temp views  to provide denormalized view of data to the business")
   # DSL Way of doing join
   df1innerjoined = dfudfapplied1.alias("a").join(txns.alias("b"), "custid","inner").select("a.*","b.*")
   df1innerjoined.show(10,False)

   dfjoinsql = spark.sql("""select a.custid,a.profession,a.isapilot,a.typeofdata,
   b.amt,b.transtype,b.city,b.product,b.dt 
   from customertransformed a inner join trans b
   on a.custid=b.custid """)

   print("Enriched final dataset")
   dfjoinsql.show(10, False)

   df1innerjoined.createOrReplaceTempView("joinedview")

   print("Identify the nth recent transaction performed by the customers or 3rd highest transaction performed by the customer")
   from pyspark.sql.window import Window
   dfjoinsqlkeygen = df1innerjoined.\
      select("custid",row_number().over(Window.partitionBy("custid").orderBy(desc("dt"))).alias("nth_trans"),
             rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("Top_Trans"),
             dense_rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("Top_Equal_Trans"),
             "profession","city","product","dt")
   dfjoinsqlkeygen.show(20)
   dfjoinsqlkeygen = spark.sql("""
   select * from (select custid,row_number() over(partition by custid order by dt desc) as transorder,
   profession,city,product,dt
    from joinedview) as joineddata """)
   dfjoinsqlkeygen.show(5)

   print("Aggregation on the joined data set")
   dfjoindfagg=df1innerjoined.groupby("city","profession","transtype")\
      .agg(sum("amt").alias("sum_amt"),avg("amt").alias("avg_amt"),countDistinct("city").alias("dist_city_cnt"))\
      .where(col("sum_amt")>500)
   print("DSL Aggregated final dataset")
   dfjoindfagg.show(10)

   dfjoinsqlagg = spark.sql("""select distinct city,profession,transtype,sum(amt) sum_amt,avg(amt) avg_amt ,count(distinct city) dist_city_cnt
      from joinedview 
      group by city,profession,transtype
      having sum(amt)>500""")

   print("SQL Aggregated final dataset")
   dfjoinsqlagg.show(10, False)

   print("*************** Data Persistance -> Discovery, Outbound, Reports, Democritization, Enablement, exports, Schema migration  *********************")
   dfjoinsqlagg.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="cust_aggr",
                            mode="overwrite",properties={"driver": 'com.mysql.jdbc.Driver'})
   dfjoinsqlagg.write.mode("overwrite").saveAsTable("default.hive_aggr")
   dfjoinsqlagg.write.partitionBy("profession").mode("overwrite").saveAsTable("default.hive_aggr")
   dfjoinsqlagg.write.mode("overwrite").json("/user/hduser/aggrcustjson")
   print("Spark App1 Completed Successfully")

#Driver program
if __name__=="__main__":
   print("Spark ETL & ELT starts here")
   from pyspark.sql.types import *
   from pyspark.sql.functions import *
   main()
