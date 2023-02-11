import union as union

from pyspark.sql import SparkSession
import sys

from pyspark.sql.functions import current_date
from pyspark.sql import functions as F

from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.functions import *





if __name__ == "__main__":

    spark=SparkSession \
    .builder \
    .appName("GKCodeDataAnalyzer") \
    .getOrCreate()

    today = datetime.now()
    yesterday = datetime.now() - timedelta(1)
    today = datetime.now()
    yesterdaydate = "_" + datetime.strftime(yesterday, '%d%m%Y')
    todaydate = "_" + datetime.strftime(today, '%d%m%Y')

    Yesterdaysource="D:\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\Data\Inputs\Sales_Landing\SalesDump"+yesterdaydate
    todaysource= "D:\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\Data\Inputs\Sales_Landing\SalesDump"+todaydate
    landingFileSchema=StructType([
         StructField("sale_ID",StringType(),True),
         StructField("product_ID",StringType(),True),
         StructField("Quantity_Sold",IntegerType(),True),
         StructField("Vendor_ID", StringType(), True),
         StructField("Sale_Date", TimestampType(), True),
         StructField("Sale_Amount", DoubleType(), True),
         StructField("Sale_Currency",StringType(),True)])
    landingFile_df = spark.read\
        .format("csv")\
        .schema(landingFileSchema)\
        .option("delimiter",'|')\
        .load(Yesterdaysource)
    TodaylandingFile_df = spark.read \
        .format("csv") \
        .schema(landingFileSchema) \
        .option("delimiter", '|') \
        .load(todaysource)

    outputLocation="D:\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\GKCodelabs-BigData-Batch-Processing-Course-Spark-Scala\Data\Outputs"
    landingFile_valid=landingFile_df.filter(F.col("Quantity_Sold").isNotNull() & F.col("Vendor_ID").isNotNull())
    landingFile_invalid = landingFile_df.filter(F.col("Quantity_Sold").isNull() | F.col("Vendor_ID").isNull())
    landingFile_valid.write\
        .mode(saveMode="overwrite")\
        .option("delimiter",'|')\
        .option("header",True)\
        .csv(outputLocation+"/Valid1/ValidData"+yesterdaydate)

    landingFile_invalid.write \
        .mode(saveMode="overwrite") \
        .option("delimiter", '|') \
        .option("header", True) \
        .csv(outputLocation + "/Hold1/HoldData" + yesterdaydate)

    # Reading previous day hold data
    previousdayHold=spark.read\
        .format("csv")\
        .schema(landingFileSchema)\
        .option("delimiter",'|') \
        .option("header", True) \
        .csv(outputLocation+"/Hold1/HoldData" +yesterdaydate)
    previousdayHold.createOrReplaceTempView("previousdayHold")
    TodaylandingFile_df.createOrReplaceTempView("TodaylandingFile_df")


    refreshedLandingData = spark.sql("select a.Sale_ID, a.Product_ID, " +
                                     "CASE " +
                                     "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold " +
                                     "ELSE a.Quantity_Sold " +
                                     "END AS Quantity_Sold, " +
                                     "CASE " +
                                     "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID " +
                                     "ELSE a.Vendor_ID " +
                                     "END AS Vendor_ID, " +
                                     "a.Sale_Date, a.Sale_Amount, a.Sale_Currency " +
                                     "from TodaylandingFile_df a left outer join previousdayHold b on a.Sale_ID = b.Sale_ID ")
    refreshedLandingData.createOrReplaceTempView("refreshedLandingData")
    RefreshedlandingFile_valid = refreshedLandingData.filter(F.col("Quantity_Sold").isNotNull() & F.col("Vendor_ID").isNotNull())


    releasedFromHold = spark.sql("select vd.Sale_ID " +
                                 "from refreshedLandingData vd INNER JOIN previousdayHold phd " +
                                 "ON vd.Sale_ID = phd.Sale_ID ")
    releasedFromHold.createOrReplaceTempView("releasedFromHold")


    notReleasedFromHold = spark.sql("select * from previousdayHold " +
                                    "where Sale_ID NOT IN (select Sale_ID from releasedFromHold)")
    notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")

    def func (Quantity_Sold,Vendor_ID):
        if Quantity_Sold == None:
            return "Qty Sold Missing"
        if Vendor_ID == None:
            return "Vendor ID Missin"



    inValidLandingData = refreshedLandingData.filter(F.col("Quantity_Sold").isNull
                                                     |  F.col("Vendor_ID").isNull)
    .withColumn("Hold_Reason", when(F.col("Quantity_Sold").isNull, "Qty Sold Missing")
                .otherwise(when(F.col("Vendor_ID").isNull, "Vendor ID Missing")))
    .union(notReleasedFromHold)







