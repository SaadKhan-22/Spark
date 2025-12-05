# Databricks notebook source
# DBTITLE 1,Import statements
#from pyspark import *
import pyspark.sql.functions as F
import pyspark.sql.types as T


# COMMAND ----------

spark

# COMMAND ----------

myRange = spark.range(1000).toDF("numbers")

# COMMAND ----------

myRange.show()

# COMMAND ----------

evenNum = myRange.where('numbers % 2 = 0')
evenNum.show()

# COMMAND ----------

evenNum.count()

# COMMAND ----------

flightsData = spark.read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("/Workspace/Users/sak@cebs.io/2015-summary.csv")

# COMMAND ----------

flightsData.orderBy("count", ascending = True).explain()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")

# COMMAND ----------

flightsData.orderBy("count", ascending = True).explain()

# COMMAND ----------

flightsData.createOrReplaceTempView("test_view")

# COMMAND ----------

sql_query = spark.sql("""
                      SELECT COUNT(*)
                      FROM test_view
                      GROUP BY DEST_COUNTRY_NAME
                      """)


# COMMAND ----------

sql_query.show()

# COMMAND ----------

bruh = flightsData.groupBy("DEST_COUNTRY_NAME").count()
bruh.show()

# COMMAND ----------

sql_query.explain()

# COMMAND ----------

bruh.explain()

# COMMAND ----------

most_pop_dest = spark.sql("""
                          select max(count)
                          from test_view
                          """)

# COMMAND ----------

most_pop_dest.show()

# COMMAND ----------

flightsData.select(max("count")).take(1)

# COMMAND ----------

top5sql = spark.sql("""
                    SELECT DEST_COUNTRY_NAME, sum(count)
                    FROM test_view
                    GROUP BY DEST_COUNTRY_NAME
                    ORDER BY sum(count) DESC
                    LIMIT 5
                    
                    """)

# COMMAND ----------

top5sql.show()

# COMMAND ----------

flightsData.groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "dest_total")\
    .orderBy("dest_total", ascending = False)\
    .limit(5)\
    .show()

# COMMAND ----------

staticDf = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/Workspace/Users/sak@cebs.io/by-day/*")

# COMMAND ----------

staticDf.take(5)

# COMMAND ----------

staticDf.createOrReplaceTempView("retail_data")
staticDfSchema = staticDf.schema

# COMMAND ----------

staticDfSchema

# COMMAND ----------

# DBTITLE 1,ayein
staticDf.show(5)

# COMMAND ----------

staticDf\
    .selectExpr(
        'CustomerID',
        '(UnitPrice*Quantity) as total_cost',
        'InvoiceDate')\
    .groupBy(F.col("CustomerID"), F.window(F.col("InvoiceDate"), "1 day"))\
        .sum("total_cost")\
        .show(15)

# COMMAND ----------

streamingDf = spark.readStream\
    .schema(staticDfSchema)\
    .option("maxFilesPerTrigger", 2)\
    .format("csv")\
    .option("header", "true")\
    .load("/Workspace/Users/sak@cebs.io/by-day/*")

# COMMAND ----------

# DBTITLE 1,Streaming Check
streamingDf.isStreaming

# COMMAND ----------

purchasesPerCustPerHour = streamingDf\
    .selectExpr('CustomerID',
        '(UnitPrice*Quantity) as total_cost',
        'InvoiceDate')\
    .groupBy(
    F.col("CustomerID"),
    F.window(F.col("InvoiceDate"), "1 hour"))\
    .sum("total_cost")

# COMMAND ----------

purchasesPerCustPerHour.writeStream\
    .format("console")\
    .queryName("customer_purchases")\
    .trigger(once = True)\
    .option("checkpointLocation", "/Workspace/Users/sak@cebs.io/by-day/Stream Output Sink")\
    .outputMode("complete")\
    .start()

# COMMAND ----------

spark.sql("""
          SELECT *
          FROM customer_purchases
          ORDER BY 'sum(total_cost)' DESC""")\
        .show(5)

# COMMAND ----------

staticDf.printSchema()

# COMMAND ----------

prepDf = staticDf.na.fill(0)\
    .withColumn('day_of_week', F.date_format(F.col("InvoiceDate"), 'EEEE'))\
    .coalesce(5)

# COMMAND ----------

prepDf.show(5)

# COMMAND ----------

trainDf = prepDf.where("InvoiceDate < '2010-12-03'")
testDf = prepDf.where("InvoiceDate >= '2010-12-03'")

# COMMAND ----------

trainDf.count()
#testDf.count()

# COMMAND ----------

spark.sparkContext.parallelize(Seq(1,2,3)).toDF()

# COMMAND ----------

# MAGIC %sh ls .

# COMMAND ----------

# DBTITLE 1,FlightsDf Defined
flightsDf = spark.read.format("json").load("/Workspace/Users/sak@cebs.io/2015-summary.json")
flightsDf.show(5)

# COMMAND ----------

flightsDf.schema

# COMMAND ----------

manualSchema = T.StructType([
    T.StructField("DEST_COUNTRY_NAME", T.StringType(), True),
    T.StructField("ORIGIN_COUNTRY_NAME", T.StringType(), True),
    T.StructField('Count', T.LongType(), False, metadata={"hello":"world"})
])

# COMMAND ----------


flightsDf = spark.read.format('json').schema(manualSchema).load("/Workspace/Users/sak@cebs.io/2015-summary.json")

# COMMAND ----------

flightsDf.show(5)

# COMMAND ----------

flightsDf.columns

# COMMAND ----------

flightsDf.first()

# COMMAND ----------

type(flightsDf.first()[1])

# COMMAND ----------

flightsDf.createOrReplaceTempView("flights_view")

# COMMAND ----------

flightsDf.select('ORIGIN_COUNTRY_NAME', 'Count').show(7)

# COMMAND ----------

flightsDf.selectExpr(
"*",
"(ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME) as IN_COUNTRY",
"Count > 10 as oof"
).show(5)

# COMMAND ----------



flightsDf.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").first()[0]

# COMMAND ----------

flightsDf.selectExpr(

    "*",
    "(ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME) as IN_COUNTRY",
    "Count > 10 as oof",
    "'bruh' as tis_but_a_test_literal"

).take(4)

# COMMAND ----------

# DBTITLE 1,Changing Columns using selectExpr()
#Changing Columns using selectExpr()
flightsDf.selectExpr(
    "(ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME) as IN_COUNTRY",
    "'bruh' as tis_the_same_test_literal"
)

# COMMAND ----------

# DBTITLE 1,Changing Columns using withColumn()
#Changing Columns using withColumn()
flightsDf\
    .withColumn("IN_COUNTRY", flightsDf.ORIGIN_COUNTRY_NAME==flightsDf.DEST_COUNTRY_NAME)\
    .withColumn("tis_the_same_test_literal", F.lit("bruh"))\
    .show()

# COMMAND ----------

flightsDf \
    .withColumnRenamed('ORIGIN_COUNTRY_NAME', 'renamed_this_column') \
    .where("renamed_this_column = 'Gibraltar'") \
    .show()

# COMMAND ----------

flightsDf \
    .filter(F.col('Count').isNull()) \
    .withColumn('countAsBool', F.col('Count').cast('Boolean')) \
    .show()

# COMMAND ----------

flightsDf.drop("DEST_COUNTRY_NAME").schema

# COMMAND ----------

flightsDf.withColumn('Count', F.col('Count').cast("long")).schema

# COMMAND ----------

flightsDf.select("ORIGIN_COUNTRY_NAME").distinct().orderBy("ORIGIN_COUNTRY_NAME", ascending = False).show()

# COMMAND ----------

flightsDf.sample(True, 0.05, 5).show()

# COMMAND ----------

splitDf = flightsDf.randomSplit([0.2, 0.8], 5)

# COMMAND ----------

splitDf

# COMMAND ----------

splitDf[0].count() > splitDf[1].count()

# COMMAND ----------

flightsDf.rdd.id()

# COMMAND ----------

splitDf[1].orderBy(F.col("DEST_COUNTRY_NAME").desc(), F.col("ORIGIN_COUNTRY_NAME")).show()

# COMMAND ----------

splitDf[1].sort(F.desc_nulls_last(F.col("DEST_COUNTRY_NAME"))).show()

# COMMAND ----------

splitDf[1].sortWithinPartitions("DEST_COUNTRY_NAME", ascending = False).show()

# COMMAND ----------

splitDf[1].repartition(5)

# COMMAND ----------
# Repartitions by a column into a 1000 partitions
# then coalesces into 50 partitions
splitDf[1] \
    .repartition(1000, F.col("DEST_COUNTRY_NAME")) \
    .coalesce(50) \
    .show(5, 10, False)

# COMMAND ----------

