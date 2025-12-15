# Databricks notebook source
# DBTITLE 1,Import statements
#from pyspark import *
import pyspark.sql.functions as F
import pyspark.sql.window as W
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
# show 5 records, truncate all strings with length >= 10, format each record vertically = False
splitDf[1] \
    .repartition(1000, F.col("DEST_COUNTRY_NAME")) \
    .coalesce(50) \
    .show(5, 10, False)

# COMMAND ----------

flightsDf.describe().display()

# COMMAND ----------

flightsDf \
    .select(F.initcap(F.col("DEST_COUNTRY_NAME")).alias("Each word caps"),
    F.upper(F.col("DEST_COUNTRY_NAME")).alias("All caps"),
    F.lower(F.col("DEST_COUNTRY_NAME")).alias("No caps")) \
    .show()


# COMMAND ----------

flightsDf.select(
 F.ltrim(F.lit(" HELLO ")).alias("ltrim"),
 F.rtrim(F.lit(" HELLO ")).alias("rtrim"),
 F.trim(F.lit(" HELLO ")).alias("trim"),
 F.lpad(F.lit("HELLO"), 3, " ").alias("lp"),
 F.rpad(F.lit("HELLO"), 3, " ").alias("rp")).show(2)


# COMMAND ----------

extract_str = "(Un|Se|t)"
flightsDf.select(
 F.regexp_extract(F.col("DEST_COUNTRY_NAME"), extract_str, 1).alias("MODIFIED_DEST_COUNTRY_NAME"),
 F.col("DEST_COUNTRY_NAME")).show(20)


# COMMAND ----------

bruh = ['a', 'b', 3]
iter_over_bruh = iter(bruh)

# COMMAND ----------

one, two, three = *iter_over_bruh

# COMMAND ----------

# DBTITLE 1,DateDF Defined
dateDF = spark.range(25) \
        .withColumn("today", F.current_date()) \
        .withColumn("now", F.current_timestamp())

# COMMAND ----------

dateDF \
    .select(F.date_sub(F.col("today"), 5),
            F.date_add(F.col("today"), 5)) \
.show(5)

# COMMAND ----------

dateDF.printSchema()

# COMMAND ----------

dateDF \
    .withColumn('bidaya', F.to_date(F.lit("2016-10-01"))) \
    .withColumn('nihaya', F.to_date(F.lit("2018-09-06"))) \
    .select(F.datediff(F.col('nihaya'), F.col('bidaya'))) \
    .show()

# COMMAND ----------

dateDF.printSchema()

# COMMAND ----------

dateDF = dateDF.withColumn('yet_another_literal', F.lit(8))

# COMMAND ----------

def third_power(val_from_Df):
    return val_from_Df ** 3

# register it with Spark as a UDF
third_power_udf = F.udf(third_power)

# COMMAND ----------

dateDF.withColumn('cubed', third_power(dateDF['yet_another_literal'])).show()

# COMMAND ----------

dateDF.withColumn('cubed_with_udf', third_power_udf(F.col('yet_another_literal'))).show()

# COMMAND ----------

# DBTITLE 1,Retail Df Defined
retailDf= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Workspace/Users/sak@cebs.io/by-day/*") \
    .coalesce(5)

# COMMAND ----------

retailDf.show(3)
retailDf.createOrReplaceTempView('retail_table')

# COMMAND ----------

retailDf.select(F.countDistinct('StockCode')).show()

# COMMAND ----------

retailDf.select(F.approx_count_distinct('StockCode', 0.01)).show()

# COMMAND ----------

retailDf.select(F.sum('Quantity')).show()

# COMMAND ----------

retailDf.groupBy("InvoiceNo", "CustomerID").count().show()

# COMMAND ----------

retailDfMod = retailDf.withColumn("date_of_invoice", F.to_date(F.col("InvoiceDate"))) \
            .select(F.col("date_of_invoice"), F.col("InvoiceDate"))

# COMMAND ----------

khirki = W.Window \
    .partitionBy("date_of_invoice") \
    .orderBy("date_of_invoice") \
    .rowsBetween(W.Window.unboundedPreceding, W.Window.currentRow)

# COMMAND ----------

invoiceKhirki = F.max(F.col("Quantity")).over(khirki)

# COMMAND ----------

purchaseDenseRank = F.dense_rank().over(khirki)
purchaseRank = F.rank().over(khirki)

# COMMAND ----------

retailDfMod.select(
    F.col('date_of_invoice'),
    purchaseDenseRank.alias('dense_ranked_purchases'),
    purchaseRank.alias('ranked_purchases')
).show()

# COMMAND ----------

# DBTITLE 1,JOINs
person = spark.createDataFrame([
 (0, "Bill Chambers", 0, [100]),
  (1, "Matei Zaharia", 1, [500, 250, 100]),
 (2, "Michael Armbrust", 1, [250, 100])]) \
 .toDF("id", "name", "graduate_program", "spark_status")

graduateProgram = spark.createDataFrame([
 (0, "Masters", "School of Information", "UC Berkeley"),
 (2, "Masters", "EECS", "UC Berkeley"),
 (1, "Ph.D.", "EECS", "UC Berkeley")]) \
 .toDF("id", "degree", "department", "school")

sparkStatus = spark.createDataFrame([
 (500, "Vice President"),
 (250, "PMC Member"),
 (100, "Contributor")]) \
 .toDF("id", "status")



person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# COMMAND ----------

leftJoinExpression = person['graduate_program'] == graduateProgram['id']

# COMMAND ----------

"""
person.join(graduateProgram, leftJoinExpression, 'left' ).show()

person.join(graduateProgram, leftJoinExpression, 'full_outer' ).show()

person.join(graduateProgram, leftJoinExpression, 'right' ).show()

person.join(graduateProgram, leftJoinExpression, 'cross' ).show()

"""
person.join(graduateProgram, leftJoinExpression, 'left' ).count()

#CROSS JOINs require an explicit function call
#person.crossJoin(graduateProgram).count()

# COMMAND ----------

person.join(F.broadcast(graduateProgram), leftJoinExpression, 'left' ).explain()
