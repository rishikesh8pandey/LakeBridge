# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/945359.ss@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-rr/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (spark.read.format("csv")
            .option('header', True)
            .option('inferSchema',True)
            .load(base_path)
            .withColumn("read_timestamp", f.current_timestamp())
            .select("*", "_metadata.file_name", "_metadata.file_size")           
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('delta')\
                        .option("delta.enableChangeDataFeed", "true")\
                        .mode('overwrite')\
                        .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source};")
df_bronze.display()

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_bronze.groupBy("customer_id").agg(f.count(f.col("customer_id")).alias("count_cid")).filter(f.col('count_cid')>1).display()

# COMMAND ----------

from pyspark.sql import Window
window = Window.partitionBy('customer_id').orderBy(f.col('read_timestamp').desc())
df_silver = df_bronze.withColumn('rn', f.row_number().over(window)).filter(f.col('rn')==1)


# COMMAND ----------

df_silver.agg(f.count("customer_id")).display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.filter(f.col('customer_name') != f.trim(f.col('customer_name'))).display()

# COMMAND ----------

df_silver = df_silver.withColumn('customer_name', f.trim(f.col('customer_name')))
df_silver.display()

# COMMAND ----------

df_silver.select('city').distinct().display()

# COMMAND ----------

city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'NewDheli' : 'New Delhi',
    'NewDelhee' : 'New Delhi',
    'NewDelhi' : 'New Delhi',

    'Hyderbad' : 'Hyderabad',
    'Hyderabadd' : 'Hyderabad'
}

allowed = ['Bengaluru', 'New Delhi', 'Hyderabad']

df_silver = df_silver.replace(city_mapping, subset = ['city'])\
    .withColumn('city',f.when(f.col("city").isNull(),None).when(f.col('city').isin(allowed), f.col("city")).otherwise(None))



# COMMAND ----------

df_silver.select('city').distinct().display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.filter(f.col('customer_name') != f.trim(f.col('customer_name'))).display()

# COMMAND ----------

df_silver = df_silver.withColumn('customer_name', f.when(f.col('customer_name').isNull(),None).otherwise(f.initcap(f.col('customer_name'))))

# COMMAND ----------

df_silver.select(f.col('customer_name')).distinct().display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.filter(f.col('city').isNull()).display()

# COMMAND ----------

null_customer_name = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(f.col('customer_name').isin(null_customer_name)).display()

# COMMAND ----------

customer_city_fix = [
    (789403 , 'New Delhi'),
    (789420 , 'Bengaluru'),
    (789521 , 'Hyderabad'),
    (789603 , 'Hyderabad')
]

df_fix = spark.createDataFrame(customer_city_fix,['customer_id','new_city'])

df_fix.display()


# COMMAND ----------



df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        f.coalesce("city", "new_city")  
    )
    .drop("new_city")
)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

null_customer_name = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(f.col('customer_name').isin(null_customer_name)).display()

# COMMAND ----------

df_silver.filter(f.col('city').isNull()).display()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", f.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = df_silver.drop('rn')
df_silver.display()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "customer",
        f.concat_ws("-", "customer_name", f.coalesce(f.col("city"), f.lit("Unknown")))
    )
    
    .withColumn("market", f.lit("India"))
    .withColumn("platform", f.lit("Sports Bar"))
    .withColumn("channel", f.lit("Acquisition"))
)

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold

# COMMAND ----------

df_silver = spark.sql(f'select * from {catalog}.{silver_schema}.{data_source};')

df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging data source with parents
# MAGIC

# COMMAND ----------

delta_table = DeltaTable.forName(spark, 'fmcg.gold.dim_customers')
df_child_customers = spark.table("fmcg.gold.customers").select(
    f.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()