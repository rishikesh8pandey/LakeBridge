# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/945359.ss@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-rr/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (spark.read.format("csv")
            .option('header', True)
            .option('inferSchema',True)
            .load(base_path)
            .withColumn("read_timestamp", current_timestamp())
            .select("*", "_metadata.file_name", "_metadata.file_size")           
)

# COMMAND ----------

df.printSchema()

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

df_bronze.select('month').distinct().display()

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "month",
    coalesce(
        try_to_date(col("month"), "yyyy/MM/dd"),
        try_to_date(col("month"), "dd/MM/yyyy"),
        try_to_date(col("month"), "yyyy-MM-dd"),
        try_to_date(col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

df_silver.select('month').distinct().display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    when(col("gross_price").rlike(r'^-?\d+(\.\d+)?$'), 
           when(col("gross_price").cast("double") < 0, -1 * col("gross_price").cast("double"))
            .otherwise(col("gross_price").cast("double")))
    .otherwise(0)
)

# COMMAND ----------

df_silver.display()


# COMMAND ----------

df_products = spark.table("fmcg.silver.products") 

df_joined = df_silver.join(
    df_products.select("product_id", "product_code"),
    on="product_id",
    how="inner"
)

# COMMAND ----------


df_joined = df_joined.select(
    "product_id",
    "product_code",
    "month",
    "gross_price",
    "read_timestamp",
    "file_name",
    "file_size"
)

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true")\
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold
# MAGIC

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_gold = df_silver.select("product_code", "month", "gross_price")


# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging data with parents
# MAGIC

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.gross_price")


# COMMAND ----------

df_gold_price.display()

# COMMAND ----------

from pyspark.sql.window import Window
df_gold_price = (
    df_gold_price
    .withColumn("year", year("month"))
    .withColumn("is_zero", when(col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(col("is_zero"), col("month").desc())
)


df_gold_latest_price = (
    df_gold_price
      .withColumn("rnk", row_number().over(w))
      .filter(col("rnk") == 1)
)


# COMMAND ----------

df_gold_price.display()

# COMMAND ----------


df_gold_latest_price = df_gold_latest_price.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")

df_gold_latest_price = df_gold_latest_price.withColumn("year", col("year").cast("string"))

df_gold_latest_price.display()

# COMMAND ----------

df_gold_latest_price.printSchema()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")


delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()

# COMMAND ----------

