# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/945359.ss@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

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

df_bronze.groupBy("product_id").agg(count(col("product_id")).alias("count_pid")).filter(col('count_pid')>1).display()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

window  = Window.partitionBy("product_id").orderBy(col("read_timestamp").desc())
df_silver = (
    df_bronze
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1))

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.drop('rn')

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.groupBy("product_id").agg(count(col("product_id")).alias("count_pid")).filter(col('count_pid')>1).display()

# COMMAND ----------

df_silver.filter(f.col('product_name') != f.trim(f.col('product_name'))).display()

# COMMAND ----------

df_silver.select(f.col('category')).distinct().display()

# COMMAND ----------

df_silver = df_silver.withColumn('category', when(col('category').isNull(),None).otherwise(initcap(col('category'))))

# COMMAND ----------

df_silver.select(f.col('category')).distinct().display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn('category', regexp_replace(col('category'), '(?i)Protein', 'Protein'))\
            .withColumn('product_name', regexp_replace(col('product_name'), '(?i)Protein', 'Protein'))

# COMMAND ----------

df_silver.select('category', 'product_name').distinct().display()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "division",
        when(col("category") == "Energy Bars",        "Nutrition Bars")
         .when(col("category") == "Protein Bars",       "Nutrition Bars")
         .when(col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)

df_silver = df_silver.withColumn(
    "variant",
    regexp_extract(col("product_name"), r"\((.*?)\)", 1)
)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "product_code",
        sha2(col("product_name").cast("string"), 256)
    )
    .withColumn(
        "product_id",
        when(
            col("product_id").cast("string").rlike("^[0-9]+$"),
            col("product_id").cast("string")
        ).otherwise(lit(999999).cast("string"))
    )
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

df_silver.display()


# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")


# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging Data source with Parents

# COMMAND ----------

delta_table = DeltaTable.forName(spark, 'fmcg.gold.dim_products')
df_child_products= spark.table("fmcg.gold.products").select(
    "product_code",
    "division",
    "category",
    "product",
    "variant"
)


# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()