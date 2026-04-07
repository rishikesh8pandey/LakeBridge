# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/945359.ss@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-rr/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)



bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze
# MAGIC

# COMMAND ----------

df = (spark.read.format("csv")
            .option('header', True)
            .option('inferSchema',True)
            .load(landing_path)
            .withColumn("read_timestamp", current_timestamp())
            .select("*", "_metadata.file_name", "_metadata.file_size")           
)

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("append") \
 .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.display()

# COMMAND ----------

df_orders = df_orders.filter(col("order_qty").isNotNull())

# COMMAND ----------

df_orders.display()

# COMMAND ----------

df_orders = df_orders.withColumn(
    "customer_id",
    when(col("customer_id").rlike("^[0-9]+$"), col("customer_id"))
     .otherwise("999999")
     .cast("string")
)


# COMMAND ----------

df_orders.select('customer_id').display()

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_placement_date",
    regexp_replace(col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)


# COMMAND ----------

df_orders.select('order_placement_date').display(
    
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_placement_date",
    coalesce(
        try_to_date("order_placement_date", "yyyy/MM/dd"),
        try_to_date("order_placement_date", "dd-MM-yyyy"),
        try_to_date("order_placement_date", "dd/MM/yyyy"),
        try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# COMMAND ----------

df_orders.select('order_placement_date').display()

# COMMAND ----------

df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# COMMAND ----------

df_orders = df_orders.withColumn('product_id', col('product_id').cast('string'))

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_orders.agg(
    min("order_placement_date").alias("min_date"),
    max("order_placement_date").alias("max_date")
).display()

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])



# COMMAND ----------

df_joined.display()

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")



# COMMAND ----------

df_gold.display()

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging with parents company

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")


# COMMAND ----------

df_child.display()

# COMMAND ----------

df_child.count()

# COMMAND ----------

df_monthly = (
    df_child

    .withColumn("month_start", trunc("date", "MM"))  

    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        sum("sold_quantity").alias("sold_quantity")
    )


    .withColumnRenamed("month_start", "date")
)

df_monthly.show(5, truncate=False)

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

