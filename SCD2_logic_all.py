---------------COMMON SETUP and SCD2 code ---------------

from pyspark.sql.functions import col, lit, sha2, concat_ws, current_date, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# SOURCE (Incoming)
src_df = spark.read.format("parquet").load("/mnt/source/customer")

# ACTIVE TARGET SNAPSHOT
tgt_active = spark.table("prod.dim_customer") \
    .filter("is_active = 'Y'")

# Define tracked business columns ONLY (avoid schema evolution false SCD)
tracked_cols = ["segment"]

# Add hash
src_df = src_df.withColumn(
    "hash_val",
    sha2(concat_ws("||", *tracked_cols), 256)
)

tgt_active = tgt_active.withColumn(
    "hash_val",
    sha2(concat_ws("||", *tracked_cols), 256)
)

# NEW RECORDS
new_df = src_df.alias("s") \
    .join(tgt_active.alias("t"),
          "account_id",
          "left_anti")

# EXISTING BUT CHANGED
change_df = src_df.alias("s") \
    .join(tgt_active.alias("t"),
          "account_id",
          "inner") \
    .filter(col("s.hash_val") != col("t.hash_val")) \
    .select("s.*")
    
    
-------------------------------------------------------------

Merge - based -- DElta

change_df.createOrReplaceTempView("updates")

spark.sql("""
MERGE INTO prod.dim_customer t
USING updates s
ON t.account_id = s.account_id
AND t.is_active = 'Y'
WHEN MATCHED THEN
UPDATE SET
t.effective_to = current_date() - 1,
t.is_active = 'N'
""")

# INSERT NEW VERSION
new_version_df = change_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

# INSERT BRAND NEW
new_insert_df = new_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

final_df = new_version_df.unionByName(new_insert_df)

final_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("prod.dim_customer")
----------------------------------------------------

merge based iceberg 
change_df.createOrReplaceTempView("updates")

spark.sql("""
MERGE INTO prod.dim_customer t
USING updates s
ON t.account_id = s.account_id
AND t.is_active = 'Y'
WHEN MATCHED THEN
UPDATE SET
t.effective_to = current_date() - 1,
t.is_active = 'N'
""")

# INSERT NEW VERSION
new_version_df = change_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

# INSERT BRAND NEW
new_insert_df = new_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

final_df = new_version_df.unionByName(new_insert_df)

final_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("prod.dim_customer")
    ------------------------------------------------
    
    insert only  SCD - same logic for delta and iceberg
    change_df.createOrReplaceTempView("updates")

spark.sql("""
MERGE INTO prod.dim_customer t
USING updates s
ON t.account_id = s.account_id
AND t.is_active = 'Y'
WHEN MATCHED THEN
UPDATE SET
t.effective_to = current_date() - 1,
t.is_active = 'N'
""")

# INSERT NEW VERSION
new_version_df = change_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

# INSERT BRAND NEW
new_insert_df = new_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

final_df = new_version_df.unionByName(new_insert_df)

final_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("prod.dim_customer")
    
 --------------------------------------------------------
 write for delta:
 final_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("prod.dim_customer")
 write for iceberg:   
    final_df.writeTo("prod.dim_customer").append()
    
----------------------------------------------------------- 
 3. SNAPSHOT REBUILD (ACTIVE LAYER)

Assume:

prod.dim_customer_hist   → history (insert-only)
prod.dim_customer_active → reporting snapshot
Step 1 — Current Active
tgt_active = spark.table("prod.dim_customer_active")
Step 2 — Remove Changed Keys
unchanged_df = tgt_active \
    .join(change_df.select("account_id"),
          "account_id",
          "left_anti")
Step 3 — Add Updated + New
new_active_df = unchanged_df \
    .unionByName(new_version_df) \
    .unionByName(new_insert_df)
🔷 DELTA SNAPSHOT OVERWRITE
new_active_df.coalesce(200) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("prod.dim_customer_active")
🔷 ICEBERG SNAPSHOT OVERWRITE
new_active_df.coalesce(200) \
    .writeTo("prod.dim_customer_active") \
    .overwritePartitions()
🟨 FACT JOIN (Read-Time Safety)
w = Window.partitionBy("account_id") \
          .orderBy(col("effective_from").desc())

dim_current = spark.table("prod.dim_customer_hist") \
    .filter(
        (col("effective_from") <= lit("2026-02-25")) &
        (col("effective_to") > lit("2026-02-25"))
    ) \
    .withColumn("rn", row_number().over(w)) \
    .filter("rn = 1") \
    .drop("rn")