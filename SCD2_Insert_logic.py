Step 1 — Active Target Snapshot
tgt_active = spark.table("prod.dim_customer") \
    .filter("is_active = 'Y'")
Step 2 — Detect NEW Records
new_df = src_df.alias("s") \
    .join(tgt_active.alias("t"),
          "account_id",
          "left_anti")

These:

✔ never existed
✔ directly insert as active

Step 3 — Detect CHANGED Records
change_df = src_df.alias("s") \
    .join(tgt_active.alias("t"),
          "account_id",
          "inner") \
    .filter(col("s.hash_val") != col("t.hash_val")) \
    .select("s.*")
Step 4 — Closure Insert (expire old)

Take existing active records
for those changed keys:

closure_df = tgt_active.alias("t") \
    .join(change_df.select("account_id"),
          "account_id",
          "inner") \
    .withColumn("effective_to",
                current_date() - expr("INTERVAL 1 DAY")) \
    .withColumn("is_active", lit("N"))

No update.

You are appending a superseding version.

Step 5 — New Active Version Insert

For changed:

new_version_df = change_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))

For new customers:

new_insert_df = new_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))
Step 6 — Final Append Set
final_df = closure_df \
    .unionByName(new_version_df) \
    .unionByName(new_insert_df)
Step 7 — Write
Iceberg:
final_df.writeTo("prod.dim_customer").append()
Delta:
final_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("prod.dim_customer")
Important:

You are NOT physically turning old row inactive.

You are:

inserting a later version
whose validity supersedes the old one

So old stays in file
but becomes logically inactive
via:

effective_from <= reporting_date
AND effective_to > reporting_date
Read Pattern Before FACT Join:

Always:

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("account_id") \
          .orderBy(col("effective_from").desc())

dim_current = spark.table("prod.dim_customer") \
    .filter(
        (col("effective_from") <= reporting_date) &
        (col("effective_to") > reporting_date)
    ) \
    .withColumn("rn", row_number().over(w)) \
    .filter("rn = 1") \
    .drop("rn")