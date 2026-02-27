Step 1 — Hash Compare (Detect Change)

from pyspark.sql.functions import sha2, concat_ws, col

src_df = src_df.withColumn(
    "hash_val",
    sha2(concat_ws("||", col("segment")), 256)
)

tgt_df = spark.table("prod.dim_customer") \
    .filter("is_active = 'Y'") \
    .withColumn(
        "hash_val",
        sha2(concat_ws("||", col("segment")), 256)
    )
✅ Step 2 — Find Changed Records
change_df = src_df.alias("s") \
    .join(tgt_df.alias("t"),
          "account_id",
          "inner") \
    .filter(col("s.hash_val") != col("t.hash_val")) \
    .select("s.*")

These are the ones you must SCD.

✅ Step 3 — Expire Old Version (MERGE 1)

This will:

✔ end-date old record
✔ set is_active = N

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

Now old Retail is closed.

✅ Step 4 — Insert New Version
from pyspark.sql.functions import lit, current_date

new_version_df = change_df \
    .withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31")) \
    .withColumn("is_active", lit("Y"))
✅ Step 5 — Insert into DIM
new_version_df.writeTo("prod.dim_customer") \
    .append()

Now SME becomes the only active row.

⚠️ Why not single MERGE?

Because this:

WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT

fails in SCD2:

Spark/Iceberg cannot:

update old row

AND insert new row

for the same matched key in same clause reliably.

You’ll either:

get two actives

or lose history

🔴 Final Production Safety Layer

Before your FACT joins DIM:

Always do:

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
    
 ---------------------TRade OFF's ------------------------------
 
 Why MERGE becomes painful in Spark (Iceberg / Delta)

MERGE is:

file-level rewrite

not row-level update

So when you MERGE:

Spark must:

Find matching files

Rewrite entire parquet files

Update manifest metadata

Commit new snapshot

Meaning:

Updating 1 customer
may rewrite a 512 MB file

Now scale that to:

5M changed records at month-end
→ thousands of file rewrites
→ commit contention
→ metadata bloat
→ slower reads later

This is why MERGE is:

✔ convenient
❌ not scalable for large SCD churn