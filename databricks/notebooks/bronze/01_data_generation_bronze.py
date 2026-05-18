# Databricks notebook source
# Verify Spark is working
print(f"Spark version: {spark.version}")
print("Spark is running!")


# COMMAND ----------

# TEMPORARY — direct key (do not push to GitHub)
storage_account_name = "bankingdatalakepn"

# Get key from Azure Portal → Storage account → Access keys → key1
storage_account_key = dbutils.secrets.get(
    scope="banking-scope",
    key="adls-storage-key"
)  # Retrieved securely from Azure Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("Connected to Azure Data Lake!")

for container in ["bronze", "silver", "gold"]:
    try:
        dbutils.fs.ls(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/")
        print(f"✅ {container} container - accessible")
    except:
        print(f"❌ {container} container - not found")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

print("✅ All libraries imported successfully!")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

num_customers = 10000

customers_df = spark.range(num_customers).select(
    # Some account IDs are null (missing from source)
    when(rand() < 0.02, lit(None).cast("long"))
    .otherwise((col("id") + 1000000).cast("long"))
    .alias("account_id"),

    # Some segments are garbage values from source system
    (array(
        lit("RETAIL"), lit("PREMIUM"), lit("BUSINESS"),
        lit("STUDENT"), lit("SENIOR"),
        lit("N/A"), lit("UNKNOWN"), lit(""),  # ← messy values
        lit(None)                              # ← nulls
    )[floor(rand() * 9).cast("int")]).alias("customer_segment"),

    # State sometimes comes as full name, sometimes abbreviation
    (array(
        lit("NEW_YORK"), lit("NY"), lit("new york"),  # ← inconsistent
        lit("CALIFORNIA"), lit("CA"), lit("california"),
        lit("TEXAS"), lit("TX"),
        lit("FLORIDA"), lit("FL"),
        lit("UNKNOWN"), lit(None)              # ← nulls
    )[floor(rand() * 11).cast("int")]).alias("state"),

    (array(
        lit("CHECKING"), lit("SAVINGS"), lit("BOTH"),
        lit("checking"), lit("CHK"),           # ← inconsistent case
        lit(None)
    )[floor(rand() * 6).cast("int")]).alias("account_type"),

    # Some credit limits are negative (bad source data)
    when(rand() < 0.01, lit(-999).cast("decimal(10,2)"))
    .when(rand() < 0.02, lit(None).cast("decimal(10,2)"))
    .otherwise((rand() * 50000 + 1000).cast("decimal(10,2)"))
    .alias("credit_limit"),

    # Some dates are in wrong format or future dates
    when(rand() < 0.02, lit("9999-12-31").cast("date"))  # ← future date
    .when(rand() < 0.01, lit(None).cast("date"))          # ← null date
    .otherwise(
        date_add(lit("2015-01-01"), (rand() * 3000).cast("int"))
    ).alias("account_open_date"),

    when(rand() < 0.05, lit("INACTIVE"))
    .when(rand() < 0.02, lit(None))            # ← null status
    .otherwise(lit("ACTIVE")).alias("account_status")
)

# Add duplicates — 3% of records are duplicates (common in real ETL)
duplicate_df = customers_df.sample(fraction=0.03, seed=42)
customers_df = customers_df.union(duplicate_df)

# Add metadata
customers_df = customers_df \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit("CRM")) \
    .withColumn("_batch_id", lit("BATCH_001"))

print(f"✅ Messy customer records generated: {customers_df.count():,}")
print(f"   (includes ~300 duplicates, nulls, bad values)")

# COMMAND ----------

num_transactions = 1000000

transactions_df = spark.range(num_transactions).select(
    # 1% duplicate transaction IDs — same txn arrives twice from source
    when(rand() < 0.01,
         (floor(rand() * 1000) + 10000000).cast("long"))  # ← duplicate ID
    .otherwise((col("id") + 10000000).cast("long"))
    .alias("transaction_id"),

    # Some account IDs are null
    when(rand() < 0.02, lit(None).cast("long"))
    .otherwise(((rand() * 9000) + 1000000).cast("long"))
    .alias("account_id"),

    # Some amounts are null, zero, or negative
    when(rand() < 0.01, lit(None).cast("decimal(10,2)"))
    .when(rand() < 0.01, lit(0).cast("decimal(10,2)"))    # ← zero amount
    .when(rand() < 0.005, lit(-50).cast("decimal(10,2)")) # ← negative
    .otherwise((rand() * 4999 + 1).cast("decimal(10,2)"))
    .alias("amount"),

    # Transaction type has inconsistent values from different source systems
    (array(
        lit("DEBIT"), lit("CREDIT"), lit("TRANSFER"),
        lit("PAYMENT"), lit("WITHDRAWAL"), lit("DEPOSIT"),
        lit("debit"), lit("Dr"), lit("Cr"),    # ← inconsistent
        lit("UNKNOWN"), lit(None)              # ← nulls
    )[floor(rand() * 11).cast("int")]).alias("txn_type"),

    (array(
        lit("ATM"), lit("ONLINE"), lit("BRANCH"),
        lit("MOBILE"), lit("WIRE"), lit("ACH"),
        lit("atm"), lit("Online Banking"),     # ← inconsistent
        lit(None)
    )[floor(rand() * 9).cast("int")]).alias("channel"),

    (array(
        lit("USD"), lit("EUR"), lit("GBP"), lit("INR"),
        lit("usd"), lit("$"), lit(None)        # ← inconsistent currency
    )[floor(rand() * 7).cast("int")]).alias("currency"),

    # Some dates are future dates, some are null, some are very old
    when(rand() < 0.01, lit("2099-01-01").cast("date"))   # ← future date
    .when(rand() < 0.01, lit(None).cast("date"))           # ← null date
    .when(rand() < 0.005, lit("1990-01-01").cast("date")) # ← very old date
    .otherwise(
        date_add(lit("2023-01-01"), (rand() * 730).cast("int"))
    ).alias("txn_date"),

    (rand() * 89999 + 10000).cast("int").alias("merchant_id"),

    (array(
        lit("GROCERY"), lit("RETAIL"), lit("RESTAURANT"),
        lit("FUEL"), lit("HEALTHCARE"), lit("TRAVEL"),
        lit("ENTERTAINMENT"), lit("UTILITIES"),
        lit(None), lit("MISC")                 # ← nulls
    )[floor(rand() * 10).cast("int")]).alias("merchant_category"),

    when(rand() < 0.002, lit("FRAUD_CONFIRMED"))
    .when(rand() < 0.008, lit("FRAUD_SUSPECTED"))
    .when(rand() < 0.02,  lit("REVIEW"))
    .otherwise(lit("CLEAN")).alias("fraud_flag"),

    (array(
        lit("COMPLETED"), lit("COMPLETED"), lit("COMPLETED"),
        lit("PENDING"), lit("FAILED"), lit(None)  # ← null status
    )[floor(rand() * 6).cast("int")]).alias("txn_status")
)

# Add partitioning columns and metadata
transactions_df = transactions_df \
    .withColumn("year",  year(col("txn_date"))) \
    .withColumn("month", month(col("txn_date"))) \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit("CORE_BANKING")) \
    .withColumn("_batch_id", lit("BATCH_001")) \
    .withColumn("_source_file", lit("core_banking_extract_20240101.csv"))

print(f"✅ Messy transaction records generated: {transactions_df.count():,}")

# Show how messy the data is
print("\n🔍 Data Quality Issues in Bronze (expected):")
print(f"Null transaction_ids : {transactions_df.filter(col('transaction_id').isNull()).count()}")
print(f"Null account_ids     : {transactions_df.filter(col('account_id').isNull()).count()}")
print(f"Null/zero amounts    : {transactions_df.filter((col('amount').isNull()) | (col('amount') <= 0)).count()}")
print(f"Null dates           : {transactions_df.filter(col('txn_date').isNull()).count()}")
print(f"Future dates         : {transactions_df.filter(col('txn_date') > current_date()).count()}")
print(f"Null txn_type        : {transactions_df.filter(col('txn_type').isNull()).count()}")
print(f"Null currency        : {transactions_df.filter(col('currency').isNull()).count()}")
print("\n✅ This is realistic Bronze data — messy, raw, unprocessed!")

# COMMAND ----------

# Write customers to Bronze layer
storage_account_name = "bankingdatalakepn"
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"

print("Writing customers to Bronze layer...")
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{bronze_path}/customers/")

print("✅ Customers written to Bronze!")

# Write transactions to Bronze layer — partitioned by year/month
print("Writing transactions to Bronze layer...")
transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(f"{bronze_path}/transactions/")

print("✅ Transactions written to Bronze!")
print("🎉 Bronze layer population COMPLETE!")

# COMMAND ----------

# Read back and verify data landed correctly
print("=== BRONZE LAYER VERIFICATION ===")

# Customers
bronze_customers = spark.read.format("delta") \
    .load(f"{bronze_path}/customers/")
print(f"✅ Bronze Customers: {bronze_customers.count():,} records")

# Transactions
bronze_transactions = spark.read.format("delta") \
    .load(f"{bronze_path}/transactions/")
print(f"✅ Bronze Transactions: {bronze_transactions.count():,} records")

# Show partition breakdown
print("\n📊 Transactions by Year/Month:")
bronze_transactions.groupBy("year", "month") \
    .count() \
    .orderBy("year", "month") \
    .show()

# Show fraud breakdown
print("🚨 Fraud Flag Distribution:")
bronze_transactions.groupBy("fraud_flag") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

print("\n🎉 BRONZE LAYER IS LIVE!")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

