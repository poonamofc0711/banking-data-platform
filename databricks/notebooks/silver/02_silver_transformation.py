# Databricks notebook source
# Setup — run this every session
storage_account_name = "bankingdatalakepn"
storage_account_key = dbutils.secrets.get(
    scope="banking-scope",
    key="adls-storage-key"
)  # Retrieved securely from Azure Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"

print("✅ Connected!")

# COMMAND ----------

# Read raw bronze data
from pyspark.sql.functions import *

bronze_transactions = spark.read.format("delta") \
    .load(f"{bronze_path}/transactions/")

bronze_customers = spark.read.format("delta") \
    .load(f"{bronze_path}/customers/")

print(f"✅ Bronze transactions loaded: {bronze_transactions.count():,}")
print(f"✅ Bronze customers loaded: {bronze_customers.count():,}")

# Check for data quality issues before transformation
print("\n📊 Data Quality Check — Bronze Transactions:")
print(f"Null transaction_ids: {bronze_transactions.filter(col('transaction_id').isNull()).count()}")
print(f"Negative amounts: {bronze_transactions.filter(col('amount') < 0).count()}")
print(f"Null dates: {bronze_transactions.filter(col('txn_date').isNull()).count()}")
print(f"Future dates: {bronze_transactions.filter(col('txn_date') > current_date()).count()}")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Silver transformation — this is where real engineering happens

# First standardize currency column itself
silver_transactions = bronze_transactions \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("txn_date").isNotNull()) \
    .filter(col("txn_date") <= current_date()) \
    
    # Standardize currency values first
silver_transactions = silver_transactions \
    .withColumn("currency_clean",
        when(upper(col("currency")).isin("USD", "$"), lit("USD"))
        .when(upper(col("currency")) == "EUR", lit("EUR"))
        .when(upper(col("currency")) == "GBP", lit("GBP"))
        .when(upper(col("currency")) == "INR", lit("INR"))
        .otherwise(lit("USD")))  # default unknown to USD \
    
    # Standardize txn_type
silver_transactions = silver_transactions \
    .withColumn("txn_type_clean",
        when(upper(col("txn_type")).isin("DEBIT", "DR"), lit("DEBIT"))
        .when(upper(col("txn_type")).isin("CREDIT", "CR"), lit("CREDIT"))
        .when(upper(col("txn_type")) == "TRANSFER", lit("TRANSFER"))
        .when(upper(col("txn_type")) == "PAYMENT", lit("PAYMENT"))
        .when(upper(col("txn_type")) == "WITHDRAWAL", lit("WITHDRAWAL"))
        .when(upper(col("txn_type")) == "DEPOSIT", lit("DEPOSIT"))
        .otherwise(lit("OTHER"))) \
    .withColumn("amount_usd",
        when(col("currency_clean") == "EUR", col("amount") * 1.08)
        .when(col("currency_clean") == "GBP", col("amount") * 1.27)
        .when(col("currency_clean") == "INR", col("amount") * 0.012)
        .otherwise(col("amount")).cast("decimal(10,2)")) \
    .withColumn("is_fraud",
        when(col("fraud_flag").isin("FRAUD_CONFIRMED", "FRAUD_SUSPECTED"),
             lit(True))
        .otherwise(lit(False))) \
    .withColumn("is_large_transaction",
        when(col("amount_usd") > 10000, lit(True))
        .otherwise(lit(False))) \
    .withColumn("txn_day_of_week", dayofweek(col("txn_date"))) \
    .withColumn("txn_quarter", quarter(col("txn_date"))) \
    .withColumn("_silver_timestamp", current_timestamp()) \
    .withColumn("_silver_batch_id", lit("SILVER_001")) \
    .drop("currency") \
    .withColumnRenamed("currency_clean", "currency") \
    .drop("txn_type") \
    .withColumnRenamed("txn_type_clean", "txn_type")

# Deduplication
window_spec = Window.partitionBy("transaction_id") \
                    .orderBy(col("_ingestion_timestamp").desc())

silver_transactions = silver_transactions \
    .withColumn("row_rank", row_number().over(window_spec)) \
    .filter(col("row_rank") == 1) \
    .drop("row_rank")

print(f"✅ Bronze records: {bronze_transactions.count():,}")
print(f"✅ Silver records after cleaning: {silver_transactions.count():,}")
print(f"✅ Records removed: {bronze_transactions.count() - silver_transactions.count():,}")

# Verify currency is now clean
print("\n💰 Currency after standardization:")
silver_transactions.groupBy("currency").count().show()

print("\n📋 Transaction types after standardization:")
silver_transactions.groupBy("txn_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Clean and standardize customer data
silver_customers = bronze_customers \
    .filter(col("account_id").isNotNull()) \
    .filter(col("account_status").isNotNull()) \
    .withColumn("customer_tier",
        when(col("credit_limit") >= 40000, lit("PLATINUM"))
        .when(col("credit_limit") >= 25000, lit("GOLD"))
        .when(col("credit_limit") >= 10000, lit("SILVER"))
        .otherwise(lit("STANDARD"))) \
    .withColumn("account_age_days",
        datediff(current_date(), col("account_open_date"))) \
    .withColumn("account_age_years",
        (col("account_age_days") / 365).cast("int")) \
    .withColumn("is_active",
        when(col("account_status") == "ACTIVE", lit(True))
        .otherwise(lit(False))) \
    .withColumn("_silver_timestamp", current_timestamp())

print(f"✅ Silver customers: {silver_customers.count():,}")

print("\n📊 Customer Tier Distribution:")
silver_customers.groupBy("customer_tier") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

# Write to Silver layer in Delta format
print("Writing transactions to Silver layer...")
silver_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(f"{silver_path}/transactions/")
print("✅ Silver transactions written!")

print("Writing customers to Silver layer...")
silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/customers/")
print("✅ Silver customers written!")

# COMMAND ----------

# Verify silver data quality
print("=== SILVER LAYER VERIFICATION ===")

sv_txn = spark.read.format("delta") \
    .load(f"{silver_path}/transactions/")
sv_cust = spark.read.format("delta") \
    .load(f"{silver_path}/customers/")

print(f"✅ Silver Transactions: {sv_txn.count():,}")
print(f"✅ Silver Customers: {sv_cust.count():,}")

print("\n💰 Currency Distribution After Standardization:")
sv_txn.groupBy("currency") \
    .agg(
        count("transaction_id").alias("txn_count"),
        avg("amount_usd").cast("decimal(10,2)").alias("avg_amount_usd")
    ).orderBy("txn_count", ascending=False) \
    .show()

print("\n🚨 Fraud Summary:")
sv_txn.groupBy("fraud_flag", "is_fraud") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

print("\n👥 Customer Tier Summary:")
sv_cust.groupBy("customer_tier", "account_status") \
    .count() \
    .orderBy("customer_tier") \
    .show()

print("\n🎉 SILVER LAYER IS LIVE!")

# COMMAND ----------

