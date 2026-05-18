# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

storage_account_name = "bankingdatalakepn"
storage_account_key = dbutils.secrets.get(
    scope="banking-scope",
    key="adls-storage-key"
)  # Retrieved securely from Azure Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"
gold_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

# Read Silver data
silver_txn  = spark.read.format("delta").load(f"{silver_path}/transactions/")
silver_cust = spark.read.format("delta").load(f"{silver_path}/customers/")

print(f"✅ Silver transactions: {silver_txn.count():,}")
print(f"✅ Silver customers: {silver_cust.count():,}")
print("✅ Ready to build Gold layer!")

# COMMAND ----------

# Most important Gold table — daily business summary
# This is what executives and BI dashboards consume

gold_daily_summary = silver_txn \
    .groupBy("txn_date", "channel", "txn_type", "currency") \
    .agg(
        count("transaction_id").alias("txn_count"),
        sum("amount_usd").cast("decimal(15,2)").alias("total_amount_usd"),
        avg("amount_usd").cast("decimal(10,2)").alias("avg_amount_usd"),
        max("amount_usd").cast("decimal(10,2)").alias("max_amount_usd"),
        min("amount_usd").cast("decimal(10,2)").alias("min_amount_usd"),
        countDistinct("account_id").alias("unique_accounts"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
        sum(when(col("is_fraud"), col("amount_usd"))
            .otherwise(0)).cast("decimal(15,2)").alias("fraud_amount_usd"),
        sum(when(col("is_large_transaction"), 1)
            .otherwise(0)).alias("large_txn_count"),
        sum(when(col("txn_status") == "FAILED", 1)
            .otherwise(0)).alias("failed_txn_count")
    ) \
    .withColumn("fraud_rate",
        (col("fraud_count") / col("txn_count") * 100)
        .cast("decimal(5,2)")) \
    .withColumn("failure_rate",
        (col("failed_txn_count") / col("txn_count") * 100)
        .cast("decimal(5,2)")) \
    .withColumn("_gold_timestamp", current_timestamp())

print(f"✅ Daily summary records: {gold_daily_summary.count():,}")
print("\n📊 Sample Gold Daily Summary:")
gold_daily_summary.orderBy("txn_date", "channel").show(10, truncate=False)

# COMMAND ----------

# Customer 360 — joins transactions with customer profile
# This is the most valuable table for business teams

customer_txn_summary = silver_txn \
    .groupBy("account_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount_usd").cast("decimal(15,2)").alias("total_spend_usd"),
        avg("amount_usd").cast("decimal(10,2)").alias("avg_txn_amount"),
        max("amount_usd").cast("decimal(10,2)").alias("max_txn_amount"),
        max("txn_date").alias("last_txn_date"),
        min("txn_date").alias("first_txn_date"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
        countDistinct("channel").alias("channels_used"),
        countDistinct("merchant_category").alias("categories_used"),
        sum(when(col("txn_type") == "DEBIT", col("amount_usd"))
            .otherwise(0)).cast("decimal(15,2)").alias("total_debits_usd"),
        sum(when(col("txn_type") == "CREDIT", col("amount_usd"))
            .otherwise(0)).cast("decimal(15,2)").alias("total_credits_usd")
    )

# Join with customer profile
gold_customer_360 = silver_cust.join(
    customer_txn_summary,
    on="account_id",
    how="left"
) \
.withColumn("is_high_risk",
    when(col("fraud_count") > 0, lit(True))
    .otherwise(lit(False))) \
.withColumn("customer_value_score",
    (col("total_spend_usd") / 1000 +
     col("total_transactions") * 0.1 +
     col("channels_used") * 5).cast("decimal(10,2)")) \
.withColumn("days_since_last_txn",
    datediff(current_date(), col("last_txn_date"))) \
.withColumn("_gold_timestamp", current_timestamp())

print(f"✅ Customer 360 records: {gold_customer_360.count():,}")
print("\n👥 High Risk Customers by Tier:")
gold_customer_360.groupBy("customer_tier", "is_high_risk") \
    .count() \
    .orderBy("customer_tier", "is_high_risk") \
    .show()

print("\n💎 Top 10 Customers by Value Score:")
gold_customer_360.select(
    "account_id", "customer_tier", "customer_segment",
    "total_transactions", "total_spend_usd", "customer_value_score"
).orderBy("customer_value_score", ascending=False).show(10)

# COMMAND ----------

# Fraud analytics table — used by fraud/risk teams
gold_fraud_summary = silver_txn \
    .filter(col("fraud_flag") != "CLEAN") \
    .groupBy("txn_date", "channel", "merchant_category", "fraud_flag") \
    .agg(
        count("transaction_id").alias("fraud_txn_count"),
        sum("amount_usd").cast("decimal(15,2)").alias("fraud_exposure_usd"),
        avg("amount_usd").cast("decimal(10,2)").alias("avg_fraud_amount"),
        countDistinct("account_id").alias("affected_accounts")
    ) \
    .withColumn("risk_level",
        when(col("fraud_exposure_usd") > 100000, lit("CRITICAL"))
        .when(col("fraud_exposure_usd") > 50000,  lit("HIGH"))
        .when(col("fraud_exposure_usd") > 10000,  lit("MEDIUM"))
        .otherwise(lit("LOW"))) \
    .withColumn("_gold_timestamp", current_timestamp())

print(f"✅ Fraud intelligence records: {gold_fraud_summary.count():,}")
print("\n🚨 Fraud by Channel:")
gold_fraud_summary.groupBy("channel") \
    .agg(
        sum("fraud_txn_count").alias("total_fraud_txns"),
        sum("fraud_exposure_usd").alias("total_exposure")
    ).orderBy("total_exposure", ascending=False).show()

print("\n⚠️ Risk Level Distribution:")
gold_fraud_summary.groupBy("risk_level") \
    .count() \
    .orderBy("count", ascending=False).show()

# COMMAND ----------

print("Writing Gold tables...")

# Daily Summary
gold_daily_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_date") \
    .save(f"{gold_path}/daily_txn_summary/")
print("✅ Gold daily_txn_summary written!")

# Customer 360
gold_customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/customer_360/")
print("✅ Gold customer_360 written!")

# Fraud Intelligence
gold_fraud_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/fraud_intelligence/")
print("✅ Gold fraud_intelligence written!")

print("\n🎉 ALL GOLD TABLES ARE LIVE!")

# COMMAND ----------

print("=" * 50)
print("   BANKING DATA PLATFORM — FULL VERIFICATION")
print("=" * 50)

# Gold tables
g_daily  = spark.read.format("delta").load(f"{gold_path}/daily_txn_summary/")
g_cust   = spark.read.format("delta").load(f"{gold_path}/customer_360/")
g_fraud  = spark.read.format("delta").load(f"{gold_path}/fraud_intelligence/")

print(f"\n🥉 BRONZE LAYER:")
print(f"   Raw Transactions : 1,000,000")
print(f"   Raw Customers    : ~10,200")

print(f"\n🥈 SILVER LAYER:")
print(f"   Clean Transactions : 946,639")
print(f"   Clean Customers    : 9,895")

print(f"\n🥇 GOLD LAYER:")
print(f"   Daily Summary      : {g_daily.count():,} records")
print(f"   Customer 360       : {g_cust.count():,} records")
print(f"   Fraud Intelligence : {g_fraud.count():,} records")

print(f"\n📊 KEY BUSINESS METRICS:")
total_fraud_exposure = g_fraud.agg(
    sum("fraud_exposure_usd")).collect()[0][0]
print(f"   Total Fraud Exposure : ${total_fraud_exposure:,.2f}")

avg_customer_value = g_cust.agg(
    avg("customer_value_score")).collect()[0][0]
print(f"   Avg Customer Score   : {avg_customer_value:,.2f}")

high_risk_customers = g_cust.filter(col("is_high_risk")).count()
print(f"   High Risk Customers  : {high_risk_customers:,}")

print("\n🎉 MEDALLION ARCHITECTURE COMPLETE!")
print("   Bronze → Silver → Gold ✅")

# COMMAND ----------

