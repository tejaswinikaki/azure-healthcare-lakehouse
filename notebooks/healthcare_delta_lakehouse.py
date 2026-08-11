# Databricks notebook source
display(
    spark.sql("""
        SELECT
            current_catalog() AS current_catalog,
            current_schema() AS current_schema
    """)
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS healthcare_lakehouse")
spark.sql("USE SCHEMA healthcare_lakehouse")

display(
    spark.sql("""
        SELECT
            current_catalog() AS current_catalog,
            current_schema() AS current_schema
    """)
)

# COMMAND ----------

# Synthetic raw healthcare data
data = [
    ("P001", "Olivia", "Martin", "1989-04-15", "Female", "Kansas City", "MO", "olivia.martin@example.com", "EHR", "2026-08-01"),
    ("P002", "Noah", "Brown", "1978-11-20", "Male", "Overland Park", "KS", "noah.brown@example.com", "EHR", "2026-08-01"),
    ("P003", "Emma", "Davis", "1995-02-28", "Female", "Lenexa", "KS", "emma.davis@example.com", "EHR", "2026-08-01"),
    ("P004", "Liam", "Wilson", "1983-07-09", "Male", "Olathe", "KS", "liam.wilson@example.com", "EHR", "2026-08-01"),
    ("P005", "Ava", "Johnson", "1991-12-05", "Female", "Kansas City", "MO", "ava.johnson@example.com", "EHR", "2026-08-01"),
    ("P006", "Ethan", "Taylor", "1986-03-17", "Male", "Shawnee", "KS", "ethan.taylor@example.com", "EHR", "2026-08-01"),
    ("P006", "Ethan", "Taylor", "1986-03-17", "Male", "Shawnee", "KS", "ethan.taylor@example.com", "EHR", "2026-08-01"),
    ("P007", "Mia", "Anderson", None, "Female", "Leawood", "KS", "mia.anderson@example.com", "EHR", "2026-08-01"),
    ("P008", "James", "Thomas", "1990-08-22", "Male", None, "KS", "james.thomas@example.com", "EHR", "2026-08-01"),
    ("P009", "Sophia", "Jackson", "1988-01-30", "Female", "Kansas City", "MO", None, "EHR", "2026-08-01"),
    ("P010", "Benjamin", "White", "1975-06-11", "Male", "Overland Park", "KS", "benjamin.white@example.com", "Claims", "2026-08-01")
]

columns = [
    "patient_id", "first_name", "last_name", "date_of_birth",
    "gender", "city", "state", "email", "source_system",
    "ingestion_date"
]

raw_df = spark.createDataFrame(data, columns)

# Save the raw records as a Delta table
(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_patients")
)

display(spark.table("bronze_patients"))

# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, when

bronze_df = spark.table("bronze_patients")

silver_df = (
    bronze_df
    .dropDuplicates(["patient_id"])
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn(
        "city",
        when(col("city").isNull() | (trim(col("city")) == ""), lit("Unknown"))
        .otherwise(trim(col("city")))
    )
    .withColumn(
        "email",
        when(col("email").isNull() | (trim(col("email")) == ""), lit("Unknown"))
        .otherwise(trim(col("email")))
    )
    .withColumn(
        "data_quality_status",
        when(col("date_of_birth").isNull(), lit("Review"))
        .otherwise(lit("Valid"))
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_patients")
)

display(spark.table("silver_patients"))

# COMMAND ----------

from pyspark.sql.functions import countDistinct, current_date

silver_df = spark.table("silver_patients")

gold_metrics_df = (
    silver_df
    .groupBy("state", "source_system", "data_quality_status")
    .agg(
        countDistinct("patient_id").alias("patient_count")
    )
    .withColumn("report_date", current_date())
    .orderBy("state", "source_system", "data_quality_status")
)

(
    gold_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_patient_metrics")
)

display(spark.table("gold_patient_metrics"))

# COMMAND ----------

from pyspark.sql.functions import col

bronze_df = spark.table("bronze_patients")
silver_df = spark.table("silver_patients")

raw_record_count = bronze_df.count()

metrics = [
    ("raw_record_count", raw_record_count),
    (
        "duplicate_patient_count",
        raw_record_count - bronze_df.dropDuplicates(["patient_id"]).count()
    ),
    (
        "missing_date_of_birth_count",
        bronze_df.filter(col("date_of_birth").isNull()).count()
    ),
    (
        "missing_city_count",
        bronze_df.filter(col("city").isNull()).count()
    ),
    (
        "missing_email_count",
        bronze_df.filter(col("email").isNull()).count()
    ),
    ("silver_record_count", silver_df.count()),
    (
        "review_record_count",
        silver_df.filter(col("data_quality_status") == "Review").count()
    )
]

quality_df = spark.createDataFrame(
    metrics,
    ["metric_name", "metric_value"]
)

(
    quality_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("data_quality_metrics")
)

display(spark.table("data_quality_metrics"))

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, current_date, lit, trim, when

# Create and use the project schema
spark.sql("CREATE SCHEMA IF NOT EXISTS healthcare_lakehouse")

bronze_table = "workspace.healthcare_lakehouse.bronze_patients"
silver_table = "workspace.healthcare_lakehouse.silver_patients"
gold_table = "workspace.healthcare_lakehouse.gold_patient_metrics"
quality_table = "workspace.healthcare_lakehouse.data_quality_metrics"

# 1. Bronze Delta table
data = [
    ("P001", "Olivia", "Martin", "1989-04-15", "Female", "Kansas City", "MO", "olivia.martin@example.com", "EHR", "2026-08-01"),
    ("P002", "Noah", "Brown", "1978-11-20", "Male", "Overland Park", "KS", "noah.brown@example.com", "EHR", "2026-08-01"),
    ("P003", "Emma", "Davis", "1995-02-28", "Female", "Lenexa", "KS", "emma.davis@example.com", "EHR", "2026-08-01"),
    ("P004", "Liam", "Wilson", "1983-07-09", "Male", "Olathe", "KS", "liam.wilson@example.com", "EHR", "2026-08-01"),
    ("P005", "Ava", "Johnson", "1991-12-05", "Female", "Kansas City", "MO", "ava.johnson@example.com", "EHR", "2026-08-01"),
    ("P006", "Ethan", "Taylor", "1986-03-17", "Male", "Shawnee", "KS", "ethan.taylor@example.com", "EHR", "2026-08-01"),
    ("P006", "Ethan", "Taylor", "1986-03-17", "Male", "Shawnee", "KS", "ethan.taylor@example.com", "EHR", "2026-08-01"),
    ("P007", "Mia", "Anderson", None, "Female", "Leawood", "KS", "mia.anderson@example.com", "EHR", "2026-08-01"),
    ("P008", "James", "Thomas", "1990-08-22", "Male", None, "KS", "james.thomas@example.com", "EHR", "2026-08-01"),
    ("P009", "Sophia", "Jackson", "1988-01-30", "Female", "Kansas City", "MO", None, "EHR", "2026-08-01"),
    ("P010", "Benjamin", "White", "1975-06-11", "Male", "Overland Park", "KS", "benjamin.white@example.com", "Claims", "2026-08-01")
]

columns = [
    "patient_id", "first_name", "last_name", "date_of_birth",
    "gender", "city", "state", "email", "source_system", "ingestion_date"
]

raw_df = spark.createDataFrame(data, columns)

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

# 2. Silver Delta table
silver_df = (
    raw_df
    .dropDuplicates(["patient_id"])
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn(
        "city",
        when(col("city").isNull(), lit("Unknown")).otherwise(trim(col("city")))
    )
    .withColumn(
        "email",
        when(col("email").isNull(), lit("Unknown")).otherwise(trim(col("email")))
    )
    .withColumn(
        "data_quality_status",
        when(col("date_of_birth").isNull(), lit("Review")).otherwise(lit("Valid"))
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# 3. Gold metrics Delta table
gold_df = (
    silver_df
    .groupBy("state", "source_system", "data_quality_status")
    .agg(countDistinct("patient_id").alias("patient_count"))
    .withColumn("report_date", current_date())
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

# 4. Data-quality Delta table
raw_record_count = raw_df.count()

metrics = [
    ("raw_record_count", raw_record_count),
    ("duplicate_patient_count", raw_record_count - raw_df.dropDuplicates(["patient_id"]).count()),
    ("missing_date_of_birth_count", raw_df.filter(col("date_of_birth").isNull()).count()),
    ("missing_city_count", raw_df.filter(col("city").isNull()).count()),
    ("missing_email_count", raw_df.filter(col("email").isNull()).count()),
    ("silver_record_count", silver_df.count()),
    ("review_record_count", silver_df.filter(col("data_quality_status") == "Review").count())
]

quality_df = spark.createDataFrame(metrics, ["metric_name", "metric_value"])

(
    quality_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(quality_table)
)

display(spark.sql("SHOW TABLES IN workspace.healthcare_lakehouse"))

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE DETAIL workspace.healthcare_lakehouse.bronze_patients
    """).select(
        "format",
        "location",
        "numFiles",
        "sizeInBytes"
    )
)