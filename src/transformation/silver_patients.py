from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, when


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("HealthcareSilverTransformation")
        .master("local[*]")
        .getOrCreate()
    )

    # Define Bronze input and Silver output locations
    project_root = Path(__file__).resolve().parents[2]
    bronze_path = project_root / "lakehouse" / "bronze" / "patients"
    silver_path = project_root / "lakehouse" / "silver" / "patients"

    # Read raw data from Bronze layer
    bronze_df = spark.read.parquet(str(bronze_path))

    # Clean and standardize the data
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

    # Display cleaned Silver data
    print("\nCleaned Silver patient data:")
    silver_df.show(truncate=False)

    # Save cleaned data in Silver layer
    (
        silver_df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )

    print(f"\nSilver data saved successfully to: {silver_path}")

    spark.stop()


if __name__ == "__main__":
    main()