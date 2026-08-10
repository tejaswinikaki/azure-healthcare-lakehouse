from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("HealthcareDataQualityReport")
        .master("local[*]")
        .getOrCreate()
    )

    # Define data locations
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / "data" / "raw" / "patients.csv"
    silver_path = project_root / "lakehouse" / "silver" / "patients"
    quality_output_path = project_root / "lakehouse" / "gold" / "data_quality_metrics"

    # Read raw and cleaned data
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(raw_path))
    )

    silver_df = spark.read.parquet(str(silver_path))

    # Calculate data-quality metrics
    raw_record_count = raw_df.count()
    duplicate_patient_count = (
        raw_record_count - raw_df.dropDuplicates(["patient_id"]).count()
    )
    missing_date_of_birth_count = raw_df.filter(
        col("date_of_birth").isNull()
    ).count()
    missing_city_count = raw_df.filter(col("city").isNull()).count()
    missing_email_count = raw_df.filter(col("email").isNull()).count()
    silver_record_count = silver_df.count()
    review_record_count = silver_df.filter(
        col("data_quality_status") == "Review"
    ).count()

    # Create one reporting table for quality metrics
    metrics = [
        ("raw_record_count", raw_record_count),
        ("duplicate_patient_count", duplicate_patient_count),
        ("missing_date_of_birth_count", missing_date_of_birth_count),
        ("missing_city_count", missing_city_count),
        ("missing_email_count", missing_email_count),
        ("silver_record_count", silver_record_count),
        ("review_record_count", review_record_count),
    ]

    quality_df = spark.createDataFrame(
        metrics,
        ["metric_name", "metric_value"]
    )

    print("\nData quality report:")
    quality_df.show(truncate=False)

    # Save report in Gold layer
    (
        quality_df.write
        .mode("overwrite")
        .parquet(str(quality_output_path))
    )

    print(f"\nData quality report saved successfully to: {quality_output_path}")

    spark.stop()


if __name__ == "__main__":
    main()