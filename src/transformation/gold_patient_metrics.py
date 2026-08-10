from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, current_date


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("HealthcareGoldMetrics")
        .master("local[*]")
        .getOrCreate()
    )

    # Define Silver input and Gold output locations
    project_root = Path(__file__).resolve().parents[2]
    silver_path = project_root / "lakehouse" / "silver" / "patients"
    gold_path = project_root / "lakehouse" / "gold" / "patient_metrics"

    # Read cleaned Silver data
    silver_df = spark.read.parquet(str(silver_path))

    # Create reporting metrics
    gold_df = (
        silver_df
        .groupBy("state", "source_system", "data_quality_status")
        .agg(
            countDistinct("patient_id").alias("patient_count")
        )
        .withColumn("report_date", current_date())
        .orderBy("state", "source_system", "data_quality_status")
    )

    # Display reporting-ready Gold data
    print("\nGold patient metrics:")
    gold_df.show(truncate=False)

    # Save Gold metrics
    (
        gold_df.write
        .mode("overwrite")
        .parquet(str(gold_path))
    )

    print(f"\nGold metrics saved successfully to: {gold_path}")

    spark.stop()


if __name__ == "__main__":
    main()