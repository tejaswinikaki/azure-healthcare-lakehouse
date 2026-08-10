from pathlib import Path
from pyspark.sql import SparkSession


def main():
    # Create a local Spark session
    spark = (
        SparkSession.builder
        .appName("HealthcareBronzeIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    # Find the project folder automatically
    project_root = Path(__file__).resolve().parents[2]

    # Define input and output locations
    input_path = project_root / "data" / "raw" / "patients.csv"
    output_path = project_root / "lakehouse" / "bronze" / "patients"

    # Read the raw CSV file
    patients_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )

    # Show the incoming raw data
    print("\nRaw patient data:")
    patients_df.show(truncate=False)

    # Save the data in the Bronze layer
    (
        patients_df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"\nBronze data saved successfully to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()