from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("HealthcareLakehouseTest")
        .master("local[*]")
        .getOrCreate()
    )

    data = [
        (1, "Patient A", 29),
        (2, "Patient B", 42),
        (3, "Patient C", 35),
    ]

    columns = ["patient_id", "patient_name", "age"]

    patients_df = spark.createDataFrame(data, columns)

    print("\nPySpark is working successfully!\n")
    patients_df.show()

    spark.stop()


if __name__ == "__main__":
    main()