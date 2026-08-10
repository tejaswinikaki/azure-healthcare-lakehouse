from ingestion.bronze_ingestion import main as run_bronze_ingestion
from transformation.silver_patients import main as run_silver_transformation
from transformation.gold_patient_metrics import main as run_gold_metrics


def main():
    print("\nStarting Healthcare Lakehouse Pipeline\n")

    print("Step 1: Running Bronze ingestion")
    run_bronze_ingestion()

    print("\nStep 2: Running Silver transformation")
    run_silver_transformation()

    print("\nStep 3: Running Gold metrics creation")
    run_gold_metrics()

    print("\nHealthcare Lakehouse Pipeline completed successfully!")


if __name__ == "__main__":
    main()