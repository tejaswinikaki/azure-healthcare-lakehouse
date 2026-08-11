# Azure Healthcare Lakehouse Pipeline

An end-to-end healthcare data pipeline built with Python and PySpark. The project ingests raw patient data, applies data-quality checks and transformations, and produces reporting-ready healthcare metrics using a Bronze, Silver, and Gold lakehouse design.

> Note: This project uses synthetic patient data only. No real patient or healthcare data is included.

## Architecture

```text
Raw CSV Data
    ↓
Bronze Layer: Raw patient records
    ↓
Silver Layer: Cleaned and validated patient records
    ↓
Data Quality Report: Missing fields, duplicates, review counts
    ↓
Gold Layer: Patient reporting metrics
```

## Technologies Used

- Python
- PySpark and Spark SQL
- Parquet storage
- Bronze, Silver, and Gold lakehouse design
- Data-quality validation
- Git and GitHub
- Conda environment management

## Pipeline Flow

### 1. Bronze ingestion

The pipeline reads the raw `patients.csv` source file and stores the incoming data in the Bronze layer without changing it.

### 2. Silver transformation

The pipeline cleans the Bronze data by:

- Removing duplicate patient records
- Trimming name fields
- Replacing missing city and email values with `Unknown`
- Marking records with missing date of birth as `Review`

### 3. Data-quality reporting

The pipeline creates a report containing:

- Total raw records received
- Duplicate patient count
- Missing date-of-birth count
- Missing city count
- Missing email count
- Cleaned Silver record count
- Records requiring review

### 4. Gold reporting metrics

The pipeline creates a reporting-ready summary of patient counts by:

- State
- Source system
- Data-quality status

## Project Structure

```text
azure-healthcare-lakehouse/
├── data/raw/                       # Synthetic source data
│   └── patients.csv
├── src/
│   ├── ingestion/
│   │   └── bronze_ingestion.py
│   ├── transformation/
│   │   ├── silver_patients.py
│   │   └── gold_patient_metrics.py
│   ├── quality/
│   │   └── data_quality_report.py
│   └── main.py                     # Runs the full pipeline
├── lakehouse/                      # Generated local output; ignored by Git
├── requirements.txt
└── README.md
```

## How to Run

Activate the Conda environment:

```bash
conda activate healthcare-lakehouse
```

Set PySpark to use the active Python environment:

```bash
export PYSPARK_PYTHON="$(which python)"
export PYSPARK_DRIVER_PYTHON="$(which python)"
```

Run the full pipeline:

```bash
python src/main.py
```

## Sample Results

The sample pipeline processes 11 raw patient records.

- 1 duplicate patient record is removed
- 3 missing-field issues are identified
- 10 cleaned patient records are saved in Silver
- 1 record is marked for review
- Gold metrics summarize valid and review records by state and source system

## Databricks Delta Lake Implementation

The repository includes a Databricks notebook at:

```text
notebooks/healthcare_delta_lakehouse.py
## Future Enhancements


- Orchestrate execution using Azure Data Factory
- Add automated tests and CI/CD using GitHub Actions or Azure DevOps
- Connect Gold metrics to a Power BI dashboard