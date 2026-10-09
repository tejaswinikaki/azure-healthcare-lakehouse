from pathlib import Path
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = ROOT_DIR / "data" / "raw" / "patients.csv"


def load_patients():
    return pd.read_csv(DATA_PATH)


def get_summary():
    patients = load_patients()

    return {
        "total_patients": len(patients),
        "unique_patients": patients["patient_id"].nunique(),
        "states": patients["state"].nunique(),
        "missing_values": int(patients.isna().sum().sum()),
    }