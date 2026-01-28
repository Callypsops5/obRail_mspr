import zipfile
from pathlib import Path
import pandas as pd

def extract_mobilitydb(
    zip_path="data/raw/tdg-83582-202601270046.zip",
    output_dir="data/raw/mobilitydb"
):
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extraction
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(output_dir)

    # Identifier l’opérateur
    agency_file = output_dir / "agency.txt"
    if agency_file.exists():
        agency = pd.read_csv(agency_file)
        print("Opérateur détecté :", agency["agency_name"].iloc[0])
    else:
        print("Impossible de détecter l’opérateur (agency.txt manquant).")

    print("Extraction MobilityDB terminée.")
