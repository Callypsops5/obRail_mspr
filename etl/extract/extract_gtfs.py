import zipfile
from pathlib import Path

def extract_gtfs(zip_path, output_dir="data/raw/backontrack/gtfs"):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(output_dir)
    print(f"GTFS extrait dans : {output_dir}")
