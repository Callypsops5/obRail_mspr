import pandas as pd
import gzip
import shutil
from pathlib import Path
import requests

def extract_eurostat(
    dataset="tran_hv_psmod",
    output_path="data/raw/eurostat_tran_hv_psmod.csv"
):
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # URL officielle SDMX 2.1 (format TSV compressé)
    url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}?format=tsv&compressed=true"

    gz_path = raw_dir / f"{dataset}.tsv.gz"
    tsv_path = raw_dir / f"{dataset}.tsv"

    print(f"Téléchargement depuis : {url}")

    # Télécharger le fichier gz
    r = requests.get(url)
    r.raise_for_status()
    gz_path.write_bytes(r.content)

    print(f"Fichier téléchargé : {gz_path}")

    # Décompresser
    with gzip.open(gz_path, "rb") as f_in:
        with open(tsv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Fichier décompressé : {tsv_path}")

    # Charger le TSV
    df = pd.read_csv(tsv_path, sep="\t")

    # Sauvegarder en CSV
    df.to_csv(output_path, index=False)

    print(f"Eurostat dataset '{dataset}' converti → {output_path}")
    return output_path
