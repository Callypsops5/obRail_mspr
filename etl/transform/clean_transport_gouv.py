import pandas as pd
from pathlib import Path

def clean_transport_gouv(
    input_folder="data/raw/transport_gouv/extracted",
    output_folder="data/processed/transport_gouv"
):
    input_folder = Path(input_folder)
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"Nettoyage du dossier : {input_folder}")

    for file in input_folder.glob("*.csv"):
        print(f"→ Nettoyage du fichier : {file.name}")

        # Lecture robuste
        try:
            df = pd.read_csv(file, sep=None, engine="python")
        except Exception:
            df = pd.read_csv(file, sep=";", engine="python", encoding="latin-1")

        # Nettoyage générique
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Conversion automatique des nombres
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

        # Export
        output_path = output_folder / file.name
        df.to_csv(output_path, index=False)

        print(f"   ✔ Fichier nettoyé → {output_path}")

    print("Nettoyage terminé.")
    return output_folder
