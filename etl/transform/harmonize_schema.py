import pandas as pd
from pathlib import Path

def harmonize_schema(
    input_folder="data/raw/transport_gouv/extracted",
    output_folder="data/processed/transport_gouv"
):
    input_folder = Path(input_folder)
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"Harmonisation du schéma GTFS depuis : {input_folder}")

    for file in input_folder.glob("*.txt"):
        print(f"→ Traitement : {file.name}")

        df = pd.read_csv(file, sep=",", dtype=str)

        # Nettoyage générique
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Export en CSV
        output_path = output_folder / (file.stem + ".csv")
        df.to_csv(output_path, index=False)

        print(f"   ✔ Exporté : {output_path}")

    print("Harmonisation terminée.")
    return output_folder
