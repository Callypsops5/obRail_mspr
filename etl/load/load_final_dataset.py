import pandas as pd
from pathlib import Path

def load_final_dataset(
    merged_path="data/processed/merged/eurostat_transport.csv",
    output_path="data/final/rail_passengers_dataset.csv"
):
    Path("data/final").mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(merged_path)

    # Derniers nettoyages
    df = df.sort_values(["country", "year"])

    df.to_csv(output_path, index=False)

    print(f"Dataset final prêt → {output_path}")
    return output_path
