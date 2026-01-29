import pandas as pd
from pathlib import Path

def clean_eurostat(
    input_path="data/raw/eurostat_tran_hv_psmod.csv",
    output_path="data/processed/eurostat_clean.csv"
):
    Path("data/processed").mkdir(parents=True, exist_ok=True)

    print(f"Chargement du fichier : {input_path}")
    df = pd.read_csv(input_path)

    # La première colonne contient 4 dimensions fusionnées
    dim_col = df.columns[0]

    # Séparer les dimensions
    dims = df[dim_col].str.split(",", expand=True)
    dims.columns = ["freq", "unit", "vehicle", "geo"]

    # Fusionner avec les colonnes d'années
    df = pd.concat([dims, df.drop(columns=[dim_col])], axis=1)

    # Transformer les années en lignes
    df = df.melt(
        id_vars=["freq", "unit", "vehicle", "geo"],
        var_name="year",
        value_name="value"
    )

    # Nettoyer les colonnes
    df["year"] = df["year"].str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Filtrer PKM + RAIL
    df = df[
        (df["unit"] == "PKM") &
        (df["vehicle"] == "RAIL")
    ]

    # Supprimer les lignes sans valeur
    df = df.dropna(subset=["value"])

    # Renommer pour harmonisation
    df = df.rename(columns={
        "geo": "country",
        "value": "passenger_km"
    })

    # Export
    df.to_csv(output_path, index=False)

    print(f"Fichier nettoyé → {output_path}")
    return output_path
