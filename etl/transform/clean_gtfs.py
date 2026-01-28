import pandas as pd
from pathlib import Path

def unpivot_json(path):
    """Dépivote un JSON Back-on-Track où chaque colonne est un objet."""
    df = pd.read_json(path)
    df_t = df.T  # transposer
    records = []

    for col_name, row in df_t.iterrows():
        rec = row.to_dict()
        rec["_column_key"] = col_name  # garder l'identifiant original
        records.append(rec)

    return pd.DataFrame(records)


def clean_gtfs_json(
    gtfs_dir="data/raw/backontrack/gtfs/night-train-data-main/data/latest",
    output_path="data/processed/backontrack_clean.csv"
):
    gtfs_dir = Path(gtfs_dir)

    # 1. Dépivotage des quatre fichiers pivotés
    trip_stop = unpivot_json(gtfs_dir / "trip_stop.json")
    stops = unpivot_json(gtfs_dir / "stops.json")
    trips = unpivot_json(gtfs_dir / "trips.json")
    routes = unpivot_json(gtfs_dir / "routes.json")

    # 2. Fusion trip_stop + stops
    trip_stop = trip_stop.merge(stops, on="stop_id", how="left", suffixes=("", "_stop"))

    # Supprimer les lignes sans trip_id ou stop_id
    trip_stop = trip_stop.dropna(subset=["trip_id", "stop_id"])

    # 3. Fusion avec trips
    trip_stop = trip_stop.merge(trips, on="trip_id", how="left", suffixes=("", "_trip"))

    # 4. Fusion avec routes
    trip_stop = trip_stop.merge(routes, on="route_id", how="left", suffixes=("", "_route"))

# Colonnes techniques à supprimer
    cols_to_drop = [
        "_column_key", "_column_key_stop", "_column_key_trip", "_column_key_route",
        "Unnamed: 52",
        "Merged Doc ID - ONTD_Chatbase_Export",
        "Merged Doc URL - ONTD_Chatbase_Export",
        "Link to merged Doc - ONTD_Chatbase_Export",
        "Document Merge Status - ONTD_Chatbase_Export"
    ]

    trip_stop = trip_stop.drop(columns=[c for c in cols_to_drop if c in trip_stop.columns])


    # 5. Trier proprement
    trip_stop = trip_stop.sort_values(["trip_id", "stop_sequence"])

    # 6. Sauvegarde
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    trip_stop.to_csv(output_path, index=False)

    print(f"Transformation GTFS JSON terminée → {output_path}")
    return output_path
