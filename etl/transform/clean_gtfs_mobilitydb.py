import pandas as pd
from pathlib import Path

def clean_gtfs_mobilitydb(
    gtfs_dir="data/raw/mobilitydb",
    output_path="data/processed/mobilitydb_clean.csv"
):
    gtfs_dir = Path(gtfs_dir)

    # Charger les fichiers GTFS standard
    stops = pd.read_csv(gtfs_dir / "stops.txt")
    stop_times = pd.read_csv(gtfs_dir / "stop_times.txt")
    trips = pd.read_csv(gtfs_dir / "trips.txt")
    routes = pd.read_csv(gtfs_dir / "routes.txt")

    # Filtrer les trains longue distance
    # GTFS route_type :
    # 2 = Intercity Rail
    # 3 = Long-distance Rail
    long_distance_routes = routes[routes["route_type"].isin([2, 3])]

    # Garder uniquement les trips associés
    trips = trips[trips["route_id"].isin(long_distance_routes["route_id"])]

    # Fusion stop_times + trips
    merged = stop_times.merge(trips, on="trip_id", how="left")

    # Fusion avec routes
    merged = merged.merge(routes, on="route_id", how="left", suffixes=("", "_route"))

    # Fusion avec stops
    merged = merged.merge(stops, on="stop_id", how="left", suffixes=("", "_stop"))

    # Trier proprement
    merged = merged.sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)

    # Sauvegarde
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)

    print(f"MobilityDB GTFS nettoyé → {output_path}")
    return output_path
