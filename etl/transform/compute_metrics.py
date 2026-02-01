import pandas as pd
from pathlib import Path

def compute_metrics(
    folder="data/processed/transport_gouv",
    output_path="data/processed/merged/gtfs_metrics.csv"
):
    folder = Path(folder)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    print(f"Calcul des métriques GTFS depuis : {folder}")

    # Charger les tables essentielles
    stops = pd.read_csv(folder / "stops.csv")
    routes = pd.read_csv(folder / "routes.csv")
    trips = pd.read_csv(folder / "trips.csv")
    stop_times = pd.read_csv(folder / "stop_times.csv")

    # Mécanique de base
    metrics = {
        "nb_stops": len(stops),
        "nb_routes": len(routes),
        "nb_trips": len(trips),
        "nb_stop_times": len(stop_times),
    }

    # Fréquence moyenne par route
    freq = (
        trips.groupby("route_id")
        .size()
        .reset_index(name="nb_trips")
    )

    freq.to_csv(Path(output_path).parent / "gtfs_frequency_by_route.csv", index=False)

    # Export des métriques globales
    pd.DataFrame([metrics]).to_csv(output_path, index=False)

    print(f"✔ Métriques globales → {output_path}")
    print(f"✔ Fréquences par route → {Path(output_path).parent / 'gtfs_frequency_by_route.csv'}")

    return output_path
