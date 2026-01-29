import zipfile
from pathlib import Path

def extract_transport_gouv_zip(
    zip_path="data/raw/transport_gouv/transport-data-gouv.zip",
    output_folder="data/raw/transport_gouv/extracted"
):
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"Extraction du ZIP : {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(output_folder)

    print(f"Fichiers extraits dans : {output_folder}")
    return output_folder
