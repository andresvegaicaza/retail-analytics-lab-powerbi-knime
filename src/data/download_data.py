from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

# Kaggle dataset slug (ajusta si usas otro dataset)
DATASET = "mashlyn/online-retail-ii-uci"

RAW_DIR = Path("data/raw")


def download_online_retail():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    print(f"📥 Downloading dataset '{DATASET}' to {RAW_DIR} ...")
    api.dataset_download_files(DATASET, path=str(RAW_DIR), unzip=True)
    print("✅ Download complete and unzipped.")

    files = list(RAW_DIR.glob("*.xlsx")) + list(RAW_DIR.glob("*.csv"))
    if files:
        print("Found data files:")
        for f in files:
            print(" -", f)
    else:
        print("⚠️ No .xlsx or .csv files found. Check the dataset contents.")


if __name__ == "__main__":
    download_online_retail()
