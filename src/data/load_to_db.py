from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Conexión a Postgres (misma que Docker)
DATABASE_URL = "postgresql+psycopg2://analytics:analytics@localhost:5433/retail"

RAW_DIR = Path("data/raw")
TABLE_SCHEMA = "staging"
TABLE_NAME = "online_retail"


def find_input_file() -> Path:
    """
    Busca el primer .xlsx o .csv en data/raw.
    Ajusta si quieres fijar un nombre específico.
    """
    candidates = list(RAW_DIR.glob("*.xlsx")) + list(RAW_DIR.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No .xlsx/.csv files found in {RAW_DIR}")
    # Tomamos el primero por simplicidad
    return candidates[0]


def load_raw_data(path: Path) -> pd.DataFrame:
    print(f"📖 Loading data from {path}")
    if path.suffix.lower() == ".xlsx":
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza columnas al esquema de staging.online_retail.
    Algunos datasets usan 'Invoice', otros 'InvoiceNo', etc.
    """
    col_map = {
        "Invoice": "InvoiceNo",
        "InvoiceNo": "InvoiceNo",
        "StockCode": "StockCode",
        "Description": "Description",
        "Quantity": "Quantity",
        "InvoiceDate": "InvoiceDate",
        "InvoiceDateTime": "InvoiceDate",
        "Price": "UnitPrice",
        "UnitPrice": "UnitPrice",
        "Customer ID": "CustomerID",
        "CustomerID": "CustomerID",
        "Country": "Country",
    }

    df = df.rename(columns={c: col_map.get(c, c) for c in df.columns})

    # Convertir fecha
    if "InvoiceDate" in df.columns:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    required_cols = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after rename: {missing}")

    df = df[required_cols]

    print("✅ Columns normalized.")
    return df


def load_to_postgres(df: pd.DataFrame):
    engine = create_engine(DATABASE_URL)
    full_table_name = f"{TABLE_SCHEMA}.{TABLE_NAME}"
    print(f"🚀 Loading {len(df)} rows into {full_table_name} ...")

    with engine.begin() as conn:
        df.to_sql(
            TABLE_NAME,
            con=conn,
            schema=TABLE_SCHEMA,
            if_exists="replace",   # 👈 lo que ya pusimos antes
            index=False,
            method="multi",
            chunksize=5000,
        )

        # sanity check usando text()
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE_SCHEMA}.{TABLE_NAME};")
        )
        total_rows = result.scalar()
        print(f"✅ Load complete. Total rows now in {full_table_name}: {total_rows}")


def main():
    path = find_input_file()
    df_raw = load_raw_data(path)
    df_norm = normalize_columns(df_raw)
    load_to_postgres(df_norm)


if __name__ == "__main__":
    main()
