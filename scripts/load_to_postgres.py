import pandas as pd
import glob
from db import get_engine

def load_csv_to_postgres():
    engine = get_engine()
    files = glob.glob("data/raw/*.csv")

    for file in files:
        df = pd.read_csv(file)
        df.to_sql("stock_data", engine, if_exists="append", index=False)
        print(f"Loaded {file} into PostgreSQL")

if __name__ == "__main__":
    load_csv_to_postgres()