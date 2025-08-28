import pandas as pd


def print_df(df: pd.DataFrame, title: str = ""):
    if title:
        print(f"\n=== {title} ===")
    if df.empty:
        print("(sin filas)")
    else:
        print(df.head(10))
        print(f"\nFilas: {len(df):,}")
