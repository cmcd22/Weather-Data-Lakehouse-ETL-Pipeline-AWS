import pandas as pd
from pathlib import Path

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
INPUT_ROOT = Path("gold_parquet")     # root folder of downloaded S3 data
OUTPUT_CSV = "gold_daily_weather.csv"

# --------------------------------------------------
# Helper: extract partition values from path
# --------------------------------------------------
def extract_partitions(parquet_path: Path):
    """
    Extracts city/year/month/day from paths like:
    city=Auckland/year=2025/month=12/day=11/
    """
    partitions = {}

    for part in parquet_path.parts:
        if "=" in part:
            key, value = part.split("=", 1)
            partitions[key] = value

    return partitions


# --------------------------------------------------
# Main conversion logic
# --------------------------------------------------
def parquet_folder_to_csv(root: Path):
    dataframes = []

    parquet_files = list(root.rglob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files")

    if not parquet_files:
        raise RuntimeError("No parquet files found!")

    for parquet_file in parquet_files:
        # Read parquet
        df = pd.read_parquet(parquet_file)

        # Extract partition metadata
        partitions = extract_partitions(parquet_file)

        # Add partition columns
        for key, value in partitions.items():
            df[key] = value

        dataframes.append(df)

    # Combine all files into one DataFrame
    final_df = pd.concat(dataframes, ignore_index=True)

    # Write CSV
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"CSV written to {OUTPUT_CSV}")

    return final_df


if __name__ == "__main__":
    parquet_folder_to_csv(INPUT_ROOT)
