import polars as pl
from pathlib import Path

from data_utils.log_config import logs_config

logger = logs_config()


def load_data(data_path: str) -> pl.DataFrame:
    """Load the data from the specified directory."""
    dataset = pl.read_parquet(data_path)
    # Load the datasets
    return dataset


def basic_eda(df: pl.DataFrame) -> None:
    """Perform basic exploratory data analysis."""
    print("=" * 50)
    print("Basic EDA Results")
    print("=" * 50)

    # Shape
    logger.info(f"DataFrame Shape: {df.shape[0]} rows and {df.shape[1]} columns")
    print(f"\nDataFrame Shape: {df.shape[0]} rows and {df.shape[1]} columns")

    # Schema
    logger.info(f"Schema: {df.schema}")
    print("\nSchema:")
    print(df.schema)

    print("\nFirst 5 rows:")
    print(df.head(20))

    print("\nDataFrame Description:")
    print(df.describe())

    print("\nNull Values by Column:")
    null_counts = df.null_count()
    print(null_counts)

    print("\nUnique Values per Column:")
    for col in df.columns:
        unique_count = df[col].n_unique()
        print(f"{col}: {unique_count} unique values")


def main():
    # Paths
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    input_dir = PROJECT_ROOT / "data" / "synthetic"
    filename = "olist_customers_synthetic_20260312_154201.parquet"
    input_file = input_dir / filename

    logger.info("=" * 50)
    logger.info("Basic EDA Results")
    logger.info("=" * 50)

    logger.info(f"Dataset: {filename}")
    # Pipeline
    print("Loading data...")
    df = load_data(input_file)

    print(f"Performing basic EDA on {input_file.name}...")
    basic_eda(df)

    print("\n✅ Finished!")


if __name__ == "__main__":
    main()