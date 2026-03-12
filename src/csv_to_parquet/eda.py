import polars as pl
from pathlib import Path

def load_data(data_path: str) -> pl.DataFrame:
    """Load the data from the specified directory."""
    # Load the datasets
    return pl.read_parquet(data_path)

def basic_eda(df: pl.DataFrame) -> None:
    """Perform basic exploratory data analysis."""
    print("=" * 50)
    print("Basic EDA Results")
    print("=" * 50)

    #Shape
    print(f"\nDataFrame Shape: {df.shape[0]} rows and {df.shape[1]} columns")

    #Schema
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
    #Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    input_dir = PROJECT_ROOT / "data" / "processed"
    input_file = input_dir / "olist_order_items_dataset.parquet"

    #Pipeline
    print("Loading data...")
    df = load_data(input_file)

    print(f"Performing basic EDA on {input_file.name}...")
    basic_eda(df)

    print("\n✅ Finished!")

if __name__ == "__main__":
    main()