import polars as pl
from pathlib import Path
from typing import List, Optional
import time

def get_csv_files(directory: Path) -> List[Path]:
    """Get a list of all CSV files in the specified directory."""
    return list(directory.glob("*.csv"))

def convert_csv_to_parquet(csv_file: Path, output_dir: Path) -> None:
    """Convert a single CSV file to Parquet format."""
    try:
        print(f"\nProcessing {csv_file.name}...")

        df = pl.read_csv(csv_file)

        output_file = output_dir / (csv_file.stem + ".parquet")

        df.write_parquet(output_file, compression="snappy")

        #Stats
        csv_size = csv_file.stat().st_size / (1024 * 1024)  # Size in MB
        parquet_size = output_file.stat().st_size / (1024 * 1024)  # Time taken
        reduction = ((csv_size - parquet_size) / csv_size) * 100 if csv_size > 0 else 0
        
        print(f"Converted CSV: ({csv_size:.2f} MB) → Parquet: ({parquet_size:.2f} MB)")
        print(f"Size reduction: {reduction:.2f}%")

        print(f"✅ Saved {output_file.name}")
    except Exception as e:
        print(f"\n❌ Error processing {csv_file.name}: {e}\n")

def main():
    PROJECT_ROOT = Path(__file__).parent.parent
    input_dir = PROJECT_ROOT / "data" / "raw"
    output_dir = PROJECT_ROOT / "data" / "processed"

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = get_csv_files(input_dir)

    if not csv_files:
        print("\n❌ No CSV files found in the input directory.")
        return

    print(f"\n📂 Found {len(csv_files)} CSV files. Starting conversion...\n")
    print("=" * 50)

    start_time = time.time()

    for csv_file in csv_files:
        convert_csv_to_parquet(csv_file, output_dir)

    elapsed_time = time.time() - start_time
    print("=" * 50)

    print(f"\n⏱️  Total time taken: {elapsed_time:.2f} seconds")
    print("\n✅ All files processed!")

if __name__ == "__main__":
    main()