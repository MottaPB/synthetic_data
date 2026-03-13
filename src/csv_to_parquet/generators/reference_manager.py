import polars as pl
from pathlib import Path
from typing import List, Optional
from datetime import date
from data_utils.log_config import logs_config
logger = logs_config()

class ReferenceDataManager:
    """Manages reference data to maintain referential integrity"""
    
    def __init__(self, data_dir: Path, synthetic_dir: Path, cache_days: int = 7):
        self.data_dir = data_dir
        self.synthetic_dir = synthetic_dir
        self.cache_days = cache_days
        self._cache = {}
    
    def get_reference_keys(
        self, 
        dataset_name: str, 
        key_column: str,
        target_date: Optional[date] = None
    ) -> List[str]:
        """
        Returns a list of valid keys from a reference dataset.
        PRIORITIZES recent synthetic data over historical.
        """
        cache_key = f"{dataset_name}_{key_column}"
        
        # Check cache first
        if cache_key in self._cache:
            logger.debug(f"Using cached keys for {cache_key}")
            return self._cache[cache_key]
        
        logger.info(f"Loading reference keys: {dataset_name}.{key_column}")
        
        all_keys = []
        
        # 1. PRIORITIZE recent synthetic data (within cache_days)
        synthetic_files = list(self.synthetic_dir.glob(f"{dataset_name}_synthetic_*.parquet"))
        
        if synthetic_files:
            logger.info(f"Found {len(synthetic_files)} synthetic files for {dataset_name}")
            
            for file in synthetic_files:
                try:
                    df = pl.read_parquet(file, columns=[key_column])
                    keys = df[key_column].drop_nulls().unique().to_list()
                    all_keys.extend(keys)
                    logger.info(f"  Loaded {len(keys):,} keys from {file.name}")
                except Exception as e:
                    logger.warning(f"Error reading {file}: {e}")
        
        # 2. If no keys found in recent synthetic data, fallback to historical data
        if not all_keys:
            logger.info(f"No synthetic data found, loading from historical data")
            historical_files = list(self.data_dir.glob(f"{dataset_name}*.parquet"))
            
            for file in historical_files:
                try:
                    df = pl.read_parquet(file, columns=[key_column])
                    keys = df[key_column].drop_nulls().unique().to_list()
                    all_keys.extend(keys)
                except Exception as e:
                    logger.warning(f"Error reading {file}: {e}")
        
        # Deduplicate keys
        unique_keys = list(set(all_keys))
        
        if not unique_keys:
            raise ValueError(
                f"No keys found for {dataset_name}.{key_column}. "
                f"Check if data exists in {self.data_dir} or {self.synthetic_dir}"
            )
        
        logger.info(f"Loaded {len(unique_keys):,} unique keys for {dataset_name}.{key_column}")
        
        # Cache the keys for future use
        self._cache[cache_key] = unique_keys
        
        return unique_keys
    
    def sample_keys(
        self, 
        keys: List[str], 
        n: int, 
        strategy: str = "random"
    ) -> List[str]:
        """
        Samples keys from a list.

        Args:
            keys: List of available keys to sample from
            n: Number of keys to sample
            strategy: 'random' or 'recent'
        
        Returns:
            List of sampled keys
        """
        import random
        
        if not keys:
            raise ValueError("No keys available to sample")
        
        if n > len(keys):
            logger.warning(
                f"Requested {n} keys but only {len(keys)} available. "
                f"Sampling with replacement."
            )
            return random.choices(keys, k=n)
        
        if strategy == "random":
            return random.sample(keys, n)
        elif strategy == "recent":
            # Presume keys are ordered by recency (if loaded from synthetic data first)
            return keys[-n:]
        else:
            return random.sample(keys, n)
    
    def get_related_data(
        self,
        dataset_name: str,
        key_column: str,
        key_values: List[str],
        additional_columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Obtains related data for a list of keys from a reference dataset, including additional columns if specified.
        
        Args:
            dataset_name: Dataset name to search (e.g., 'customers')
            key_column: Key column name to filter by (e.g., 'customer_id')
            key_values: List of key values to search for
            additional_columns: List of additional columns to return
        
        Returns:
            DataFrame with related data
        """
        logger.info(f"Getting related data for {len(key_values)} keys from {dataset_name}")
        
        # Columns to read
        columns = [key_column]
        if additional_columns:
            columns.extend(additional_columns)
        
        # Look for data in recent synthetic files
        historical_files = list(self.data_dir.glob(f"{dataset_name}*.parquet"))
        
        dfs = []
        for file in historical_files:
            df = pl.read_parquet(file, columns=columns)
            df_filtered = df.filter(pl.col(key_column).is_in(key_values))
            if len(df_filtered) > 0:
                dfs.append(df_filtered)
        
        if not dfs:
            raise ValueError(f"No data found for {dataset_name} with provided keys")
        
        result = pl.concat(dfs).unique(subset=[key_column])
        
        logger.info(f"Found {len(result)} matching records")
        
        return result
    
    def clear_cache(self):
        """Clears the reference cache"""
        self._cache.clear()
        logger.info("Reference cache cleared")