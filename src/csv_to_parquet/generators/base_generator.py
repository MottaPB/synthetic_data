from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any
from data_utils.log_config import logs_config
logger = logs_config()

class BaseSyntheticGenerator(ABC):
    """Abstract base class for synthetic data generators"""

    def __init__(self, profile: Dict[str, Any], output_dir: Path):
        self.profile = profile
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def generate(self, n_rows: int, output_filename: str) -> Path:
        """
        Create synthetic data and save as Parquet file.
        
        Args:
            n_rows: Row count to generate
            output_filename: Output file name (without extension)
        
        Returns:
            Path for the generated Parquet file
        """
        pass
    
    @abstractmethod
    def get_engine_name(self) -> str:
        """Returns engine name (polars, spark, etc.)"""
        pass
    
    def should_switch_engine(self, n_rows: int, threshold: int) -> bool:
        """
        Verifies if it should switch engines based on the number of rows.
        
        Args:
            n_rows: NNumber of rows to generate
            threshold: Limit for switching engines
        
        Returns:
            True if it should switch engines
        """
        return n_rows > threshold