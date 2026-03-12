from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any
from data_utils.log_config import logs_config
logger = logs_config()

class BaseSyntheticGenerator(ABC):
    """Classe base abstrata para geradores de dados sintéticos"""
    
    def __init__(self, profile: Dict[str, Any], output_dir: Path):
        self.profile = profile
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def generate(self, n_rows: int, output_filename: str) -> Path:
        """
        Gera dados sintéticos.
        
        Args:
            n_rows: Número de linhas a gerar
            output_filename: Nome do arquivo de saída
        
        Returns:
            Path do arquivo gerado
        """
        pass
    
    @abstractmethod
    def get_engine_name(self) -> str:
        """Retorna nome do engine (polars, spark, etc.)"""
        pass
    
    def should_switch_engine(self, n_rows: int, threshold: int) -> bool:
        """
        Verifica se deve trocar de engine baseado no número de linhas.
        
        Args:
            n_rows: Número de linhas a gerar
            threshold: Limite para troca
        
        Returns:
            True se deve trocar
        """
        return n_rows > threshold