"""
Geradores de dados sintéticos
"""
from .base_generator import BaseSyntheticGenerator
from .polars_generator import PolarsGenerator
from .reference_manager import ReferenceDataManager
__all__ = [
    'BaseSyntheticGenerator',
    'PolarsGenerator',
    'ReferenceDataManager',
]