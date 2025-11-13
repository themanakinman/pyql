"""
PyQL - A Custom SQL-ish Data Processing Engine
"""

__version__ = "0.1.0"
__author__ = "Jhene Ekuwem"

from .parser import CSVParser
from .dataframe import DataFrame
from .filters import BooleanMask, compare

__all__ = [
    'CSVParser',
    'DataFrame',
    'BooleanMask',
    'compare'
]
