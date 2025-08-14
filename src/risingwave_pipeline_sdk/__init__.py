"""RisingWave Pipeline SDK

A Python SDK for creating and managing RisingWave data pipelines.
"""

from .client import RisingWaveClient
from .pipelines.postgres_cdc import PostgresCDCPipeline, PostgresCDCConfig
from .models import Source, Table, MaterializedView

__all__ = [
    "RisingWaveClient",
    "PostgresCDCPipeline", 
    "PostgresCDCConfig",
    "Source",
    "Table", 
    "MaterializedView",
]

__version__ = "0.1.0"
