from pydantic import BaseModel, Field
from enum import Enum

from graphrag.config.models.storage_config import StorageConfig as GraphRagStorageConfig
import graphrag.config.defaults as defs

class StorageType(str, Enum):
    """The storage type for the pipeline."""

    file = "file"
    """The file storage type."""
    memory = "memory"
    """The memory storage type."""
    blob = "blob"
    """The blob storage type."""
    cosmos = "cosmos"
    """The cosmos storage type."""

    def __repr__(self):
        """Get a string representation."""
        return f'"{self.value}"'


class StorageConfig(GraphRagStorageConfig, extra='allow'):
    """The default configuration section for Storage."""

    type: StorageType = Field(
        description="The storage type to use.", default=StorageType.cosmos
    )
    base_dir: str = Field(
        description="The base directory for the storage.",
        default=defs.STORAGE_BASE_DIR,
    )
    connection_string: str | None = Field(
        description="The storage connection string to use.", default=None
    )
    container_name: str | None = Field(
        description="The storage container name to use.", default=None
    )
    storage_account_blob_url: str | None = Field(
        description="The storage account blob url to use.", default=None
    )
    database_name:str | None   = Field(
        description="The name of the CosmosDB Database.", default=None
    )
    """The name of the CosmosDB Database."""
    account_host:str | None = Field(
        description="The account host for the CosmosDB (if not using a connection string).", default=None
    )
    """The account host for the CosmosDB."""
    account_key:str | None = Field(
        description="The account key for the CosmosDB (if not using AccessKeys - alternatives are a Connection String or ManagedIdentity).", default=None
    )
    """The account key for the CosmosDB."""
