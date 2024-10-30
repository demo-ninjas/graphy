from typing import Generic, Literal, TypeVar
from graphrag.index.config.storage import PipelineStorageConfig
from pydantic import BaseModel
from pydantic import Field as pydantic_Field

class CosmosDBStorageConfig(PipelineStorageConfig[Literal['cosmos']]):
    type: Literal['cosmos'] = 'cosmos'

    database_name: str | None = pydantic_Field(
        description="The name of the CosmosDB Database.", default=None
    )
    """The name of the CosmosDB Database."""

    connection_string: str | None = pydantic_Field(
        description="The connection string for the CosmosDB.", default=None
    )
    """The connection string for the CosmosDB."""

    account_host: str | None = pydantic_Field(
        description="The account host for the CosmosDB (if not using a connection string).", default=None
    )
    """The account host for the CosmosDB."""

    account_key: str | None = pydantic_Field(
        description="The account key for the CosmosDB (if not using AccessKeys - alternatives are a Connection String or ManagedIdentity).", default=None
    )
    """The account key for the CosmosDB."""

