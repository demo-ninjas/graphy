import os
import pandas as pd
from graphrag.index.emit import TableEmitter
from graphrag.index.storage import PipelineStorage
from graphrag.index.typing import ErrorHandlerFn

from azure.cosmos import DatabaseProxy, ContainerProxy
from azure.cosmos.cosmos_client import CosmosClient
from azure.identity import DefaultAzureCredential

from graphy.dataaccess.cosmos_storage import CosmosDBStorage

class CosmosEmitter(TableEmitter):
    """CosmosEmitter protocol for emitting tables to a destination."""

    def __init__(self, storage: CosmosDBStorage, on_error: ErrorHandlerFn) -> None:
        self.storage = storage
        self.on_error = on_error

        
    async def emit(self, name: str, data: pd.DataFrame) -> None:
        """Emit a dataframe to CosmosDB."""
        client:ContainerProxy = await self.storage.get_client('_' + name)      ## We'll put all the temp data into a container with a name prefixed by an underscore
        ## Emit the data
        print(f"Emitting {name} to CosmosDB [Rows: {len(data)}]")
        for i, row in data.iterrows():
            await client.upsert_item(row.to_dict())

      