from azure.cosmos import ContainerProxy, DatabaseProxy
from graphrag.index.config.storage import PipelineBlobStorageConfig, PipelineFileStorageConfig, PipelineMemoryStorageConfig
from graphrag.index.storage.load_storage import load_storage as graphrag_load_storage
from graphrag.index.storage import  PipelineStorage

from ..config.cosmos_storage_config import CosmosDBStorageConfig
from .cosmos_storage import CosmosDBStorage

__CLIENT_CACHE = {}

def client_factory(container_name:str, db:DatabaseProxy):
    global __CLIENT_CACHE
    key = f"{db.id}-{container_name}"
    if key in __CLIENT_CACHE:
        return __CLIENT_CACHE[key]
    else:
        client = db.get_container_client(container_name)
        __CLIENT_CACHE[key] = client
        return client


PipelineStorageConfigTypes = (
    PipelineFileStorageConfig | PipelineMemoryStorageConfig | PipelineBlobStorageConfig | CosmosDBStorageConfig
)


def create_storage(config: PipelineStorageConfigTypes) -> PipelineStorage:
    if isinstance(config, CosmosDBStorageConfig):
        return CosmosDBStorage(config)
    else: 
        return graphrag_load_storage(config)