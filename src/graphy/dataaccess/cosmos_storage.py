
import os
from azure.cosmos import DatabaseProxy, PartitionKey, ContainerProxy
from azure.cosmos.cosmos_client import CosmosClient
from azure.identity import DefaultAzureCredential

from graphrag.index.storage import PipelineStorage

from ..config.cosmos_storage_config import CosmosDBStorageConfig


class CosmosDBStorage(PipelineStorage):
    def __init__(self, config:CosmosDBStorageConfig) -> None:
        self._db = None
        self._connect(config)

    async def get(self, key: str, as_bytes: bool | None = None, encoding: str | None = None) -> any:
        arr = key.split("/")
        container_name = arr[0]
        key = arr[1]
        client = self.get_client(container_name)
        return client.read_item(key, key)
        

    async def set(self, key: str, value: str | bytes | None, encoding: str | None = None) -> None:
        arr = key.split("/")
        container_name = arr[0]
        key = arr[1]
        client = self.get_client(container_name)
        client.upsert_item(value)

    async def has(self, key: str) -> bool:
        arr = key.split("/")
        container_name = arr[0]
        key = arr[1]
        client = self.get_client(container_name)
        return client.read_item(key, key) is not None

    async def delete(self, key: str) -> None:
        arr = key.split("/")
        container_name = arr[0]
        key = arr[1]
        client = self.get_client(container_name)
        client.delete_item(key, key)

    async def clear(self) -> None:
        for container_name in self._container_clients:
            client = self.get_client(container_name)
            items = client.read_all_items()
            for item in items:
                client.delete_item(item['id'], item['id'])

    def child(self, name: str | None) -> "PipelineStorage":
        return self


    def get_client(self, container_name:str) -> ContainerProxy:
        client = None
        if container_name in self._container_clients:
            client = self._container_clients[container_name]
            if client is None: 
                client = self._db.get_container_client(container_name)
                self._container_clients[container_name] = client
        else: 
            client = self._db.create_container(container_name, partition_key=PartitionKey(path='/id'))
            self._container_clients[container_name] = client
        return client

    def _connect(self, config:CosmosDBStorageConfig) -> None:
        if self._db is not None:
            return
        
        ## Load CosmosDB Client
        cosmos_database = config.database_name or os.environ.get("GRAPH_DATABASE_ID") or os.environ.get("COSMOS_DATABASE_ID")
        if cosmos_database is None or len(cosmos_database) == 0:
            raise ValueError("Cosmos Database Name is required")
        
        cosmos_connection_str = config.connection_string or os.environ.get("COSMOS_CONNECTION_STRING")
        client = None
        if cosmos_connection_str is not None and len(cosmos_connection_str) > 0:
            client = CosmosClient.from_connection_string(cosmos_connection_str)
        else: 
            # .documents.azure.com:443/
            cosmos_account_host = config.account_host or os.environ.get("COSMOS_ACCOUNT_HOST")
            if cosmos_account_host is None or len(cosmos_account_host) == 0:
                raise ValueError("Cosmos Account Host is required (when not using a connection string)")
            cosmos_account_key = config.account_key or os.environ.get("COSMOS_ACCOUNT_KEY")
            if cosmos_account_key is not None and len(cosmos_account_key) > 0:
                client = CosmosClient(cosmos_account_host, {'masterKey': cosmos_account_key})
            else: 
                client = CosmosClient(url=cosmos_account_host, credential=DefaultAzureCredential())

        self._db = client.get_database_client(cosmos_database)

        containers = self._db.list_containers()
        container_map = dict[str, DatabaseProxy|None ]()
        for container_props in containers:
            container_map[container_props['name']] = None
        self._container_clients = container_map
