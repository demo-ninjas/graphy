#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import azure.cosmos.cosmos_client as cosmos_client
from azure.identity import DefaultAzureCredential

from azure.cosmos import ContainerProxy, DatabaseProxy
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient

from graphy.dataaccess import client_factory
from graphy.data import Entity

from tqdm import tqdm
import dotenv
dotenv.load_dotenv(".env")


async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: push_entities_to_search_index")
        print("")
        print("Options:")
        return

    ## Load CosmosDB Client
    cosmos_database = os.environ.get("COSMOS_DATABASE_ID", "cardiology-canon")
    cosmos_connection_str = os.environ.get("COSMOS_CONNECTION_STRING")
    cosmos_account = None
    if not cosmos_connection_str:
        ## Get Cosmos Account and use Managed Identity
        cosmos_account = os.environ.get("COSMOS_ACCOUNT") or os.environ.get("COSMOS_ACCOUNT_NAME")
        cosmos_key = os.environ.get("COSMOS_KEY")
        if not cosmos_account:
            raise ValueError("COSMOS_ACCOUNT or COSMOS_ACCOUNT_NAME must be set in the environment when not using a connection string")
        if cosmos_account and cosmos_key:
            if cosmos_account.startswith("https://"):
                cosmos_connection_str = f"AccountEndpoint={cosmos_account};AccountKey={cosmos_key};"
            else:
                cosmos_connection_str = f"AccountEndpoint=https://{cosmos_account}.documents.azure.com:443/;AccountKey={cosmos_key};"
        
    if cosmos_connection_str: 
        client = cosmos_client.CosmosClient.from_connection_string(cosmos_connection_str)
    else:
        if cosmos_account.startswith("https://"):
            client = cosmos_client.CosmosClient(url=cosmos_account, credential=DefaultAzureCredential())
        else:
            client = cosmos_client.CosmosClient(url=f"https://{cosmos_account}.documents.azure.com:443/", credential=DefaultAzureCredential())
    
    db = client.get_database_client(cosmos_database)

    entity_container = "entities"
    entity_metadata_container = "entity_metadata"    

    ## Load the Search Client
    service_endpoint = os.environ.get("SEARCH_API_ENDPOINT", None)
    index_name = os.environ.get("SEARCH_INDEX_NAME", None)
    key = os.environ.get("SEARCH_API_KEY", None)
    if '--index' in args:
        index_name = args['--index']

    print(f"Publishing to Index: {index_name}")
    if index_name is None or len(index_name) == 0:
        print("Please provide the Azure Search Index name using the environment variable AZURE_SEARCH_INDEX_NAME or the --index flag.")
        return
    if service_endpoint is None or len(service_endpoint) == 0:
        print("Please provide the Azure Search Service Endpoint using the environment variable AZURE_SEARCH_SERVICE_ENDPOINT.")
        return
    # if key is None or len(key) == 0:
    #     print("Please provide the Azure Search Admin API Key using the environment variable SEARCH_API_KEY.")
    #     return

    creds = AzureKeyCredential(key) if key is not None else DefaultAzureCredential()
    search_client = SearchClient(service_endpoint, index_name, creds)

    ## Create Worker Pool
    pool = ThreadPoolExecutor(max_workers=8)

    entity_client = db.get_container_client(entity_container)
    
    ## Load a list of entity ids from entity client
    query = "SELECT c.id FROM c"
    result = entity_client.query_items(query=query, enable_cross_partition_query=True)
    result = list(result)
    entity_ids = [r.get("id") for r in result]

    ## Iterate data in dataframe
    pbar = tqdm(total=len(entity_ids), desc=f"Publishing Entities")
    futures = []
    failures = []
    uploaded_count = 0
    publish_batch = []
    for entity_id in entity_ids:
        futures.append(pool.submit(_publish_entity, entity_id, db))
        if len(futures) > 1000:
            for future in futures:
                result, id, msg = future.result()
                pbar.update(1)
                if not result:
                    failures.append({ "id": id, "message": msg })
                else: 
                    publish_batch.append(result)
                if len(publish_batch) > 500:
                    search_client.upload_documents(publish_batch)
                    uploaded_count += len(publish_batch)
                    publish_batch = []
            futures = []
    
    if len(futures) > 0:
        for future in futures:
            result, id, msg = future.result()
            pbar.update(1)
            if not result:
                failures.append({ "id": id, "message": msg })
            else: 
                publish_batch.append(result)
            if len(publish_batch) > 500:
                search_client.upload_documents(publish_batch)
                uploaded_count += len(publish_batch)
                publish_batch = []
        
        if len(publish_batch) > 0:
            search_client.upload_documents(publish_batch)
            uploaded_count += len(publish_batch)

    pbar.close()

    if len(failures) == 0:
        print("All Entities published successfully (" + str(uploaded_count) + " entities).")
    else:
        print(f"{len(failures)} entities failed to publish.")
        print("Failures:")
        for failure in failures:
            print(f" - {failure.get('id')} - {failure.get('message')}")

def _publish_entity(entity_id: str, db:DatabaseProxy) -> tuple[dict, str, str]:
    try:
        entity = Entity.load(entity_id, db, include_metadata=True)
        record = {
            "id": entity.id,
            "uid": entity.uid,
            "title": entity.title,
            "type": entity.type,
            "description": entity.description,
            "communities": entity.community_ids,
            "sources": entity.sources,
            "description_embedding": entity.description_embedding
        }
        return (record, entity_id, None)
    except Exception as e:
        return (None, entity_id, str(e))

def _parse_args() -> dict[str, str]:
    args = sys.argv[1:]
    if len(args) == 0:
        return {}
    res = {}
    non_flag_args = []
    
    for arg in args: 
        if arg.startswith("--"):
            arr = arg.split("=")
            key = arr[0]
            value = arr[1] if len(arr) > 1 else True
            res[key] = value
        else:
            non_flag_args.append(arg)
    
    if len(non_flag_args) > 0:
        for i in range(0, len(non_flag_args)):
            arg = non_flag_args[i]
            res[i] = arg
    
    return res

def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
if __name__ ==  '__main__':
    run_main()