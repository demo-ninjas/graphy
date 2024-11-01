#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys
from tqdm import tqdm
import dotenv
dotenv.load_dotenv(".env")

import azure.cosmos.cosmos_client as cosmos_client
from azure.identity import DefaultAzureCredential
from azure.cosmos import ContainerProxy, DatabaseProxy
from azure.cosmos.errors import CosmosResourceNotFoundError
from azure.identity import DefaultAzureCredential

from concurrent.futures import ThreadPoolExecutor
from typing import Hashable, List, Tuple
from pandas.core.series import Series

from graphy.dataaccess import client_factory
from graphy.data import Entity, Relationship, TextUnit, Community, Document, COMMUNITY_CONTAINER_NAME, ENTITY_CONTAINER_NAME, ENTITY_METADATA_CONTAINER_NAME, RELATIONSHIP_CONTAINER_NAME, TEXT_UNIT_CONTAINER_NAME, DOCUMENT_CONTAINER_NAME

async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if '--help' in args:
        print("Usage: python publish_graph.py [--run=<run_id>] [--entities] [--relationships] [--covariates] [--community-reports] [--text-units]")

        print("Options:")
        print("\t--run=<run_id>\tSpecify a run_id to load the data from (default: latest)")
        print("\t--entities\tPublish the entities to the CosmosDB")
        print("\t--relationships\tPublish the relationships to the CosmosDB")
        print("\t--covariates\tPublish the covariates to the CosmosDB")
        print("\t--community-reports\tPublish the community reports to the CosmosDB")
        print("\t--text-units\tPublish the text units to the CosmosDB")
        print("\t--community-weights\tBuild + Publish the community weights to the CosmosDB")
        print("\t--documents \tPublish the documents to the CosmosDB")
        print("\t--all\tPublish all data to the CosmosDB")
        print("\t--force\tForce the re-publishing of all data")
        exit()
    
    INPUT_DIR = None
    if "--run" in args:
        run_id = args["--run"]
        INPUT_DIR = f"output/{run_id}/artifacts"
    else: 
        INPUT_DIR = _infer_data_dir(".")

    LANCEDB_URI = f"lancedb"
    COMMUNITY_REPORT_TABLE = "create_final_community_reports"
    COMMUNITY_TABLE = "create_final_communities"
    ENTITY_TABLE = "create_final_nodes"
    ENTITY_EMBEDDING_TABLE = "create_final_entities"
    RELATIONSHIP_TABLE = "create_final_relationships"
    COVARIATE_TABLE = "create_final_covariates"
    TEXT_UNIT_TABLE = "create_final_text_units"
    COMMUNITY_LEVEL = 2

    data_path = Path(INPUT_DIR)
    print("Using Data Path: ", data_path)
    
    is_all = '--all' in args

    combined_entities = None
    if is_all or '--entities' in args:
        print(f"Loading Entity Table...")
        final_entities = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_TABLE}.parquet")
        print("Loading Embedding Table...")
        final_embeddings = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_EMBEDDING_TABLE}.parquet")
        print("Merging Entity Tables...")
        combined_entities = pd.merge(final_entities, final_embeddings, on="id", how="left")
        final_entities = None
        final_embeddings = None    

    final_text_units = None
    if is_all or '--text-units' in args:
        print("Loading Text Unit Table...")
        final_text_units = pd.read_parquet(f"{data_path.as_posix()}/{TEXT_UNIT_TABLE}.parquet")
    
    final_relationships = None
    if is_all or '--relationships' in args:
        print("Loading Relationship Table...")
        final_relationships = pd.read_parquet(f"{data_path.as_posix()}/{RELATIONSHIP_TABLE}.parquet")
    
    final_covariates = None
    if is_all or '--covariates' in args or '--entities' in args or '--text-units' in args:
        print("Loading Covariate Table...")
        if Path(f"{data_path.as_posix()}/{COVARIATE_TABLE}.parquet").exists():
            final_covariates = pd.read_parquet(f"{data_path.as_posix()}/{COVARIATE_TABLE}.parquet")

    final_community_reports = None
    final_communities = None
    if is_all or '--community-reports' in args:
        print("Loading Community Report Table...")
        final_community_reports = pd.read_parquet(f"{data_path.as_posix()}/{COMMUNITY_REPORT_TABLE}.parquet")
        final_communities = pd.read_parquet(f"{data_path.as_posix()}/{COMMUNITY_TABLE}.parquet")

    
    final_documents = None
    if is_all or '--documents' in args:
        print("Loading Document Table...")
        final_documents = pd.read_parquet(f"{data_path.as_posix()}/create_final_documents.parquet")

    ## Load CosmosDB Client
    ## Load CosmosDB Client
    cosmos_database = os.environ.get("COSMOS_DATABASE_ID", "graph-database")
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


    ## Create Worker Pool
    pool = ThreadPoolExecutor(max_workers=50)

    skip_existing = '--force' not in args

    force_ids = []
    if '--force-ids' in args:
        force_ids = args['--force-ids'].split(',')

    if '--ensure-entities' in args:
        ensure_entities(pool, db)
        
    if '--refresh-entities' in args:
        # print("Refreshing Entities...")
        refresh_entities(pool, db)

    if '--refresh-communities' in args:
        # print("Refreshing Communities...")
        refresh_communities(pool, db)
    
    if is_all or '--documents' in args:
        # print("Publishing Document Table...")
        publish_documents(final_documents, db, pool, skip_existing, force_ids=force_ids)

    if is_all or '--entities' in args:
        # print("Publishing Entity Table...")
        publish_entities(combined_entities, db, pool, skip_existing, final_covariates, force_ids=force_ids)
        
    if is_all or '--relationships' in args:
        # print("Publishing Relationship Table...")
        publish_relationships(final_relationships, db, pool, skip_existing, force_ids=force_ids)

    if is_all or '--text-units' in args:
        publish_text_units(final_text_units, db, pool, skip_existing, final_covariates, force_ids=force_ids)

    if is_all or '--community-reports' in args:
        # print("Publishing Community Report Table...")
        publish_community_reports(final_community_reports, final_communities, db, pool, skip_existing, force_ids=force_ids)

    if is_all or '--community-weights' in args:
        # print("Building Community Weights...")
        build_and_publish_community_weights(db, pool, skip_existing, force_ids=force_ids)

    print("Awaiting last of the tasks to complete...")
    pool.shutdown(wait=True)
    exit()


def publish_community_reports(final_community_reports:pd.DataFrame, final_communities:pd.DataFrame, db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, force_ids:list[str]=[]):
    pbar = tqdm(total=len(final_community_reports), desc="Processing Community Reports", colour='MAGENTA')
    community_reports_conn = client_factory(COMMUNITY_CONTAINER_NAME, db)

    ## Get existing IDs (to skip)
    existing_ids_res = community_reports_conn.query_items(query="SELECT c.id FROM c", enable_cross_partition_query=True) if skip_existing else []
    id_list = {item["id"]: True for item in existing_ids_res}

    futures = []
    for community_report in final_community_reports.itertuples():
        if skip_existing and community_report.community in id_list:       ## Skip existing records
            if community_report.id not in force_ids and community_report.community not in force_ids:        ## Force IDs will override the skip_existing
                pbar.update(1)
                continue

        futures.append(pool.submit(process_community_report, community_report, final_communities, db, pbar))
        if len(futures) > 20:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
        
    for future in futures:
        future.result()


def process_community_report(community_report:any, final_communities:pd.DataFrame, db:DatabaseProxy, pbar:tqdm):
    try:
        raw_community_data = final_communities[final_communities["id"] == community_report.community].iloc[0]
        if raw_community_data is not None and raw_community_data.id != community_report.community:
            print(f"Error: Raw Community Data found was for a different Community Report: {community_report.id} != {raw_community_data.id}")
            return
        
        ## Step 1: Load Community Report
        community = Community.load_from_df_row(community_report, raw_community_data)
        ## Step 2: Save the community report to the CosmosDB
        community.save(db)    
        pbar.update(1)
    except Exception as e:
        print(f"Error inserting Community Report: {community_report.id}")
        print(e)


def publish_relationships(relationships:pd.DataFrame, db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, force_ids:list[str]=[]):
    pbar = tqdm(total=len(relationships), desc="Processing Relationships", colour='YELLOW')
    relationships_conn = client_factory(RELATIONSHIP_CONTAINER_NAME, db)

    ## Get existing IDs (to skip)
    existing_ids_res = relationships_conn.query_items(query="SELECT c.id FROM c", enable_cross_partition_query=True) if skip_existing else []
    id_list = {item["id"]: True for item in existing_ids_res}
    
    ## Build map of entity title -> Entity ID
    entities_res = client_factory(ENTITY_CONTAINER_NAME, db).query_items(query=f"SELECT c.id, c.title FROM c", enable_cross_partition_query=True)
    entity_map = {entity["title"]: entity["id"] for entity in entities_res}

    futures = []
    for relationship in relationships.itertuples():
        if skip_existing and str(relationship.human_readable_id) in id_list:       ## Skip existing records
            if relationship.id not in force_ids and relationship.human_readable_id not in force_ids:        ## Force IDs will override the skip_existing
                pbar.update(1)
                continue

        futures.append(pool.submit(process_relationship, relationship, db, pbar, entity_map))
        if len(futures) > 30:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
    
    for future in futures:
        future.result()


def process_relationship(relationship_data:any, db:DatabaseProxy, pbar:tqdm, entity_map:dict[str, str]):

    try:
        # Step 1: Load Relationship
        relationship = Relationship.load_from_df_row(relationship_data, entity_map)
        # Step 2: Save the relationship to the CosmosDB
        relationship.save(db)
        pbar.update(1)
    except Exception as e:
        print(f"Error inserting Relationship: {relationship_data.id}")
        print(e)



def publish_entities(entities:pd.DataFrame, db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, final_covariates:pd.DataFrame = None, force_ids:list[str]=[]):
    entities_conn = client_factory(ENTITY_CONTAINER_NAME, db)

    ## Get existing IDs (to skip)
    existing_ids_res = entities_conn.query_items(query="SELECT c.uid FROM c", enable_cross_partition_query=True) if skip_existing else []
    id_list = {item["uid"]: True for item in existing_ids_res}
    
    ## Get a unique list of entity ids from the data
    entity_ids = entities["id"].unique()
    pbar = tqdm(total=len(entity_ids), desc="Processing Entities", colour='BLUE')

    futures = []
    for entity_id in entity_ids:
        if skip_existing and entity_id in id_list:       ## Skip existing records
            if entity_id not in force_ids:        ## Force IDs will override the skip_existing
                pbar.update(1)
                continue

        futures.append(pool.submit(process_entity, entity_id, entities, db, pbar, final_covariates))
        if len(futures) > 30:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
    
    ## Wait for any remaining futures
    for future in futures:
        future.result()


def ensure_entities(pool:ThreadPoolExecutor, db:DatabaseProxy):
    entities_conn = client_factory(ENTITY_CONTAINER_NAME, db)
    entities_res = entities_conn.query_items(query="SELECT c.uid FROM c", enable_cross_partition_query=True)
    entity_ids = [entity["uid"] for entity in entities_res]
    print("Loaded entity ids")
    entity_meta_conn = client_factory(ENTITY_METADATA_CONTAINER_NAME, db)
    entity_meta_res = entity_meta_conn.query_items(query="SELECT c.uid FROM c", enable_cross_partition_query=True)
    print("Loaded meta ids")
    entity_meta_ids = [entity["uid"] for entity in entity_meta_res]
    entity_meta_ids = {entity["uid"]: True for entity in entity_meta_res}
    print("Mapped meta ids")
    missing_meta_entries = [entity_id for entity_id in entity_ids if entity_id not in entity_meta_ids]
    # Remove duplicates
    entry_set = set(missing_meta_entries)
    missing_meta_entries = list(entry_set)
    print("Missing ENtities: ", ",".join(missing_meta_entries))
    with open("missing_entities.txt", "w") as f:
        f.write(",".join(missing_meta_entries))


def refresh_entities(pool:ThreadPoolExecutor, db:DatabaseProxy):
    entities_conn = client_factory(ENTITY_CONTAINER_NAME, db)
    entities_res = entities_conn.query_items(query="SELECT c.id FROM c", enable_cross_partition_query=True)
    entity_ids = [entity["id"] for entity in entities_res]

    pbar = tqdm(total=len(entity_ids), desc="Refreshing Entities", colour='YELLOW')
    futures = []
    for entity_id in entity_ids:
        futures.append(pool.submit(refresh_entity, entity_id, db, pbar))
        if len(futures) > 6:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
    
    ## Wait for any remaining futures
    for future in futures:
        future.result()

def refresh_entity(entity_id:str, db:DatabaseProxy, pbar:tqdm):
    while True: 
        try:
            entity = Entity.load(entity_id, db)
            entity.save(db)
            pbar.update(1)
            break
        except Exception as e:
            if 'TooManyRequests' in str(e):
                import time
                print(f"Going too fast, will pause and try again in a few seconds...")
                time.sleep(1)
            else:
                print(f"Error refreshing Entity: {entity_id}")
                import traceback
                traceback.print_exception(e)
                break

def process_entity(entity_id:str, entities:pd.DataFrame, db:DatabaseProxy, pbar:tqdm, final_covariates:pd.DataFrame):
    ## Step 1: Collect up all the occurances of this entity in the entities table
    entity_set = entities[entities["id"] == entity_id]

    try:
        ## Step 2: Build Entity Record
        entity = Entity.load_from_data_frame(entity_set, final_covariates)

        ## Step 3: Save the entity to the CosmosDB 
        entity.save(db)
        pbar.update(1)
    except Exception as e:
        import traceback
        print(f"Error inserting Entity: {entity_id}")
        print(e)
        traceback.print_exception(e)


def publish_text_units(final_text_units:pd.DataFrame, db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, 
                       covariates:pd.DataFrame = None, force_ids:list[str]=[]):
    pbar = tqdm(total=len(final_text_units), desc="Processing Text Units", colour='green')
    txt_units_conn = client_factory(TEXT_UNIT_CONTAINER_NAME, db)

    ## Get existing IDs (to skip)
    existing_ids_res = txt_units_conn.query_items(query="SELECT c.id FROM c", enable_cross_partition_query=True) if skip_existing else []
    id_list = {item["id"]: True for item in existing_ids_res}

    ## Build a map of Entity UID -> Entity ID for faster lookup
    entities_res = client_factory(ENTITY_CONTAINER_NAME, db).query_items(query=f"SELECT c.id, c.uid FROM c", enable_cross_partition_query=True)
    entity_id_map = {entity["uid"]: entity["id"] for entity in entities_res}

    relaationshipd_res = client_factory(RELATIONSHIP_CONTAINER_NAME, db).query_items(query=f"SELECT c.id, c.uid FROM c", enable_cross_partition_query=True)
    relationship_id_map = {relationship["uid"]: relationship["id"] for relationship in relaationshipd_res}
    
    futures = []
    for text_unit in final_text_units.itertuples():
        if skip_existing and text_unit.id in id_list:       ## Skip existing records
            if text_unit.id not in force_ids:        ## Force IDs will override the skip_existing    
                pbar.update(1)
                continue

        futures.append(pool.submit(process_text_unit, text_unit, db, pbar, entity_id_map, relationship_id_map, covariates))
        if len(futures) > 30:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
    
    for future in futures:
        future.result()

def process_text_unit(text_unit:Tuple[Hashable, Series], db:DatabaseProxy, pbar:tqdm, 
                      entity_map:dict[str, str] = None, relationship_map:dict[str, str] = None, covariates:pd.DataFrame = None):

    try: 
        ## Step 1: Load Text Unit
        text_unit = TextUnit.load_from_df_row(text_unit, entity_map, relationship_map, covariates, db)
        ## Step 2: Save the text unit to the CosmosDB
        text_unit.save(db)
        pbar.update(1)
    except Exception as e:
        print(f"Error inserting Text Unit: {text_unit.id}")
        print(e)


def publish_documents(final_documents:pd.DataFrame, db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, force_ids:list[str]=[]):
    pbar = tqdm(total=len(final_documents), desc="Processing Documents", colour='YELLOW')
    documents_conn = client_factory(DOCUMENT_CONTAINER_NAME, db)

    ## Get existing IDs (to skip)
    existing_ids_res = documents_conn.query_items(query="SELECT c.uid FROM c", enable_cross_partition_query=True) if skip_existing else []
    id_list = {item["uid"]: True for item in existing_ids_res}

    futures = []
    doc_counter = 0
    for document in final_documents.itertuples():
        doc_counter += 1
        if skip_existing and document.id in id_list:       ## Skip existing records
            if document.id not in force_ids:        ## Force IDs will override the skip_existing
                pbar.update(1)
                continue

        futures.append(pool.submit(process_document, document, doc_counter, db, pbar))
        if len(futures) > 20:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []

def process_document(document:Tuple[Hashable, Series], doc_id:int, db:DatabaseProxy, pbar:tqdm):
    try:
        ## Step 1: Load the Document
        document = Document.load_from_df_row(document, doc_id)
        ## Step 2: Save the document to the CosmosDB
        document.save(db)
        pbar.update(1)
    except Exception as e:
        print(f"Error inserting Document: {document.id}")
        print(e)



def build_and_publish_community_weights(db:DatabaseProxy, pool:ThreadPoolExecutor, skip_existing:bool=True, force_ids:list[str]=[]):
    communities_con = client_factory(COMMUNITY_CONTAINER_NAME, db)
    entities_con = client_factory(ENTITY_CONTAINER_NAME, db)

    ## Get a count of all communities
    community_count_res = communities_con.query_items(query="SELECT VALUE count(c.id) FROM c", enable_cross_partition_query=True)
    community_count = list(community_count_res)[0]
    
    ## Create Progress Bar
    pbar = tqdm(total=community_count, desc="Processing Community Weights", colour='green')

    ## Get all communities
    all_community_weights = []
    communities_res = communities_con.query_items(query="SELECT c.id, c.level, c.rank, c.title FROM c", enable_cross_partition_query=True)
    futures = []
    for community in communities_res:
        futures.append(pool.submit(build_community_weight, community, entities_con, db))
        if len(futures) > 20:
            ## Wait for the futures to complete
            for future in futures:
                all_community_weights.append(future.result())
                pbar.update(1)
            futures = []

    for future in futures:
        all_community_weights.append(future.result())
        pbar.update(1)
        
    ## Now calculate the normalised weights for each community + publish the commity record
    pbar = tqdm(total=len(all_community_weights), desc="Normalising + Publishing Community Weights", colour='YELLOW')
    max_weight = max([community["weight"] for community in all_community_weights])
    max_level = max([community["level"] for community in all_community_weights])
    level_maxes = []
    for level in range(0, max_level+1):
        level_weights = [community["weight"] for community in all_community_weights if community["level"] == level]
        if len(level_weights) == 0:
            level_maxes.append(0)
        else:
            level_maxes.append(max(level_weights))

    # level_maxes = [max([community["weight"] for community in all_community_weights if community["level"] == level]) for level in range(0, max_level+1)]
    futures = []
    for community_weight in all_community_weights:
        futures.append(pool.submit(publish_community_weight, max_weight, level_maxes, community_weight, db))
        if len(futures) > 20:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
                pbar.update(1)
            futures = []
    
    for future in futures:
        future.result()
        pbar.update(1)
    
    print("Done!")

def publish_community_weight(max_weight, level_maxes, community_weight, db:DatabaseProxy):
    community_id = community_weight["id"]
    weight = community_weight["weight"]
    community = Community.load(community_id, db)
    community.weight = weight
    
    normalised_weight = weight / max_weight
    community.normalised_weight = normalised_weight
    community.normalised_level_weight = weight / level_maxes[community.level] if level_maxes[community.level] > 0 else weight
    
    try:
        community.save(db)
    except Exception as e:
        print(f"Error adding Community Weights to Community: {community_id}")
        print(e)

def build_community_weight(community, entities_con:ContainerProxy, db:DatabaseProxy) -> dict:
    community_id = community["id"]
    level = community["level"]
    rank = community["rank"]
    title = community["title"]

    ## Get all entities in the community
    entity_weights = []
    entities_res = entities_con.query_items(query=f"SELECT c.id FROM c WHERE ARRAY_CONTAINS(c.community_ids, '{community_id}')", enable_cross_partition_query=True)
    entity_ids = [entity["id"] for entity in entities_res]
    try:
        entities = Entity.load_all(entity_ids, db, include_metadata=True)
        for entity in entities:
            num_text_units = int(len(entity.sources) if entity.sources is not None else 0)
            entity_weights.append(num_text_units)
    except CosmosResourceNotFoundError:
        entities = Entity.load_all(entity_ids, db, include_metadata=False)
        entity_uids = [entity.uid for entity in entities]
        print(f"One of these Entities not found: {entity_uids}")
        
    # for sources_len in entities_res:
    #     try:
    #         entity = Entity.load(sources_len["id"], db, include_metadata=True)
    #         num_text_units = int(len(entity.sources) if entity.sources is not None else 0)
    #         entity_weights.append(num_text_units)
    #     except CosmosResourceNotFoundError:
    #         print(f"Entity not found: {sources_len['id']}")
    #         raise

    ## Calculate the community weight
    community_weight = sum([entity for entity in entity_weights])
    return {
            "id": community_id,
            "title": title,
            "level": level,
            "rank": rank,
            "weight": community_weight
        }

def refresh_communities(pool:ThreadPoolExecutor, db:DatabaseProxy):
    communities_conn = client_factory(COMMUNITY_CONTAINER_NAME, db)
    communities_res = communities_conn.query_items(query="SELECT c.id FROM c", enable_cross_partition_query=True)
    community_ids = [entity["id"] for entity in communities_res]

    pbar = tqdm(total=len(community_ids), desc="Refreshing Communities", colour='GREEN')
    futures = []
    for community_id in community_ids:
        futures.append(pool.submit(refresh_community, community_id, db, pbar))
        if len(futures) > 6:
            ## Wait for the futures to complete
            for future in futures:
                future.result()
            futures = []
    
    ## Wait for any remaining futures
    for future in futures:
        future.result()

def refresh_community(community_id:str, db:DatabaseProxy, pbar:tqdm):
    while True: 
        try:
            community = Community.load(community_id, db)
            community.save(db)
            pbar.update(1)
        except Exception as e:
            if 'TooManyRequests' in str(e):
                import time
                print(f"Going too fast, will pause and try again in a few seconds...")
                time.sleep(1)
            else:
                print(f"Error refreshing Community: {community_id}")
                import traceback
                traceback.print_exception(e)
                break

def _infer_data_dir(root: str) -> str:
    output = Path(root) / "output"
    # use the latest data-run folder
    if output.exists():
        folders = sorted(output.iterdir(), key=os.path.getmtime, reverse=True)
        if len(folders) > 0:
            folder = folders[0]
            return str((folder / "artifacts").absolute())
    msg = f"Could not infer data directory from root={root}"
    raise ValueError(msg)

def _parse_args() -> dict[str, str]:
    args = sys.argv[1:]
    if len(args) == 0:
        return {}
    res = {}
    for arg in args: 
        if arg.startswith("--"):
            arr = arg.split("=")
            key = arr[0]
            value = arr[1] if len(arr) > 1 else True
            res[key] = value
    return res


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
if __name__ ==  '__main__':
    run_main()