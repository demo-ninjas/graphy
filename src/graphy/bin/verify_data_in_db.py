#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos import ContainerProxy, DatabaseProxy
from tqdm import tqdm
import dotenv
dotenv.load_dotenv(".env")


async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: verify-data-in-db --run=<run_id> --file=<file> --list")
        print("")
        print("Options:")
        print("  --file=<file>                         The file to load, and verify (e.g. 'entities', 'relationships', 'embeddings', 'communities', 'texts', 'covariates')")
        print("  --head=<n>                            The number of example rows to display (Default: 10)")
        return

    INPUT_DIR = None
    if "--run" in args:
        run_id = args["--run"]
        INPUT_DIR = f"output/{run_id}/artifacts"
    else: 
        INPUT_DIR = _infer_data_dir(".")

    LANCEDB_URI = f"lancedb"
    COMMUNITY_REPORT_TABLE = "create_final_community_reports"
    ENTITY_TABLE = "create_final_nodes"
    ENTITY_EMBEDDING_TABLE = "create_final_entities"
    RELATIONSHIP_TABLE = "create_final_relationships"
    COVARIATE_TABLE = "create_final_covariates"
    TEXT_UNIT_TABLE = "create_final_text_units"
    COMMUNITY_LEVEL = 2

    data_path = Path(INPUT_DIR)


    if "--list" in args:
        print("Available Files:")
        for file in data_path.iterdir():
            print(f" - {file.name}")
        return

    file = args.get("--file", None)
    if not file:
        file = args.get(0)  ## Assume the first argument is the file name
        
    if file is None or len(file) == 0:
        print("No file specified.")
        return
    
    container_name = None
    if file == "entities":
        file = ENTITY_TABLE
        container_name = "entities"
    elif file == "embeddings":
        file = ENTITY_EMBEDDING_TABLE
        container_name = "entities"
    elif file == "relationships":
        file = RELATIONSHIP_TABLE
        container_name = "relationships"
    elif file == "communities":
        file = COMMUNITY_REPORT_TABLE
        container_name = "communities"
    elif file == "covariates":
        file = COVARIATE_TABLE
        container_name = "covariates"
    elif file == "texts":
        file = TEXT_UNIT_TABLE
        container_name = "text-units"
    
    if not file.endswith(".parquet"):
        file = f"{file}.parquet"

    file_path = Path(f"{data_path.as_posix()}/{file}")
    if not file_path.exists():
        print(f"File not found: {file}")
        return
    
    print(f"Loading {file}...")
    data = pd.read_parquet(file_path.as_posix())

    print(f"\n{file} Sample:\n")
    head_count = int(args.get("--head", 10))
    print(data.head(head_count))

    print("\nCols:\n")
    for col in data.columns:
        print(f" - {col}")
    
    print("\n\nStarting Verification...\n")

    ## Load CosmosDB Client
    cosmos_connection_str = os.environ.get("COSMOS_CONNECTION_STRING")
    cosmos_database = os.environ.get("COSMOS_DATABASE_ID", "cardiology-canon")
    client = cosmos_client.CosmosClient.from_connection_string(cosmos_connection_str)
    db = client.get_database_client(cosmos_database)

    ## Create Worker Pool
    pool = ThreadPoolExecutor(max_workers=20)
    client = db.get_container_client(container_name)

    ## Iterate data in dataframe
    pbar = tqdm(total=len(data), desc=f"Verifying {container_name}")
    futures = []
    done_ids = set()
    failures = []
    for index, row in data.iterrows():
        futures.append(pool.submit(_verify_row, client, row, index, done_ids))
        if len(futures) > 100:
            for future in futures:
                result, msg, idx = future.result()
                pbar.update(1)
                if not result:
                    failures.append({ "index": idx, "message": msg })
            futures = []
    
    if len(futures) > 0:
        for future in futures:
            result, msg, idx = future.result()
            pbar.update(1)
            if not result:
                failures.append({ "index": idx, "message": msg })
    
    pbar.close()

    if len(failures) == 0:
        print("All rows verified successfully.")
    else:
        print(f"{len(failures)} rows failed verification.")
        duplicate_count = sum(1 for f in failures if 'DUPLICATE' in f.get("message"))
        missing_count = sum(1 for f in failures if 'MISSING' in f.get("message"))
        replicated_count = sum(1 for f in failures if 'REPLICATED' in f.get("message"))
        print(f" - {duplicate_count} duplicates")
        print(f" - {missing_count} missing")
        print(f" - {replicated_count} replicated")
        print("\n\n")

        print("Duplicates:")
        counter = 0
        for failure in failures:
            counter += 1
            if counter > 20:
                print("...")
                break
            idx = failure.get("index")
            msg = failure.get("message")
            if 'DUPLICATE' in msg:
                ## Find the duplicate row in the data and compare
                row = data.iloc[idx]
                row_id = row.get("id", None)
                ## Find in the data all the records with the same ID
                duplicate_rows = data[data["id"] == row_id]
                print(f" - {row_id} - {row.get('title', None)} [{len(duplicate_rows)}]")

                # if counter == 1: 
                #     print("Test Record:")
                #     print(row)
                #     print("\n\n")
                #     print("Duplicate Records:")
                #     for _, dup_row in duplicate_rows.iterrows():
                #         print(dup_row)
                #         print("\n\n")

        print("\n\nMissing:")
        counter = 0
        for failure in failures:
            counter += 1
            if counter > 20:
                print("...")
                break
            idx = failure.get("index")
            msg = failure.get("message")
            if 'MISSING' in msg:
                print(f" - {msg}")
        
        print("\n\nReplicated:")
        counter = 0
        for failure in failures:
            counter += 1
            if counter > 20:
                print("...")
                break
            idx = failure.get("index")
            msg = failure.get("message")
            if 'REPLICATED' in msg:
                print(f" - {msg}")

                
    

def _verify_row(client: ContainerProxy, row: pd.Series, index: int, done_ids:set) -> tuple[bool, str, int]:
    row_id = row.get("id", None)
    row_title = row.get("title", None)
    # print(f"Verifying {row_id} - {row_title}...")
    
    if row_id in done_ids:
        return False, f"[DUPLICATE] [{index}] {row_id} already exists.", index
    done_ids.add(row_id)

    query = f"SELECT * FROM c WHERE c.id = '{row_id}'"
    result = client.query_items(query=query, enable_cross_partition_query=True)
    items = list(result)
    if len(items) == 0:
        return False, f"[MISSING] [{index}] {row_id} - {row_title} not found in CosmosDB.", index
    elif len(items) > 1:
        return False, f"[REPLICATED] [{index}] {row_id} - {row_title} has multiple entries in CosmosDB.", index
    else:
        return True, f"[OK] [{index}] {row_id} - {row_title} verified.", index



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