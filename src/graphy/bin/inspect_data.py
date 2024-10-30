#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys


from graphrag.query.indexer_adapters import (
    read_indexer_covariates,
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    read_indexer_text_units,
)


async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: inspect-data --run=<run_id> --file=<file> --head=<n> --list")
        print("")
        print("Options:")
        print("  --list                                List all available files (for the run)")
        print("  --run=<run_id>                        The run ID to use (aka. the folder name) - defaults to the latest run in the output directory")
        print("  --file=<file>                         The file to load, either full file name or the short name of the file (e.g. 'entities', 'relationships', 'embeddings', 'communities', 'texts', 'covariates')")
        print("  --head=<n>                            The number of rows to display (Default: 10)")
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
    
    if file == "entities":
        file = ENTITY_TABLE
    elif file == "embeddings":
        file = ENTITY_EMBEDDING_TABLE
    elif file == "relationships":
        file = RELATIONSHIP_TABLE
    elif file == "communities":
        file = COMMUNITY_REPORT_TABLE
    elif file == "covariates":
        file = COVARIATE_TABLE
    elif file == "texts":
        file = TEXT_UNIT_TABLE
    
    if not file.endswith(".parquet"):
        file = f"{file}.parquet"

    file_path = Path(f"{data_path.as_posix()}/{file}")
    if not file_path.exists():
        print(f"File not found: {file}")
        return
    
    print(f"Loading {file}...")
    data = pd.read_parquet(file_path.as_posix())

    print(f" Total Records: {len(data)}")
    if 'id' in data.columns:
        print(f"Unique Records: {len(data['id'].unique())}")
    
    print(f"\nSample Records:\n")
    head_count = int(args.get("--head", 10))
    print(data.head(head_count))

    print("\nCols:\n")
    for col in data.columns:
        print(f" - {col}")
    
    print("\n\nFirst Record:\n")
    print(data.iloc[0])
    
    exit(0)

    # file_arg = args.get("--file", None)
    # file_to_load = f"{data_path.as_posix()}/{ENTITY_TABLE}.parquet"
    # if file_arg:
    #     if '.parquet' not in file_arg:
    #         uf = file_arg.upper()
    #         if uf == "ENTITY" or uf == "ENTITIES":
    #             file_arg = ENTITY_TABLE
    #         elif uf == "ENTITY_EMBEDDING" or uf == "EMBEDDING" or uf == "EMBEDDINGS":
    #             file_arg = ENTITY_EMBEDDING_TABLE
    #         elif uf == "COMMUNITY_REPORT" or uf == "COMMUNITY" or uf == "COMMUNITIES":
    #             file_arg = COMMUNITY_REPORT_TABLE
    #         elif uf == "TEXT_UNIT" or uf == "TEXT" or uf == "TEXT_UNITS":
    #             file_arg = TEXT_UNIT_TABLE
    #         elif uf == "RELATIONSHIP" or uf == "RELATIONSHIPS":
    #             file_arg = RELATIONSHIP_TABLE
    #         elif uf == "COVARIATE" or uf == "COVARIATES":
    #             file_arg = COVARIATE_TABLE

    #         file_arg = f"{file_arg}.parquet"
        
    #     fp = Path(f"{data_path.as_posix()}/{file_arg}")
    #     if fp.exists():
    #         file_to_load = fp.as_posix()
    #     else:
    #         print(f"File not found: {file_arg}")
    #         return
    
    
    # print(f"Loading Relationship Table...")
    # final_relationships: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{RELATIONSHIP_TABLE}.parquet")
    # print(f"Loading Embedding Table...")
    # final_nodes: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_EMBEDDING_TABLE}.parquet")

    # print("Reading relationships")
    # edges = read_indexer_relationships(final_relationships=final_relationships)
    # print("Reading entities")
    # entities = read_indexer_entities(final_entities=final_nodes, final_nodes=final_entities, community_level=COMMUNITY_LEVEL)

    print(f"Loading Entity Table...")
    final_entities = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_TABLE}.parquet")
    entity_0 = final_entities.iloc[0]
    print("\nEntities Sample:\n")
    print(final_entities.head(10))
    print("\nCols:\n")
    print(final_entities.columns)
    with open("entities.txt", "w") as f:
        f.write(final_entities.head(20).to_string())
    final_entities = None

    print("Loading Embedding Table...")
    final_nodes = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_EMBEDDING_TABLE}.parquet")
    print("\nEmbeddings Sample:\n")
    print(final_nodes.head(10))
    print("\nCols:\n")
    print(final_nodes.columns)

    with open("embeddings.txt", "w") as f:
        f.write(final_nodes.head(20).to_string())
    final_nodes = None


    print("Loading Relationship Table...")
    final_relationships = pd.read_parquet(f"{data_path.as_posix()}/{RELATIONSHIP_TABLE}.parquet")
    print("\nRelationships Sample:\n")
    print(final_relationships.head(10))
    print("\nCols:\n")
    print(final_relationships.columns)

    with open("relationships.txt", "w") as f:
        f.write(final_relationships.head(20).to_string())
    final_relationships = None
    
    print("Loading Community Report Table...")
    final_community_reports = pd.read_parquet(f"{data_path.as_posix()}/{COMMUNITY_REPORT_TABLE}.parquet")
    print("\nCommunity Reports Sample:\n")
    print(final_community_reports.head(10))
    print("\nCols:\n")
    print(final_community_reports.columns)
    with open("community_reports.txt", "w") as f:
        f.write(final_community_reports.head(20).to_string())
    final_community_reports = None


    print("Loading Covariate Table...")
    final_covariates = pd.read_parquet(f"{data_path.as_posix()}/{COVARIATE_TABLE}.parquet")
    print("\nCovariates Sample:\n")
    print(final_covariates.head(10))
    print("\nCols:\n")
    print(final_covariates.columns)
    with open("covariates.txt", "w") as f:
        f.write(final_covariates.head(20).to_string())
    final_covariates = None


    print("Loading Text Unit Table...")
    final_text_units =pd.read_parquet(f"{data_path.as_posix()}/{TEXT_UNIT_TABLE}.parquet")
    print("\nText Units Sample:\n")
    print(final_text_units.head(10))
    print("\nCols:\n")
    print(final_text_units.columns)
    with open("text_units.txt", "w") as f:
        f.write(final_text_units.head(20).to_string())
    final_text_units = None
    
    print("Data loaded successfully.")
    exit(0)

    
    query = args.get("--query", None)   # The user query to search for.
    single_query = True if query else False
    
    if not single_query:
        ## Select the top 10 rows and print them out
        print("\nEntities Sample:\n")
        print(entities.head(10))
        print("\nRelationships Sample:\n")
        print(edges.head(10))

    print("\n\n")
    while True: 
        # Prompt the user for a search query
        if not single_query:
            query = input("Query: ")
            if not query or query == "q":
                return

        # Search the data frame for a match on the query
        # print(f"Searching for '{query}' in the data")
        # result = data[data.apply(lambda row: query in str(row), axis=1)]
        # print("\n\nResult:\n")
        # print(result)
        # print("\n\n")
        break
        
        if single_query:
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