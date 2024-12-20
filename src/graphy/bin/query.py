#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys
import dotenv
dotenv.load_dotenv(".env")

from graphrag.config import create_graphrag_config
from graphy.patch.graphrag.query.api import local_search, global_search

async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: query --run=<run_id> --response-type=<response_type> --query-type=<query_type> --query=<query>")
        print("")
        print("Options:")
        print("  --run=<run_id>                        The run ID to use (aka. the folder name) - defaults to the latest run in the output directory")
        print("  --response-type=<response_type>       The response type to return (eg. 'Multiple Paragraphs', 'Single Sentence', 'List of 3-7 Points', 'Single Page', 'Multi-Page Report')")
        print("  --query-type=<query_type>             The query type to use (eg. 'global', 'local') [Default: local]")
        print("  --local                               Use local search")
        print("  --global                              Use global search")
        print("  --query=<query>                       To run a single query immediately (otherwise, you will be prompted for a query)")
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

    print(f"Loading data from {INPUT_DIR}")
    data_path = Path(INPUT_DIR)
    final_nodes: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_TABLE}.parquet")
    final_entities: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_EMBEDDING_TABLE}.parquet")
    final_community_reports: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{COMMUNITY_REPORT_TABLE}.parquet")
    final_text_units: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{TEXT_UNIT_TABLE}.parquet")
    final_relationships: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{RELATIONSHIP_TABLE}.parquet")
    final_covariates: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{COVARIATE_TABLE}.parquet")
    
    config = None
    settings_path = Path("settings.yaml")
    if settings_path.exists():
        with settings_path.open("rb") as file:
            import yaml
            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            config = create_graphrag_config(data, root_dir="./")
    
    response_type = args.get("--response-type", "Multiple Paragraphs")   # Simple string that describes the response type (eg. "Multiple Paragraphs", "Single Sentence", "List of 3-7 Points", "Single Page", "Multi-Page Report")
    
    query_type = args.get("--query-type", "local")   # Simple string that describes the query type (eg. "global", "local")
    if "--global" in args:
        query_type = "global"
    elif "--local" in args:
        query_type = "local"
    
    query = args.get("--query", None)   # The user query to search for.
    single_query = True if query else False
    while True: 
        # Prompt the user for a search query
        if not single_query:
            query = input("Query: ")
            if not query or query == "q":
                return

        # Perform a Global Search
        print(f"Searching {query_type}")
        if query_type == "local":
            local_search_result = await local_search(
                config=config, 
                nodes=final_nodes, 
                entities=final_entities, 
                community_reports=final_community_reports, 
                text_units=final_text_units, 
                relationships=final_relationships, 
                covariates=final_covariates, 
                community_level=COMMUNITY_LEVEL, 
                response_type=response_type, 
                query=query)
            
            print("\n\nResult:\n")
            print(local_search_result.response)
            print("\n\n")
            print(f" - LLM Queries: {local_search_result.llm_calls}")
            print(f" -      Tokens: {local_search_result.prompt_tokens}")
            print(f" - Search Time: {local_search_result.completion_time} seconds")
            print("\n\n")
            local_search_result.prompt_tokens
        else:
            global_search_result = await global_search(
                config=config, 
                nodes=final_nodes, 
                entities=final_entities, 
                community_reports=final_community_reports, 
                community_level=COMMUNITY_LEVEL, 
                response_type=response_type, 
                query=query)
        
            print("\n\nResult:\n")
            print(global_search_result)
            print("\n\n")
            print(f" - LLM Queries: {global_search_result.llm_calls}")
            print(f" -      Tokens: {global_search_result.prompt_tokens}")
            print(f" - Search Time: {global_search_result.completion_time} seconds")
            print("\n\n")
        
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