#!/usr/bin/env python
import pandas as pd
from pathlib import Path
import asyncio
import os
import sys
from time import sleep
from threading import Thread
import azure.cosmos.cosmos_client as cosmos_client
from azure.identity import DefaultAzureCredential

import dotenv
dotenv.load_dotenv(".env")

from tqdm import tqdm
from graphy.query import search

from graphrag.query.structured_search.global_search.callbacks import GlobalSearchLLMCallback
from graphrag.query.structured_search.base import SearchResult

class SearchCallback(GlobalSearchLLMCallback): 
    def on_map_response_start(self, map_response_contexts: list[str]):
        super().on_map_response_start(map_response_contexts)
        print("Map Response Start - No. Chunks: " + str(len(map_response_contexts)))

    def on_map_response_end(self, map_response_outputs: list[SearchResult]):
        super().on_map_response_end(map_response_outputs)
        print("Map Response End - No. Outputs: " + str(len(map_response_outputs)))


async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: query --response-type=<response_type> --query-type=<query_type> --community-level=<community_level> --query=<query>")
        print("")
        print("Options:")
        print("  --response-type=<response_type>        The response type to return (eg. 'Multiple Paragraphs', 'Single Sentence', 'List of 3-7 Points', 'Single Page', 'Multi-Page Report')")
        print("  --query-type=<query_type>              The query type to use (eg. 'global', 'local') [Default: global]")
        print("  --community-level=<community_level>    The community level to search at (eg. '1', '2', '3', etc...) [Default: 2]")
        print("  --min-rank=<min_rank>                  The minimum rank to return results for [Default: 0]")
        print("  --use-summary=<true/false>             Use the summary instead of the full text [Default: false]")
        print("  --allow-general-knowledge=<true/false> Allow general knowledge to be used in the search [Default: false]")
        print("  --fast-token-count=<true/false>        Estimate the token count for the query rather than calculating the actual token count (faster but less accurate) [Default: true]")
        print("  --local                                Use local search")
        print("  --global                               Use global search")
        print("  --query=<query>                        To run a single query immediately (otherwise, you will be continuously prompted for a query)")
        print("  --load-sources=<true/false>            Load the detailed source information [Default: true]")
        print("  --save-output=<true/false>             Save the output to a file [Default: true]")
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

    ## Check for a local settings file
    config = None
    settings_path = Path("settings.yaml")
    if settings_path.exists():
        with settings_path.open("rb") as file:
            import yaml
            from graphrag.config import create_graphrag_config
            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            config = create_graphrag_config(data, root_dir="./")
    

    response_type = args.get("--response-type", "Multiple Paragraphs")   # Simple string that describes the response type (eg. "Multiple Paragraphs", "Single Sentence", "List of 3-7 Points", "Single Page", "Multi-Page Report")
    
    query_type = args.get("--query-type", "global")   # Simple string that describes the query type (eg. "global", "local")
    if "--global" in args:
        query_type = "global"
    elif "--local" in args:
        query_type = "local"
    
    community_level = int(args.get("--community-level", "2"))   # The community level to search at (eg. "1", "2", "3", etc...)
    min_rank = float(args.get("--min-rank", args.get('--rank', "0")))   # The minimum rank to return results for

    use_summary = args.get("--use-summary", "false").lower() == "true"   # Use the summary instead of the full text
    allow_general_knowledge = args.get("--allow-general-knowledge", "false").lower() == "true"   # Allow general knowledge to be used in the search

    estimate_token_count = args.get("--fast-token-count", "true").lower() == "true"   # Estimate the token count for the query rather than calculating the actual token count (faster but less accurate)

    load_sources = True
    if "--load-sources" in args:
        load_sources = args.get("--load-sources", "true").lower() == "true"

    save_output = True
    if "--save-output" in args:
        save_output = args.get("--save-output", "true").lower() == "true"
    if '--no-output' in args:
        save_output = False

    query = args.get("--query", None)   # The user query to search for.
    single_query = True if query else False
    while True: 
        # Prompt the user for a search query
        if not single_query:
            query = input("Query: ")
            if not query or query == "q":
                return

        if query.startswith("/"):
            # Perform a command
            if query == "/exit":
                return
            elif query == "/help":
                print("Commands:")
                print("  /exit - Exit the program")
                print("  /help - Show this help message")
                print("  /show-settings - Show the current settings")
                print("  /local - Switch to local search")
                print("  /global - Switch to global search (default)")
                print("  /community-level <level> - Set the community level [Default: 2]")
                print("  /min-rank <rank> - Set the minimum rank to return results for [Default: 0]")
                print("  /use-summary <true/false> - Use the summary instead of the full text [Default: false]")
                print("  /allow-general-knowledge <true/false> - Allow general knowledge to be used in the search [Default: false]")
                print("  /fast-token-count <true/false> - Estimate the token count for the query rather than calculating the actual token count [Default: true]")
                print("  /response-type <type> - Set the response type [Default: Multiple Paragraphs] (eg. 'Multiple Paragraphs', 'Single Sentence', 'List of 3-7 Points', 'Single Page', 'Multi-Page Report)")
                print("  /load-sources <true/false> - Load the detailed source information [Default: true]")
                print("  /save-output <true/false> - Save the output to a file [Default: true]")
                continue
            elif query == "/show-settings" or query == "/settings":
                print("Settings:")
                print(f" -              Query Type: {query_type}")
                print(f" -     Max Community Level: {community_level}")
                print(f" -            Minimum Rank: {min_rank}")
                print(f" -             Use Summary: {use_summary}")
                print(f" -    Estimate Token Count: {estimate_token_count}")
                print(f" - Allow General Knowledge: {allow_general_knowledge}")
                print(f" -           Response Type: {response_type}")
                print(f" -            Load Sources: {load_sources}")
                print(f" -             Save Output: {save_output}")
                continue
            elif query == "/local":
                query_type = "local"
                print("Switched to local search.")
                continue
            elif query == "/global":
                query_type = "global"
                print("Switched to global search.")
                continue
            elif query.startswith("/community-level") or query.startswith("/community") or query.startswith("/level"):
                community_level = int(query.split(" ")[1].strip())
                print(f"Community level set to {community_level}")
                continue
            elif query.startswith("/min-rank") or query.startswith("/rank"):
                min_rank = float(query.split(" ")[1].strip())
                print(f"Minimum rank set to {min_rank}")
                continue
            elif query.startswith("/use-summary"):
                use_summary = query.split(" ", 1)[1].strip().lower() == "true"
                print(f"Use summary set to {use_summary}")
                continue
            elif query.startswith("/allow-general-knowledge") or query.startswith("/allow-gk") or query.startswith("/general-knowledge"):
                allow_general_knowledge = query.split(" ", 1)[1].strip().lower() == "true"
                print(f"Allow general knowledge set to {allow_general_knowledge}")
                continue
            elif query.startswith("/fast-token-count"):
                estimate_token_count = query.split(" ", 1)[1].strip().lower() == "true"
                print(f"Estimate token count set to {estimate_token_count}")
                continue
            elif query.startswith("/response-type"):
                response_type = query.split(" ", 1)[1].strip()
                print(f"Response type set to {response_type}")
                continue
            elif query.startswith("/load-sources"):
                load_sources = query.split(" ", 1)[1].strip().lower() == "true"
                print(f"Load sources set to {load_sources}")
                continue
            elif query.startswith("/save-output"):
                save_output = query.split(" ", 1)[1].strip().lower() == "true"
                print(f"Save output set to {save_output}")
                continue
            else:
                print("Unknown command: " + query)
                print("Type '/help' for a list of commands.")
                continue
        else: 
            # Perform the Search
            pbar = tqdm(desc="Preparing community data", unit="s", bar_format="{l_bar} {n}{unit}")
            event = asyncio.Event()
            
            # Spawn new thread to update the progress bar
            async def update_pbar():
                while not event.is_set():
                    await asyncio.sleep(1)
                    pbar.set_description("Searching")
                    pbar.update(1)

            async def run_search() -> SearchResult:
                result = await search(
                    load_sources=load_sources, 
                    query_type=query_type,
                    config=config, 
                    db=db,
                    community_level=community_level, 
                    min_rank=min_rank,
                    use_summary=use_summary,
                    allow_general_knowledge=allow_general_knowledge,
                    response_type=response_type, 
                    estimate_tokens=estimate_token_count,
                    query=query)
                event.set()
                return result
            
            outputs = await asyncio.gather(run_search(), update_pbar())  
            result = outputs[0]
            pbar.close()

            print(result)

            if save_output:
                with open("output.txt", "w") as file:
                    file.write(str(result))
                print("Result written to output.txt")

        if single_query:
            break

def write_out_dataframe(file, df: pd.DataFrame):
    col_max_lenths = {col: max([len(str(val)) for val in df[col]]) for col in df.columns}
    ## Write out the column names
    file.write(" | ".join([format_fixed_length_string(col, col_max_lenths[col]) for col in df.columns]))
    for index, row in df.iterrows():
        file.write(f"\n{index}")
        for col, val in row.items():
            file.write(f" | {format_fixed_length_string(str(val), col_max_lenths[col])}")
        file.write(" |")
    file.write("\n")

def format_fixed_length_string(input_string, length):
    if len(input_string) > length:
        return input_string[:length]
    else:
        return input_string.ljust(length)

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