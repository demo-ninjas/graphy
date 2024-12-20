#!/usr/bin/env python
import sys
from pathlib import Path
import asyncio
import dotenv
dotenv.load_dotenv(".env")

from graphy.config import create_graphrag_config
from graphy.ingest import build_graph

async def main():
    args = _parse_args()

    if "--help" in args:
        print("Usage: build_graph --config=<config_file> --run=<run_id> --resume")
        print("")
        print("Options:")
        print("  --config=<config_file>          The configuration file to use (default: settings.yaml)")
        print("  --run=<run_id>                  The run ID to resume (aka. the folder name) - if not specified, will start a new run (unless --resume is specified)")
        print("  --resume                        Resume the latest run")
        return
    
    
    ## Run the pipeline
    print("Initialising pipeline...")
    graphrag_config = None

    ## Load the Config File
    settings_yaml = Path(args.get("--config", "settings.yaml"))
    if not settings_yaml.exists():
        print(f"Config file not found: {settings_yaml}")
        return
    
    with settings_yaml.open("rb") as file:
        import yaml
        data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
        graphrag_config = create_graphrag_config(data, ".")
    
    ## Get the first command line argument as the run id
    run_id = args.get("--run", None)
    is_resume = run_id is not None

    ## If resume is in the args and a run is not specified, then infer the run to be the latest run
    if '--resume' in args:
        is_resume = True
        if run_id is None:
            run_id = _infer_latest_run(".")
    
    ## Build the Graph
    await build_graph(graphrag_config, is_resume, run_id, True)
    

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

def _infer_latest_run(root: str) -> str:
    import os
    output = Path(root) / "output"
    # use the latest data-run folder
    if output.exists():
        folders = sorted(output.iterdir(), key=os.path.getmtime, reverse=True)
        if len(folders) > 0:
            folder = folders[0]
            return folder.name

    msg = f"Could not infer latest run from root={root}"
    raise ValueError(msg)


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
if __name__ ==  '__main__':
    run_main()