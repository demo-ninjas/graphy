#!/usr/bin/env python
import sys
from pathlib import Path
import asyncio
import os

import pandas as pd
from graphrag.index import run_pipeline, run_pipeline_with_config, create_pipeline_config
from graphrag.config import create_graphrag_config
from graphrag.index.emit import TableEmitterType
from graphrag.index.config import PipelineWorkflowReference
from graphrag.index.workflows.default_workflows import default_workflows
from graphy.ingest.parser import DocumentParser, DocumentParserConfig, ParsedDocument
from graphy.ingest import parse_file

import dotenv
dotenv.load_dotenv(".env")

async def main():
    args = _parse_args()

    if "--help" in args:
        print("Usage: ingest-file --file=<file_path> [--input-dir=<input_dir>] [--output-dir=<output_dir>] [--markdown=<true|false>] [--json=<true|false>] [--min-chunk-chars=<min_chunk_chars>] [--title-height=<title_height>] [--subtitle-height=<subtitle_height>] [--paragraph-height=<paragraph_height>]")
        print("")
        print("Options:")
        print("  --file=<file_path>                 The file to parse")
        print("  --input-dir=<input_dir>            The directory to save the output to (default: input)")
        print("  --output-dir=<output_dir>           The directory to save the output to (default: output)")
        print("  --markdown=<true|false>             Save the markdown representation (default: true)")
        print("  --json=<true|false>                 Save the json representation (default: true)")
        print("  --min-chunk-chars=<min_chunk_chars> The minimum number of characters in a chunk (default: 50)")
        print("  --title-height=<title_height>       The height of the title (default: 1.5)")
        print("  --subtitle-height=<subtitle_height> The height of the subtitle (default: 1.25)")
        print("  --paragraph-height=<paragraph_height> The height of the paragraph (default: 1.0)")
        return

    # Determine File path
    file_path = args.get("--file", os.getenv('FILE_PATH', None))
    if file_path is None:
        print("No file path provided - you must specify a file using '--file', eg:")
        print("python ingest-file.py --file=<file_path>")
        return
    
    file_path = Path(file_path)
    input_dir = Path(args.get('--input-dir', 'input'))
    if not input_dir.exists():
        input_dir.mkdir()
    
    output_dir = Path(args.get('--output-dir', 'output'))
    if not output_dir.exists():
        output_dir.mkdir()
    
    # Load the configuration (currently not offering option of modifying from defaults)
    config = DocumentParserConfig()
    if "--min-chunk-chars" in args:
        config.min_chunk_chars = int(args["--min-chunk-chars"])
    if "--title-height" in args:
        config.title_height = float(args["--title-height"])
    if "--subtitle-height" in args:
        config.subtitle_height = float(args["--subtitle-height"])
    if "--paragraph-height" in args:
        config.paragraph_height = float(args["--paragraph-height"])
        
    # Create the parser
    parser = DocumentParser(config)

    ## Parse the File 
    parse_file(file_path, parser, input_dir, True, True, True)

    ## Build/Rebuild the Graph
    print(f"Building Graph")


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