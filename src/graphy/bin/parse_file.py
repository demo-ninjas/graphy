#!/usr/bin/env python

import os
import sys
import json
from pathlib import Path
import asyncio
import dotenv
dotenv.load_dotenv(".env")

from azure.identity import DefaultAzureCredential, get_bearer_token_provider

from graphrag.query.llm.oai.chat_openai import ChatOpenAI
from graphrag.query.llm.oai.embedding import OpenAIEmbedding
from graphrag.query.llm.oai.typing import OpenaiApiType


from graphy.parser import ParsedDocument, DocumentChunk, PdfDocIntelligenceParser, PdfParser, DocIntelligenceParser

async def main():
    args = _parse_args()

    if "--help" in args:
        print("Usage: parse-file --file=<file_path> [--output-dir=<output_dir>] [--markdown=<true|false>] [--json=<true|false>] [--min-chunk-chars=<min_chunk_chars>] [--title-height=<title_height>] [--subtitle-height=<subtitle_height>] [--paragraph-height=<paragraph_height>]")
        print("")
        print("Options:")
        print("  --file=<file_path>                 The file to parse")
        print("  --output-dir=<output_dir>           The directory to save the output to (default: input)")
        print("  --config-<key>=<value>              Set a configuration option (eg: --config-min-chunk-chars=100)")
        print("  --parser=<parser>                   The parser to use (pdf or pdfdocintel or docintel; default: pdfdocintel)")
        print("  --markdown=<true|false>             Whether to save the markdown representation (default: true)")
        print("  --json=<true|false>                 Whether to save the json representation (default: true)")
        print("  --analyse-images=<true|false>       Whether to analyse the images within the file (default: true)")
        return

    # Load the configuration (currently not offering option of modifying from defaults)
    config = {}
    for arg in args:
        if arg.startswith("--config-"):
            config_key = arg.replace("--config-", "")
            config_value = args[arg]
            config[config_key] = config_value

    ## Load the GraphRAG Config, in case the settings are described in a settings.yaml 
    graphrag_config = None
    settings_path = Path("settings.yaml")
    if settings_path.exists():
        with settings_path.open("rb") as file:
            import yaml
            from graphrag.config import create_graphrag_config
            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            graphrag_config = create_graphrag_config(data, root_dir="./")


    ## Load the LLM Library
    should_analyse_images = next((v for v in [args.get('--analyse-images'), os.environ.get('ANALYSE_IMAGES')] if v is not None), 'true').lower() in ["true", "yes", "1"]
    llm = None
    if should_analyse_images:
        grc_llm = graphrag_config.llm if graphrag_config is not None else None
        llm_model = next((v for v in [args.get('--openai-model'), os.environ.get('OPENAI_MODEL'), grc_llm.deployment_name if grc_llm is not None else None] if v is not None), 'gpt-4o')
        llm_api_key=next((v for v in [args.get('--openai-key'), os.environ.get('GRAPHRAG_API_KEY'), os.environ.get('OPENAI_API_KEY'), grc_llm.api_key if grc_llm is not None else None] if v is not None)) ## Will throw error if not of these options provide a key
        llm_api_base=next((v for v in [args.get('--openai-api-base'),os.environ.get('GRAPHRAG_API_BASE'), os.environ.get('OPENAI_API_BASE')] if v is not None)) ## Will throw error if not of these options provide a base url
        llm_api_version=next((v for v in [args.get('--openai-api-version'), os.environ.get('GRAPHRAG_API_VERSION'), os.environ.get('OPENAI_API_VERSION')] if v is not None), '2024-02-01')
        llm_api_retries=next((v for v in [args.get('--openai-api-retries'), os.environ.get('GRAPHRAG_API_RETRIES'), os.environ.get('OPENAI_API_RETRIES')] if v is not None), 3)
        llm_org_api=next((v for v in [args.get('--ad-org'), os.environ.get('GRAPHRAG_AD_ORG_ID'), os.environ.get('AD_ORG_ID')] if v is not None), None)
        llm = ChatOpenAI(
            api_key=llm_api_key,
            api_base=llm_api_base,
            azure_ad_token_provider=(get_bearer_token_provider(DefaultAzureCredential()) if not llm_api_key else None),
            organization=llm_org_api,
            model=llm_model,
            deployment_name=llm_model,
            api_type=OpenaiApiType.AzureOpenAI,
            api_version=llm_api_version,
            max_retries=llm_api_retries,
        )

    ## Determine parser type
    parser_type = args.get("--parser", os.getenv('PARSER', "pdfdocintel")).lower()

    # Create the parser
    if parser_type == "pdf":
        parser = PdfParser(config)
    elif parser_type == "pdfdocintel":
        parser = PdfDocIntelligenceParser(config, llm)
    elif parser_type == "docintelligence":
        parser = DocIntelligenceParser(config)
    else:
        print(f"Unknown parser type: {parser_type}")
        return

    # Determine File path
    file_path = args.get("--file", os.getenv('FILE_PATH', None))
    if file_path is None:
        print("No file path provided - you must specify a file using '--file', eg:")
        print("python parse-file.py --file=<file_path>")
        return

    print(f"Parsing file: {file_path}")
    result = parser.parse(file_path)

    ## Load the LLM Library
    should_analyse_images = next((v for v in [args.get('--analyse-images'), os.environ.get('ANALYSE_IMAGES')] if v is not None), True)
    if should_analyse_images and result.pre_parsed_md is None:
        analyse_img_custom_msg = args.get('--custom-analyse-image-prompt', None)
        def progress_notifier(chunk:DocumentChunk, msg:str, progress:float):
            if msg == "started":
                print(f"[{int(progress*100.0)}%] Analysing image {chunk.page_chunk_idx} on Page: {chunk.page}")
        print("Analysing Images within file...")
        result.analyse_images(llm=llm, custom_analysis_msg=analyse_img_custom_msg, progress_notifier=progress_notifier)      ## Analyse the images into text

    ## Directory to save the output to (default: input - which seems like a bad default, but it's the typical location where graphrag ingests files from)
    output_path = Path(args.get("--output-dir", os.getenv('OUTPUT_DIR', "input")))
    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)

    ## Save the result to a file
    data_file_path = Path(file_path)

    save_markdown = args.get("--markdown", os.getenv('SAVE_MARKDOWN', "true")).lower() in ["true", "yes", "1"]
    save_json = args.get("--json", os.getenv('SAVE_JSON', "true")).lower() in ["true", "yes", "1"]

    if save_markdown:
        markdown_output_file = output_path / f"{data_file_path.stem}.md"
        print(f"Writing Markdown Representation to :{markdown_output_file}")
        with open(markdown_output_file, 'w') as f:
            f.write(result.to_markdown())

    if save_json:
        json_output_file = output_path / f"{data_file_path.stem}.json"
        print(f"Writing JSON Representation to :{json_output_file}")
        with open(json_output_file, 'w') as f:
            f.write(json.dumps(result.to_json(), indent=4))

    print(f"Done!")


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