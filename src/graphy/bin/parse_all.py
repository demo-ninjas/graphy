#!/usr/bin/env python

from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
import asyncio
import sys
import os
import json

import dotenv
dotenv.load_dotenv(".env")

from azure.identity import DefaultAzureCredential, get_bearer_token_provider

from graphrag.query.llm.oai.chat_openai import ChatOpenAI
from graphrag.query.llm.oai.embedding import OpenAIEmbedding
from graphrag.query.llm.oai.typing import OpenaiApiType


from graphy.parser import Parser, DocumentChunk, PdfDocIntelligenceParser, PdfParser, DocIntelligenceParser

def parse_file(file:Path, parser:Parser, target_dir:Path, llm:ChatOpenAI, custom_analyse_image_prompt:str, save_markdown:bool, save_json:bool, print_logs:bool, force:bool=False) -> bool:
    prefix = f"[{file.name}] "
    try:
        processed_output_file = target_dir / f"{file.stem}.processed"
        if not force and processed_output_file.exists():
            if print_logs:
                print(f"{prefix} Already processed, skipping...")
            return True

        ## Parse the File
        if print_logs:
            print(f"{prefix} Parsing File")
        result = parser.parse(file)

        ## Analyse the images within the file (if desired)
        if llm is not None: # Aka. We're analysing the images
            def progress_notifier(chunk:DocumentChunk, msg:str, progress:float):
                if print_logs and msg == "started":
                    print(f"{prefix} ({int(progress*100.0)}%) Analysing image {chunk.page_chunk_idx} on Page: {chunk.page}")
            if print_logs:
                print(f"{prefix} Analysing Images within file...")            
            result.analyse_images(llm=llm, custom_analysis_msg=custom_analyse_image_prompt, progress_notifier=progress_notifier)      ## Analyse the images into text

        ## Save the Markdown
        if save_markdown:
            markdown_output_file = target_dir / f"{file.stem}.md"
            if print_logs:
                print(f"{prefix} Writing Markdown Representation")
            with open(markdown_output_file, 'w') as f:
                f.write(result.to_markdown())

        ## Save the JSON
        if save_json:
            json_output_file = target_dir / f"{file.stem}.json"
            if print_logs:
                print(f"{prefix} Writing JSON Representation")
            with open(json_output_file, 'w') as f:
                f.write(json.dumps(result.to_json(), indent=4))

        ## Save the processed file
        with open(processed_output_file, 'w') as f:
            f.write("")
        
        if print_logs:
            print(f"{prefix} Done!")

        return True
    except Exception as e: 
        print(f"{prefix} Failed to process file - Error: {e}")
        return False

async def main():
    args = _parse_args()

    if "--help" in args:
        print("Usage: parse-all --source=<source_dir> --target=<target_dir> --markdown=<true|false> --json=<true|false> --concurrency=<num_threads>")
        print("")
        print("Options:")
        print("  --source=<source_dir>                  The source directory to read files from (default: 'source')")
        print("  --target=<target_dir>                  The target directory to save the parsed files to (default: 'input')")
        print("  --config-<key>=<value>                 Set a configuration option (eg: --config-min-chunk-chars=100)")
        print("  --pdf-parser=<parser>                  The parser to use for PDF Files (pdf or pdfdocintel or docintel; default: pdfdocintel)")
        print("  --doc-parser=<parser>                  The parser to use for non-PDF Files (pdf or pdfdocintel or docintel; default: docintel)")
        print("  --markdown=<true|false>                Whether to save the markdown representation (default: true)")
        print("  --json=<true|false>                    Whether to save the json representation (default: true)")
        print("  --concurrency=<num_threads>            The number of threads to use for processing (default: 4)")
        print("  --analyse-images=<true|false>          Whether to analyse images within the document (default: true)")
        return
    
    print("Initialising...")
    # Load the configuration (currently not offering option of modifying from defaults)
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
    pdf_parser_type = args.get("--pdf-parser", os.getenv('PARSER', "docintelligence")).lower()
    doc_parser_type = args.get("--doc-parser", os.getenv('PARSER', "docintelligence")).lower()

    # Create the parser
    pdf_parser = None
    doc_parser = None

    ## Parser for PDF Documents
    if pdf_parser_type == "pdf" or pdf_parser_type == "native":
        pdf_parser = PdfParser(config)
    elif pdf_parser_type == "pdfdocintel" or pdf_parser_type == "smart":
        pdf_parser = PdfDocIntelligenceParser(config, llm)
    elif pdf_parser_type == "docintel" or pdf_parser_type == "docintelligence":
        pdf_parser = DocIntelligenceParser(config)
    else:
        print(f"Unknown parser type for PDF Parser: {pdf_parser_type}")
        return

    ## Parser for non PDF Documents    
    if doc_parser_type == "pdf":
        doc_parser = PdfParser(config)
    elif doc_parser_type == "pdfdocintel" or doc_parser_type == "smart":
        doc_parser = PdfDocIntelligenceParser(config, llm)
    elif doc_parser_type == "docintelligence" or doc_parser_type == "docintel":
        doc_parser = DocIntelligenceParser(config)
    else:
        print(f"Unknown parser type for Doc Parser: {doc_parser_type}")
        return
    
    ## Determine Source Directory
    source_dir = Path(args.get("--source", os.getenv('SOURCE_DIR', "source")))

    ## Determine Target Directory (Defaults to "input" - this might seem like a bad default, but it's the typical location where graphrag ingests files from)
    target_dir = Path(args.get("--target", os.getenv('TARGET_DIR', "input")))
    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
    
    ## Whether or not to save the markdown + json outputs
    save_markdown = args.get("--markdown", os.getenv('SAVE_MARKDOWN', "true")).lower() in ["true", "yes", "1"]
    save_json = args.get("--json", os.getenv('SAVE_JSON', "true")).lower() in ["true", "yes", "1"]

    ## Should force reprocessing of files
    force = ('--force' in args and args.get("--force")) or os.getenv('FORCE', "false").lower() in ["true", "yes", "1"]


    ## Iterate over the files in the "source" directory
    print(f"Processing files in {source_dir}...")
    concurrency = int(args.get("--concurrency", os.getenv('CONCURRENCY', 4)))
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = list[Future]()
        for file in source_dir.iterdir():
            if file.is_file() and file.suffix.lower() in [".pdf", ".docx", ".html", ".xls", ".xlsx", ".pptx", ".ppt"]:
                parser = pdf_parser if file.suffix.lower() == '.pdf' else doc_parser
                futures.append(executor.submit(parse_file, file, parser, target_dir, llm, None, save_markdown, save_json, True, force))
            else: 
                print(f"Skipping file: {file.name} - Unsupported file type")
        executor.shutdown(wait=True)

        success_count = 0
        fail_count = 0
        for f in futures:
            if f.result() == True: 
                success_count += 1
            else: 
                fail_count += 1

    print(f"Done, {success_count} files processed successfully, {fail_count} files failed to be processed.")

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