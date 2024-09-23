from pathlib import Path
import atexit

from graphrag.index import run_pipeline_with_config
from graphrag.index import create_pipeline_config

from graphrag.index.emit import TableEmitterType

from graphy.monitor.build_progress_reporter import BuildProgressReporter
from graphy.monitor.build_workflow_monitor import WorkflowMonitor
from .parser import DocumentParser

def parse_file(file:Path, parser:DocumentParser, target_dir:Path, save_markdown:bool, save_json:bool, print_logs:bool=False):
    file_id = file.stem
    output_file_name = file_id.replace(" ", "-").replace("--", "-").lower()
    
    marker_file = target_dir / f"{output_file_name}.json"
    if marker_file.exists():
        if print_logs:
            print(f"Skipping: {file.name}...")
        return

    try: 
        ## Parse the document
        if print_logs:
            print(f"Processing: {file.name}...")
        parsed_document = parser.parse(file.absolute().as_posix())

        if parsed_document.title is None:
            parsed_document.title = file_id

        if save_markdown:
            ## Write out a raw MD Text file
            content = parsed_document.as_text()
            with open(target_dir / f"{output_file_name}.md", "w") as f:
                f.write(content)
        
        if save_json:
            ## Write out a JSON file
            with open(marker_file, "w") as f:
                import json
                f.write(json.dumps(parsed_document.to_dict(), indent=4))

        if print_logs:
            print(f"Finished Processing: {file.name}.")
    except Exception as e:
        if print_logs:
            print(f"Error processing: {file.name}, Error: {e}")
        with open(target_dir / f"{output_file_name}.error", "w") as f:
            f.write(str(e) + "\n\n")
            import traceback
            traceback.print_exc(file=f)

from graphrag.config import GraphRagConfig

async def build_graph(config:GraphRagConfig, resume_run:bool=False, run_id:str|None=None, report_progress_to_console:bool=False):
    try: 
        ## Create the Pipeline Config
        pipeline_config = create_pipeline_config(config)

        if report_progress_to_console:
            print("Workflow Pipeline: ")
            for workflow in pipeline_config.workflows:
                print(f"- {workflow.name}")
                if workflow.steps is not None:  ## Note: Steps are not likely to have been loaded yet - so this will usually be empty
                    for step in workflow.steps:
                        print(f"  -- {step.verb} {step.node_id}")
            print("\n")
            
        ## Create the Workflow Monitors
        callback = WorkflowMonitor(pipeline_config)
        progress_reporter = None
        if report_progress_to_console:
            progress_reporter = BuildProgressReporter(prefix="Progress:")

        ## Register a cleanup function that prints out data from the monitor if the program is stopped before the workflow has completed
        outputs:list[dict] = []
        workflow_completed = False
        def and_thats_a_wrap():
            if not workflow_completed: 
                print("Cleaning up...", flush=True)
                if callback is not None:
                    callback.stop()
        atexit.register(and_thats_a_wrap)
        
        ## Run the pipeline
        if report_progress_to_console:
            if run_id is not None: print("Resuming pipeline: " + run_id)
            else: print("Running pipeline...")

        async for output in run_pipeline_with_config(
                                config_or_path=pipeline_config, 
                                is_resume_run=resume_run, 
                                run_id=run_id,
                                callbacks=callback,
                                progress_reporter=progress_reporter, 
                                emit=[ TableEmitterType.Parquet ], 
                                workflows=pipeline_config.workflows
                            ):
            
            if report_progress_to_console:
                print(f"Writing Workflow '{output.workflow}' to artifacts dir")
            outputs.append({
                "workflow": output.workflow,
                "has_result": output.result is not None,
                "has_errors": output.errors is not None
            })
            if output.result is not None:
                with open(f"{config.storage.base_dir}/{output.workflow}-result.json", "w") as f:
                    try:
                        output.result.to_json(f, indent=4)
                    except Exception as e:
                        print(e)

            if output.errors is not None:
                with open(f"{config.storage.base_dir}/{output.workflow}-errors.json", "w") as f:
                    try:
                        import json
                        json.dump(output.errors, f, indent=4)
                    except Exception as e:
                        print(e)

        if report_progress_to_console:
            print("Pipeline Completed - here's the outcome of each pipeline workflow:")
        for output in outputs:
            print(f"- {output.get('workflow')} [Has Result: {output.get('has_result')}, Has Errors: {output.get('has_errors')}]")
        
        ## Pipeline Done
        workflow_completed = True
        if report_progress_to_console:
            print("Done.")
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        traceback.print_exc()