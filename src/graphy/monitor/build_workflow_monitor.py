from pathlib import Path
import datetime
import gc 

from datashaper.progress import Progress
from datashaper.workflow.workflow_callbacks import WorkflowCallbacks
from datashaper.execution import ExecutionNode
from datashaper.table_store.types import TableContainer
from graphrag.index.config import PipelineConfig


class WorkflowMonitor(WorkflowCallbacks):
    """A Workflow Reporter that records workflow progress information into 'workflow_monitor' and 'workflow_steps' files the artifacts directory"""

    def __init__(self, pipeline_config:PipelineConfig):
        self.pipeline_config = pipeline_config
        self.output_file = None
        self.output_steps = None
        self.current_workflow = None
        self.current_step = None
        self._recent_progress = None
        self._recent_error = None
        self._recent_log = None
        self._recent_measure = None

    def init_files(self):
        print("Initializing files...", flush=True)
        self.output_file = Path(self.pipeline_config.storage.base_dir) / "workflow_monitor.txt"
        self.output_steps = Path(self.pipeline_config.storage.base_dir) / "workflow_steps.txt"

    def stop(self): 
        print("Stopping...", flush=True)
        print("Current Workflow: ", self.current_workflow)
        print("Current Step: ", self.current_step)
        print("Recent Progress: ", self._recent_progress)
        print("Recent Error: ", self._recent_error)
        print("Recent Log: ", self._recent_log)
        print("Recent Measure: ", self._recent_measure)


    def on_workflow_start(self, name: str, instance: object) -> None:
        """Execute this callback when a workflow starts."""
        if self.output_file is None:
            self.init_files()

        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [START] Workflow {name}\n")

        self.current_workflow = instance
        if self.current_workflow is not None and self.current_workflow._schema is not None:
            workflow_steps = self.current_workflow._schema.get('steps', [])
            with self.output_steps.open("a") as f:
                f.write(f"{datetime.datetime.now()} - [START] Workflow {name}\n")
                for step in workflow_steps:
                    f.write(f"  -- {step.get('verb')}\n")
        else: 
            with self.output_steps.open("a") as f:
                f.write(f"{datetime.datetime.now()} - [END] Workflow {name}\n")

        
    def on_workflow_end(self, name: str, instance: object) -> None:
        """Execute this callback when a workflow ends."""
        if self.output_file is None:
            self.init_files()

        self.current_workflow = None
        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [END] Workflow {name}\n")
        with self.output_steps.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [END] Workflow {name}\n")


    def on_step_start(self, node: ExecutionNode, inputs: dict) -> None:
        """Execute this callback every time a step starts."""
        if self.output_file is None:
            self.init_files()
        
        self.current_step = node

        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [START] Step {node.verb}: {node.node_id} - [{node.node_input}]\n")
        with self.output_steps.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [START] Step {node.verb}: {node.node_id} - [{node.node_input}]\n")
        
        gc.collect()

    def on_step_end(self, node: ExecutionNode, result: TableContainer | None) -> None:
        """Execute this callback every time a step ends."""
        if self.output_file is None:
            self.init_files()

        next_step = None
        if self.current_workflow is not None and self.current_workflow._schema is not None:
            workflow_steps = self.current_workflow._schema.get('steps', [])
            for (idx, step) in enumerate(workflow_steps):
                if step.get('verb') == node.verb.name:
                    if idx < len(workflow_steps) - 1:
                        next_step = workflow_steps[idx + 1]
                        break
            
        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [END] Step {node.verb}: {node.node_id} - Next Step: {next_step or 'None'}\n")
        with self.output_steps.open("a") as f:
            f.write(f"{datetime.datetime.now()} - [END] Step {node.verb}: {node.node_id} - Next Step: {next_step or 'None'}\n")

    def on_step_progress(self, node: ExecutionNode, progress: Progress) -> None:
        """Handle when progress occurs."""
        self._recent_progress = node.verb.name + " - " + str(progress.percent)

    def on_error(
        self,
        message: str,
        cause: BaseException | None = None,
        stack: str | None = None,
        details: dict | None = None,
    ) -> None:
        """Handle when an error occurs."""
        if self.output_file is None:
            self.init_files()

        self._recent_error = message

        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - Error: {message}\n")

    def on_warning(self, message: str, details: dict | None = None) -> None:
        """Handle when a warning occurs."""
        if self.output_file is None:
            self.init_files()

        self._recent_error = message

        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - Warning: {message}\n")

    def on_log(self, message: str, details: dict | None = None) -> None:
        """Handle when a log message occurs."""
        if self.output_file is None:
            self.init_files()

        self._recent_log = message

        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - Log: {message}\n")

    def on_measure(self, name: str, value: float, details: dict | None = None) -> None:
        """Handle when a measurement occurs."""
        if self.output_file is None:
            self.init_files()

        self._recent_measure = name + " - " + str(value)
        with self.output_file.open("a") as f:
            f.write(f"{datetime.datetime.now()} - Measure: {name} - {value}\n")

