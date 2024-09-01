import subprocess
import os
from src.error_handler import KNIMEError
from config.settings import KNIME_WORKFLOW_DIR

class KNIMERunner:
    def __init__(self, knime_executable):
        self.knime_executable = knime_executable

    def run_workflow(self, params=None):
        try:
            command = [
                self.knime_executable,
                "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
                "-workflowDir", KNIME_WORKFLOW_DIR,
                "-reset",
                "-nosplash",
                "-nosave",
            ]
            
            if params:
                for key, value in params.items():
                    command.extend(["-workflow.variable", f"{key},{value},String"])

            result = subprocess.run(command, check=True, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            error_message = f"KNIME workflow execution failed. Exit code: {e.returncode}. Error output: {e.stderr}"
            raise KNIMEError(error_message)