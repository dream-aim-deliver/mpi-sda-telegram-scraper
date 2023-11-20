import logging
from typing import Any, Dict

from fastapi import FastAPI
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository
from app.sdk.workflow_manager import BaseWorkflow, BaseWorkflowManager
from app.sdk.workflow_executor import BaseWorkflowExecutor


class DataPreparationWorkflowManager(BaseWorkflowManager):
    def __init__(self, app: FastAPI):
        super().__init__(
            "sda_data_processing",
            app=app,
            kernel_plankster=app.kernel_plankster,  # type: ignore
            minio=app.minio_repository,  # type: ignore
        )

    def create_workflow_executor(
        self, workflow: BaseWorkflow, *args, **kwargs
    ) -> "DataPreparationWorkflowExecutor":
        if "telegram_channel" not in workflow.job_args:
            raise ValueError(
                "Invalid workflow! Missing required argument: telegram_channel"
            )
        # For Alpha, we are hardcoding the telegram channel to be used
        telegram_channel = workflow.job_args.get("telegram_channel", "GCC_report")
        executor = DataPreparationWorkflowExecutor(
            kernel_plankster=self.kernel_plankster,
            workflow=workflow,
            minio=self.minio,
            telegram_channel=telegram_channel,
        )
        return executor

    def route_create_workflow(self):
        @self.router.post(
            "/",
            name="Create a new Data Preparation Workflow",
        )
        def create_workflow(
            tracer_key: str,  # TIP: This is ALWAYS required to trace your workflow files across the system
            telegram_channel: str = "GCC_report",  # TIP: You can request explicit values for your workflow
            workflow_args: Dict[str, Any] = {
                "version": "1.0.0-alpha"
            },  # TIP: Here you can pass additional values that will be available in the workflow context
        ) -> BaseWorkflow:
            return self._create_workflow(
                tracer_key=tracer_key,
                telegram_channel=telegram_channel,
                **workflow_args,
            )


class DataPreparationWorkflowExecutor(BaseWorkflowExecutor):
    def __init__(
        self,
        workflow: BaseWorkflow,
        telegram_channel: str,
        kernel_plankster: KernelPlancksterGateway,
        minio: MinIORepository,
    ):
        super().__init__(
            workflow=workflow, kernel_plankster=kernel_plankster, minio=minio
        )
        self.telegram_channel = telegram_channel

    async def run(self, *args, **kwargs):
        """
        This is where you will implement your workflow logic.
        """
        self.log("***********************************************")
        print("START HERE")
        self.log(level=logging.INFO, message=f"Args: {args}")
        self.log(level=logging.INFO, message=f"Kwargs: {kwargs}")
