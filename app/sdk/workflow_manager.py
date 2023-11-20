import logging
from typing import Any, Dict, List

from fastapi import APIRouter, BackgroundTasks, FastAPI, HTTPException, status, Depends
from pydantic import BaseModel
from app.placeholder_job import workflow_executor_wrapper
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository
from app.sdk.models import BaseWorkflow
from app.sdk.workflow_executor import BaseWorkflowExecutor

logger = logging.getLogger(__name__)


class BaseWorkflowManager:
    def __init__(
        self,
        name: str,
        app: FastAPI,
        kernel_plankster: KernelPlancksterGateway,
        minio: MinIORepository,
    ) -> None:
        self._name: str = name
        self._nonce: int = 0
        self._app = app
        self._workflows: Dict[int, BaseWorkflow] = {}
        self._router = APIRouter(prefix=f"/{self._name}", tags=[self._name])
        self._kernel_planckster = kernel_plankster
        self._minio = minio
        self.route_create_workflow()
        self.route_get_workflow()
        self.route_execute_workflow()
        self.route_list_workflows()
        self._app.include_router(self._router)

    @property
    def name(self) -> str:
        return self._name

    @property
    def nonce(self) -> int:
        self._nonce += 1
        return self._nonce

    @property
    def workflows(self) -> Dict[int, BaseWorkflow]:
        return self._workflows

    @property
    def router(self) -> APIRouter:
        return self._router

    @property
    def kernel_plankster(self) -> KernelPlancksterGateway:
        return self._kernel_planckster

    @property
    def minio(self) -> MinIORepository:
        return self._minio

    def _create_workflow(self, tracer_key: str, **kwargs) -> BaseWorkflow:
        id = self.nonce
        workflow = BaseWorkflow(
            id=id, name=self.name, job_args=kwargs, tracer_key=tracer_key
        )
        self.workflows[id] = workflow
        return workflow

    def _list_workflows(self, *args, **kwargs) -> List[BaseWorkflow]:
        self.log(level=logging.INFO, message=f"{self.workflows}")
        return list(self.workflows.values())

    def _get_workflow(self, workflow_id: int) -> BaseWorkflow:
        return self.workflows[workflow_id]

    def route_create_workflow(self):
        @self.router.post(
            f"/",
            name="Create a new Workflow",
            response_model=BaseWorkflow,
        )
        def create_workflow(tracer_key: str, **kwargs: Dict[str, Any]):
            workflow = self._create_workflow(tracer_key=tracer_key, **kwargs)
            return workflow

    def route_list_workflows(self):
        @self.router.get(
            "/",
            name="List all Workflows",
            response_model=List[BaseWorkflow],
        )
        def list_workflows():
            return self._list_workflows()

    def route_get_workflow(self):
        @self.router.get(
            "/{workflow_id}",
            name="Get details about a  Workflow",
            response_model=BaseWorkflow,
        )
        def get_workflow(workflow_id: int):
            try:
                return self._get_workflow(workflow_id)
            except KeyError:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Workflow {workflow_id} not found",
                )

    def route_execute_workflow(self):
        @self.router.post(
            "/{workflow_id}",
            name="Execute a Workflow",
            response_model=BaseWorkflow,
        )
        def start_workflow(
            workflow_id: int, background_tasks: BackgroundTasks
        ):  # TIP: Here we are awaiting workflow completions
            try:
                workflow = self._get_workflow(workflow_id)
                executor = self.create_workflow_executor(workflow=workflow)
                # self.execute_workflow(workflow=workflow)
                background_tasks.add_task(workflow_executor_wrapper, workflow=workflow)
                return workflow
            except KeyError:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Workflow {workflow_id} not found",
                )

    def log(self, level: int, message: str):
        msg = f"[{self.name}] {message}"
        logger.log(level, msg)

    def create_workflow_executor(
        self, workflow: BaseWorkflow, *args, **kwargs
    ) -> BaseWorkflowExecutor:
        raise NotImplementedError(
            "create_executor method must be implemented for your workflow!!"
        )

    async def execute_workflow(self, workflow: BaseWorkflow, *args, **kwargs):
        executor = self.create_workflow_executor(workflow=workflow, **kwargs)
        await executor._execute(*args, **kwargs)
