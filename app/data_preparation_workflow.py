from app.sdk.workflow_executor import BaseWorkflowExecutor, WorkfowArgs


class DataPreparationWorkflowArgs(WorkfowArgs):
    pass


class DataPreparationWorkflow(BaseWorkflowExecutor):
    def __init__(
        self,
        tracer_key: str,
        kp_host: str,
        kp_port: str,
        minio_host: str,
        minio_port: str,
        minio_bucket: str,
        minio_access_key: str,
        minio_secret_key: str,
    ) -> None:
        super().__init__(
            tracer_key,
            kp_host,
            kp_port,
            minio_host,
            minio_port,
            minio_bucket,
            minio_access_key,
            minio_secret_key,
        )

    async def run(self, args: WorkerArgs) -> None:
        pass
