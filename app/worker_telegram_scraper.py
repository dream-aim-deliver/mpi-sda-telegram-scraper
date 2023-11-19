from app.sdk.background import BackgroundWorker
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_repository import MinIORepository


class TelegramScraperWorker(BackgroundWorker):
    def __init__(
        self,
        tracer_key: str,
        kp_gateway: KernelPlancksterGateway,
        minio_repository: MinIORepository,
    ):
        super().__init__(tracer_key, kp_gateway, minio_repository)
