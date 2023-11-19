from minio import Minio  # type: ignore

from app.sdk.models import LFN, DataSource, Protocol


class MinIORepository:
    def __init__(
        self,
        host: str = "localhost",
        port: str = "9000",
        access_key: str = "minio",
        secret_key: str = "minio123",
        bucket: str = "default",
    ) -> None:
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._host = host
        self._port = port

    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> str:
        return self._port

    @property
    def bucket(self) -> str:
        return self._bucket

    def get_client(self) -> Minio:
        client = Minio(
            self.url,
            access_key=self._access_key,
            secret_key=self._secret_key,
            secure=False,
        )
        return client

    def create_bucket_if_not_exists(self, bucket_name: str) -> None:
        if bucket_name in self.list_buckets():
            return
        client = self.get_client()
        client.make_bucket(bucket_name)

    def list_buckets(self) -> list[str]:
        client = self.get_client()
        buckets = client.list_buckets()
        return [bucket.name for bucket in buckets]

    def list_objects(self, bucket_name: str) -> list[str]:
        client = self.get_client()
        objects = client.list_objects(bucket_name)
        objects = list(objects)
        return [obj.object_name for obj in objects]

    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Upload a file to an object in a bucket.

        :param bucket_name: The name of the bucket.
        :param object_name: The name of the object.
        :param file_path: The path to the file to upload.
        """
        client = self.get_client()
        client.fput_object(bucket_name, object_name, file_path)

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Download a file from an object in a bucket.

        :param bucket_name: The name of the bucket.
        :param object_name: The name of the object.
        :param file_path: The path to the file to download to.
        """
        client = self.get_client()
        client.fget_object(bucket_name, object_name, file_path)

    def lfn_to_pfn(self, lfn: LFN) -> str:
        """
        Generate a PFN for MinIO S3 Repository from a LFN.

        :param lfn: The LFN to generate a PFN for.
        :type lfn: LFN
        :raises ValueError: If the LFN protocol is S3.
        :return: The PFN.
        """
        if lfn.protocol == Protocol.S3:
            return f"s3://{self.host}:{self.port}/{self.bucket}/{lfn.tracer_id}/{lfn.source.value}/{lfn.job_id}/{lfn.relative_path}"
        raise ValueError(
            f"Protocol {lfn.protocol} is not supported by MinIO Repository. Cannot create a PFN for LFN {lfn}."
        )

    def pfn_to_lfn(self, pfn: str):
        """
        Generate a LFN from a PFN for MinIO S3 Repository.

        :param pfn: The PFN to generate a LFN for.
        :type pfn: str
        :raises ValueError: If the PFN protocol is S3.
        :return: The LFN.
        """
        if pfn.startswith(f"s3://{self.host}:{self.port}/{self.bucket}/"):
            path = pfn.split("://")[1].split("/")
            if len(path) < 6:
                raise ValueError(
                    f"Path {pfn} is not supported by this MinIO Repository at {self.url}. Cannot create a LFN for PFN {pfn}."
                )
            return LFN(
                protocol=Protocol.S3,
                tracer_id=path[2],
                source=DataSource(path[3]),
                job_id=int(path[4]),
                relative_path="/".join(path[5:]),
            )
        raise ValueError(
            f"Path {pfn} is not supported by this MinIO Repository at {self.url}. Cannot create a LFN for PFN {pfn}."
        )
