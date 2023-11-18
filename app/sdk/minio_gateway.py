from minio import Minio # type: ignore


class MinIOGateway:
    def __init__(self, host: str, port: str, root_user: str, password: str) -> None:
        self._user = root_user
        self._password = password
        self._url = f"{host}:{port}"

    @property
    def user(self) -> str:
        return self._user
    
    @property
    def password(self) -> str:
        return self._password
    
    @property
    def url(self) -> str:
        return self._url
    
    def get_client(self) -> Minio:
        client = Minio(
            self.url,
            access_key=self.user,
            secret_key=self.password,
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
        return [object.name for object in objects]
    
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
