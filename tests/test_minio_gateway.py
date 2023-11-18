import tempfile
from app.sdk.minio_gateway import MinIOGateway

def test_minio_gateway(minio_config):
    minio = MinIOGateway(
        host=minio_config["host"],
        port=minio_config["port"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )
    assert minio.user == minio_config["access_key"]

def test_minio_gateway_get_client(minio):
    client = minio.get_client()
    assert client is not None

def test_minio_gateway_create_bucket_if_not_exists(minio):
    bucket_name = "test-bucket"
    minio.create_bucket_if_not_exists(bucket_name)
    assert bucket_name in minio.list_buckets()

def test_minio_gateway_list_objects(minio: MinIOGateway):
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"test")
        f.seek(0)
        bucket_name = "test-bucket"
        object_name = "test-object"
        minio.upload_file(bucket_name, object_name, f.name)
        objects = minio.list_objects(bucket_name)
        assert object_name in objects

def test_download_file(minio: MinIOGateway):
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"test")
        f.seek(0)
        bucket_name = "test-bucket"
        object_name = "test-object"
        minio.upload_file(bucket_name, object_name, f.name)
        minio.download_file(bucket_name, object_name, f.name)
        assert f.read() == b"test"
