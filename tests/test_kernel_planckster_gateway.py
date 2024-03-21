import os
import tempfile
import requests
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.models import LFN, KnowledgeSourceEnum, ProtocolEnum


def test_generate_signed_url(
    kernel_planckster_gateway: KernelPlancksterGateway,
    test_file_path: str,
) -> None:

    kernel_planckster = kernel_planckster_gateway

    tmp_file = test_file_path

    test_media_lfn: LFN = LFN(
        protocol=ProtocolEnum.S3,
        tracer_id="test_tracer_id",
        job_id=1,
        source=KnowledgeSourceEnum.TELEGRAM,
        relative_path=tmp_file,
    )

    signed_url = kernel_planckster.generate_signed_url(test_media_lfn)

    # Test signed url works
    with open(tmp_file, "rb") as f:
        upload_res = requests.put(signed_url, data=f)

    assert upload_res.status_code == 200

    # Clean up
    os.remove(tmp_file) 


def test_register_new_source_data(
    kernel_planckster_gateway: KernelPlancksterGateway,
    test_file_path: str,
) -> None:

    kernel_planckster = kernel_planckster_gateway

    tmp_file = test_file_path

    test_media_lfn: LFN = LFN(
        protocol=ProtocolEnum.S3,
        tracer_id="test_tracer_id",
        job_id=1,
        source=KnowledgeSourceEnum.TELEGRAM,
        relative_path=tmp_file,
    )

    signed_url = kernel_planckster.generate_signed_url(test_media_lfn)

    # Test signed url works
    with open(tmp_file, "rb") as f:
        upload_res = requests.put(signed_url, data=f)

    assert upload_res.status_code == 200

    # Test registering new source data
    kp_sd = kernel_planckster.register_new_source_data(test_media_lfn)

    assert kp_sd is not None
    assert kp_sd["lfn"] == test_media_lfn

    # Clean up
    os.remove(tmp_file)