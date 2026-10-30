"""테스트 공용 픽스처."""

import pytest

pytest_plugins = ("core.testing",)


@pytest.fixture(autouse=True)
def _mock_s3(s3_bucket):
    """모든 테스트를 moto S3 bucket 안에서 실행한다."""
