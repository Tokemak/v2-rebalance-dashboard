import boto3
from botocore.config import Config
from mainnet_launch.constants import ALL_AUTOPOOLS
from botocore import UNSIGNED


def test_s3_bucket_access():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in ALL_AUTOPOOLS:
        bucket_name = autopool.solver_rebalance_plans_bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        assert response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
