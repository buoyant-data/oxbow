"""
Shared fixtures for the oxbow end-to-end acceptance tests.

The tests expect LocalStack (see compose.yml) to be running with S3, Lambda,
SQS, DynamoDB and IAM enabled, and the lambda artifacts to exist under
target/lambda/ (see acceptance/run.sh, which builds them if needed).
"""

import json
import os
import time
import uuid
from pathlib import Path

import boto3
import botocore
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = REPO_ROOT / "tests" / "data" / "hive" / "deltatbl-non-partitioned"

REGION = "us-east-1"
ENDPOINT = os.environ.get(
    "LOCALSTACK_ENDPOINT",
    f"http://127.0.0.1:{os.environ.get('LOCALSTACK_PORT', '4567')}",
)

# `cargo lambda build --lambda-dir target/lambda/x86-64` writes each package to
# target/lambda/x86-64/<package>/bootstrap.zip (see acceptance/run.sh).
OXBOW_ZIP = Path(
    os.environ.get(
        "OXBOW_ZIP", REPO_ROOT / "target/lambda/x86-64/oxbow-lambda/bootstrap.zip"
    )
)
SQS_INGEST_ZIP = Path(
    os.environ.get(
        "SQS_INGEST_ZIP", REPO_ROOT / "target/lambda/x86-64/sqs-ingest/bootstrap.zip"
    )
)

# Name of the DynamoDB table oxbow uses for the commit lock (see
# crates/buoyant-oxbow/src/lock.rs and the README's env var table).
LOCK_TABLE = "delta_rs_lock_table"
# Table used by delta-rs' DynamoDB S3 locking provider.
DELTA_LOG_TABLE = "delta_log"

# Lambda containers run on the compose network, where the host's 127.0.0.1 is
# not LocalStack. They reach it through the `localstack` network alias.
LAMBDA_ENDPOINT = os.environ.get("LAMBDA_ENDPOINT", "http://localstack:4566")

ROLE_ARN = "arn:aws:iam::000000000000:role/oxbow-acceptance"


def _client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def s3():
    return _client("s3")


@pytest.fixture(scope="session")
def dynamodb():
    return _client("dynamodb")


@pytest.fixture(scope="session")
def sqs():
    return _client("sqs")


@pytest.fixture(scope="session")
def aws_lambda():
    return _client("lambda")


@pytest.fixture(scope="session", autouse=True)
def localstack_ready():
    """Fail fast with a useful message when LocalStack is not running."""
    import urllib.request

    try:
        with urllib.request.urlopen(f"{ENDPOINT}/_localstack/health", timeout=5) as r:
            body = json.loads(r.read())
    except Exception as exc:  # noqa: BLE001
        pytest.exit(
            f"LocalStack is not reachable at {ENDPOINT} ({exc}). "
            "Start it with `podman compose up -d`.",
            returncode=2,
        )
    services = body.get("services", {})
    missing = [s for s in ("s3", "lambda", "sqs", "dynamodb") if s not in services]
    if missing:
        pytest.exit(f"LocalStack is missing services: {missing}", returncode=2)


@pytest.fixture(scope="session")
def lock_tables(dynamodb):
    """Create the DynamoDB tables oxbow uses for locking."""
    _ensure_table(
        dynamodb,
        LOCK_TABLE,
        key_schema=[{"AttributeName": "key", "KeyType": "HASH"}],
        attributes=[{"AttributeName": "key", "AttributeType": "S"}],
    )
    _ensure_table(
        dynamodb,
        DELTA_LOG_TABLE,
        key_schema=[
            {"AttributeName": "tablePath", "KeyType": "HASH"},
            {"AttributeName": "fileName", "KeyType": "RANGE"},
        ],
        attributes=[
            {"AttributeName": "tablePath", "AttributeType": "S"},
            {"AttributeName": "fileName", "AttributeType": "S"},
        ],
    )


def _ensure_table(dynamodb, name, key_schema, attributes):
    try:
        dynamodb.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attributes,
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException:
        pass


@pytest.fixture
def bucket(s3):
    """A fresh bucket per test, removed afterwards."""
    name = f"oxbow-acc-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=name)
    yield name
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=name, Delete={"Objects": objects})
    s3.delete_bucket(Bucket=name)


def upload_fixture_table(s3, bucket, prefix):
    """Copy the non-partitioned senators fixture into s3://bucket/prefix."""
    for path in FIXTURES.rglob("*"):
        if path.is_file():
            key = f"{prefix}/{path.relative_to(FIXTURES).as_posix()}"
            s3.upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{prefix}"


def list_keys(s3, bucket, prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def log_versions(s3, bucket, prefix):
    """Sorted commit versions present in the table's _delta_log."""
    versions = []
    for key in list_keys(s3, bucket, f"{prefix}/_delta_log/"):
        name = key.rsplit("/", 1)[-1]
        if name.endswith(".json") and name[:-5].isdigit():
            versions.append(int(name[:-5]))
    return sorted(versions)


def read_commit(s3, bucket, prefix, version):
    body = s3.get_object(
        Bucket=bucket, Key=f"{prefix}/_delta_log/{version:020d}.json"
    )["Body"].read()
    return [json.loads(line) for line in body.decode().splitlines() if line]


def function_env(extra=None):
    env = {
        "AWS_ENDPOINT_URL": LAMBDA_ENDPOINT,
        "AWS_ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": REGION,
        "AWS_S3_LOCKING_PROVIDER": "dynamodb",
        "DYNAMO_LOCK_TABLE_NAME": LOCK_TABLE,
        "RUST_LOG": "info",
    }
    env.update(extra or {})
    return {"Variables": env}


def deploy_function(aws_lambda, name, zip_path, env):
    """Create or update a `provided.al2023` lambda from a cargo-lambda zip."""
    if not zip_path.exists():
        pytest.exit(
            f"Lambda artifact {zip_path} is missing; run acceptance/run.sh "
            "or `cargo lambda build` first.",
            returncode=2,
        )
    code = zip_path.read_bytes()
    try:
        aws_lambda.get_function_configuration(FunctionName=name)
        aws_lambda.update_function_code(FunctionName=name, ZipFile=code)
        _wait_active(aws_lambda, name)
        aws_lambda.update_function_configuration(
            FunctionName=name, Environment=env, Timeout=60, MemorySize=512
        )
    except aws_lambda.exceptions.ResourceNotFoundException:
        aws_lambda.create_function(
            FunctionName=name,
            Runtime="provided.al2023",
            Role=ROLE_ARN,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=60,
            MemorySize=512,
            Architectures=["x86_64"],
            Environment=env,
        )
    _wait_active(aws_lambda, name)
    return name


def _wait_active(aws_lambda, name, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cfg = aws_lambda.get_function_configuration(FunctionName=name)
        if cfg.get("LastUpdateStatus", "Successful") == "Successful" and cfg.get(
            "State", "Active"
        ) == "Active":
            return cfg
        if cfg.get("LastUpdateStatus") == "Failed":
            pytest.fail(f"lambda {name} update failed: {cfg.get('LastUpdateStatusReason')}")
        time.sleep(1)
    pytest.fail(f"lambda {name} did not become active within {timeout}s")


def invoke(aws_lambda, name, payload):
    """Synchronously invoke a lambda and return its decoded response body."""
    body, error = invoke_raw(aws_lambda, name, payload)
    if error:
        pytest.fail(f"lambda {name} returned {error}: {body}")
    return body


def invoke_raw(aws_lambda, name, payload):
    """Invoke a lambda and return (body, function_error); never fails the test."""
    resp = aws_lambda.invoke(
        FunctionName=name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    return resp["Payload"].read().decode(), resp.get("FunctionError")


def sqs_event(records):
    """Wrap message bodies in the SQS event shape the lambdas expect."""
    return {
        "Records": [
            {
                "messageId": str(uuid.uuid4()),
                "receiptHandle": "acceptance",
                "body": body,
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": str(int(time.time() * 1000)),
                    "SenderId": "acceptance",
                    "ApproximateFirstReceiveTimestamp": str(int(time.time() * 1000)),
                },
                "messageAttributes": {},
                "md5OfBody": "",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:000000000000:acceptance",
                "awsRegion": REGION,
            }
            for body in records
        ]
    }


def s3_put_event_body(bucket, key, size):
    """The S3 ObjectCreated:Put notification body that oxbow consumes."""
    return json.dumps(
        {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": REGION,
                    "eventTime": "2026-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "acceptance"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "acceptance",
                        "x-amz-id-2": "acceptance",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "acceptance",
                        "bucket": {
                            "name": bucket,
                            "ownerIdentity": {"principalId": "acceptance"},
                            "arn": f"arn:aws:s3:::{bucket}",
                        },
                        "object": {"key": key, "size": size, "eTag": "acceptance"},
                    },
                }
            ]
        }
    )


@pytest.fixture(scope="session")
def oxbow_function(aws_lambda, lock_tables):
    return deploy_function(
        aws_lambda, "oxbow-acceptance", OXBOW_ZIP, function_env()
    )
