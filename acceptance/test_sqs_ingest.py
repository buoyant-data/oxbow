"""
End-to-end tests for the sqs-ingest lambda.

sqs-ingest appends the JSON payload of each SQS message to the Delta table
named by its DELTA_TABLE_URI environment variable, so the function is deployed
against a table created for this module.
"""

import json
import uuid

import pytest

from conftest import (
    SQS_INGEST_ZIP,
    deploy_function,
    function_env,
    invoke,
    log_versions,
    read_commit,
    sqs_event,
    upload_fixture_table,
)


@pytest.fixture(scope="module")
def ingest_table(s3):
    bucket = f"oxbow-acc-sqs-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=bucket)
    uri = upload_fixture_table(s3, bucket, "senators")
    yield bucket, uri
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture(scope="module")
def buffer_queue_url(sqs):
    """sqs-ingest reads BUFFER_MORE_QUEUE_URL at startup and panics without it."""
    return sqs.create_queue(QueueName=f"oxbow-acc-buffer-{uuid.uuid4().hex[:8]}")["QueueUrl"]


@pytest.fixture(scope="module")
def ingest_function(aws_lambda, lock_tables, ingest_table, buffer_queue_url):
    _, uri = ingest_table
    return deploy_function(
        aws_lambda,
        "sqs-ingest-table-acceptance",
        SQS_INGEST_ZIP,
        function_env(
            {
                "DELTA_TABLE_URI": uri,
                "BUFFER_MORE_QUEUE_URL": buffer_queue_url,
            }
        ),
    )


def test_messages_are_appended_as_a_new_commit(
    s3, aws_lambda, ingest_table, ingest_function
):
    bucket, _ = ingest_table
    prefix = "senators"
    before = log_versions(s3, bucket, prefix)
    assert before, "fixture table should have an initial commit"

    rows = [{"c1": 101, "c2": "ingested-a"}, {"c1": 102, "c2": "ingested-b"}]
    payload = sqs_event([json.dumps(row) for row in rows])
    invoke(aws_lambda, ingest_function, payload)

    after = log_versions(s3, bucket, prefix)
    assert after[-1] == before[-1] + 1, f"expected one new commit, got {before} -> {after}"

    commit = read_commit(s3, bucket, prefix, after[-1])
    added = [a["add"] for a in commit if "add" in a]
    assert added, "the new commit should add at least one data file"
    assert any(
        path.endswith(".parquet") for path in (a["path"] for a in added)
    ), f"unexpected add actions: {added}"
