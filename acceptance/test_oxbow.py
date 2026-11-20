"""
End-to-end tests for the oxbow lambda.

The oxbow lambda receives S3 ObjectCreated notifications (wrapped in SQS
messages) and commits the new objects into the Delta table's _delta_log.
"""

from conftest import (
    invoke,
    invoke_raw,
    list_keys,
    log_versions,
    read_commit,
    s3_put_event_body,
    sqs_event,
    upload_fixture_table,
)


def test_append_new_parquet_file_commits_a_version(
    s3, bucket, oxbow_function, aws_lambda
):
    table = upload_fixture_table(s3, bucket, "senators")
    prefix = "senators"
    before = log_versions(s3, bucket, prefix)
    assert before, "fixture table should have an initial commit"

    # Stage a new data file in the table root, then notify oxbow about it.
    source = next(
        k
        for k in list_keys(s3, bucket, prefix)
        if k.endswith(".parquet")
    )
    new_key = f"{prefix}/appended-by-oxbow.snappy.parquet"
    s3.copy_object(
        Bucket=bucket,
        Key=new_key,
        CopySource={"Bucket": bucket, "Key": source},
    )

    payload = sqs_event(
        [s3_put_event_body(bucket, new_key, s3.head_object(Bucket=bucket, Key=new_key)["ContentLength"])]
    )
    invoke(aws_lambda, oxbow_function, payload)

    after = log_versions(s3, bucket, prefix)
    assert after, "table should still have commits"
    assert after[-1] == before[-1] + 1, f"expected one new commit, got {before} -> {after}"

    commit = read_commit(s3, bucket, prefix, after[-1])
    adds = [a["add"]["path"] for a in commit if "add" in a]
    assert any(path.endswith("appended-by-oxbow.snappy.parquet") for path in adds), (
        f"new commit did not add the appended file: {adds}"
    )


def test_event_for_non_table_prefix_errors(s3, bucket, oxbow_function, aws_lambda):
    """An object outside any Delta table must make oxbow fail, not commit anything."""
    key = "not-a-table/data.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=b"not really parquet")

    payload = sqs_event(
        [s3_put_event_body(bucket, key, len(b"not really parquet"))]
    )
    body, error = invoke_raw(aws_lambda, oxbow_function, payload)

    assert error == "Unhandled", f"expected oxbow to error, got {error!r}: {body}"
    assert list_keys(s3, bucket, "not-a-table/_delta_log/") == []
