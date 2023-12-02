# This terraform file implements a more advanced pattern for Oxbow which
# involves multiple queues to reduce lock contention with high traffic S3
# buckets
#
# Unlike the Simple example, the flow for events is slightly different for the
# advanced use-case:
#
# S3 Event Notifications -> SQS -> group-events -> SQS FIFO -> oxbow

resource "aws_s3_bucket" "parquets-advanced" {
  bucket = "oxbow-advanced"
}

resource "aws_s3_bucket_notification" "advanced-bucket-notifications" {
  bucket = aws_s3_bucket.parquets-advanced.id

  queue {
    queue_arn     = aws_sqs_queue.group-events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".parquet"
  }

  depends_on = [aws_lambda_permission.advanced-allow-bucket]
}


resource "aws_lambda_function" "oxbow-advanced" {
  description   = "An advanced lambda for converting parquet files to delta tables"
  filename      = "../target/lambda/oxbow-lambda/bootstrap.zip"
  function_name = "oxbow-advanced"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "provided"
  runtime       = "provided.al2"

  environment {
    variables = {
      AWS_S3_LOCKING_PROVIDER = "dynamodb"
      RUST_LOG                = "deltalake=debug,oxbow=debug"
      DYNAMO_LOCK_TABLE_NAME  = aws_dynamodb_table.oxbow_advanced_locking.name
    }
  }
}

resource "aws_lambda_function" "group-events" {
  description   = "Group events for oxbow based on the table prefix"
  filename      = "../target/lambda/group-events/bootstrap.zip"
  function_name = "group-events"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "provided"
  runtime       = "provided.al2"

  environment {
    variables = {
      RUST_LOG  = "group-events=debug"
      QUEUE_URL = aws_sqs_queue.oxbow-advanced-fifo.url
    }
  }
}

resource "aws_lambda_event_source_mapping" "group-events-trigger" {
  event_source_arn = aws_sqs_queue.group-events.arn
  function_name    = aws_lambda_function.group-events.arn
}
resource "aws_lambda_event_source_mapping" "oxbow-advanced-trigger" {
  event_source_arn = aws_sqs_queue.oxbow-advanced-fifo.arn
  function_name    = aws_lambda_function.oxbow-advanced.arn
}


resource "aws_sqs_queue" "oxbow-advanced-fifo" {
  name   = "oxbow-advanced.fifo"
  policy = data.aws_iam_policy_document.queue.json

  content_based_deduplication = true
  fifo_queue                  = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.oxbow-advanced-dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue" "oxbow-advanced-dlq" {
  name       = "obxow-advanced-dlq.fifo"
  fifo_queue = true
}

resource "aws_sqs_queue" "group-events" {
  name   = "group-events"
  policy = data.aws_iam_policy_document.queue.json

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.group-events-dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue" "group-events-dlq" {
  name = "group-events-dlq"
}

resource "aws_lambda_permission" "advanced-allow-bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.group-events.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.parquets.arn
}

# The DynamoDb table is used for providing safe concurrent writes to delta
# tables.
resource "aws_dynamodb_table" "oxbow_advanced_locking" {
  name         = "oxbow_advanced_lock_table"
  billing_mode = "PROVISIONED"
  # Default name of the partition key hard-coded in delta-rs
  hash_key       = "key"
  read_capacity  = 10
  write_capacity = 10

  attribute {
    name = "key"
    type = "S"
  }

  ttl {
    attribute_name = "leaseDuration"
    enabled        = true
  }
}
