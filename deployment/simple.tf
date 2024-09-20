# This Terraform file is necessary to configure the basic
# infrastructure around the Oxbow lambda function
resource "aws_s3_bucket" "parquets" {
  bucket = "oxbow-simple"
}

resource "aws_s3_bucket_notification" "bucket-notifications" {
  bucket = aws_s3_bucket.parquets.id

  queue {
    queue_arn     = aws_sqs_queue.oxbow.arn
    events        = ["s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"]
    filter_suffix = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow-bucket]
}

resource "aws_lambda_function" "oxbow" {
  description      = "A simple lambda for converting parquet files to delta tables"
  filename         = "../target/lambda/oxbow-lambda/bootstrap.zip"
  source_code_hash = filesha256("../target/lambda/oxbow-lambda/bootstrap.zip")

  function_name = "oxbow-delta-lake-conversion"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "provided"
  runtime       = "provided.al2023"

  environment {
    variables = {
      AWS_S3_LOCKING_PROVIDER = "dynamodb"
      RUST_LOG                = "deltalake=debug,oxbow=debug"
      DYNAMO_LOCK_TABLE_NAME  = aws_dynamodb_table.oxbow_locking.name
      DELTA_DYNAMO_TABLE_NAME = aws_dynamodb_table.oxbow_logstore.name
    }
  }
}

resource "aws_lambda_event_source_mapping" "oxbow-trigger" {
  event_source_arn = aws_sqs_queue.oxbow.arn
  function_name    = aws_lambda_function.oxbow.arn
}

resource "aws_sqs_queue" "oxbow" {
  name   = "oxbow-notification-queue"
  policy = data.aws_iam_policy_document.queue.json

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.oxbow_dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue" "oxbow_dlq" {
  name = "obxow-notification-dlq"
}

resource "aws_lambda_permission" "allow-bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.oxbow.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.parquets.arn
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_policy" "lambda_permissions" {
  name = "oxbow-permissions"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["dynamodb:*"]
        Resource = ["*"]
        Effect   = "Allow"
      },
      {
        Action   = ["s3:*"]
        Resource = var.s3_bucket_arn
        Effect   = "Allow"
      },
      {
        Action   = ["sqs:*"]
        Resource = "*"
        Effect   = "Allow"
      }
    ]
  })
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "iam_for_oxbow_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  managed_policy_arns = [
    aws_iam_policy.lambda_permissions.arn,
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
  ]
}

# This DynamoDb table is used for providing safe table creation for oxbow
resource "aws_dynamodb_table" "oxbow_locking" {
  name         = "oxbow_lock_table"
  billing_mode = "PAY_PER_REQUEST"
  # Default name of the partition key hard-coded in delta-rs
  hash_key = "key"

  attribute {
    name = "key"
    type = "S"
  }

  ttl {
    attribute_name = "leaseDuration"
    enabled        = true
  }
}

resource "aws_dynamodb_table" "oxbow_logstore" {
  name         = "oxbow_logstore_table"
  billing_mode = "PAY_PER_REQUEST"
  # Default name of the partition key hard-coded in delta-rs
  hash_key  = "tablePath"
  range_key = "fileName"

  attribute {
    name = "tablePath"
    type = "S"
  }

  attribute {
    name = "fileName"
    type = "S"
  }
}

