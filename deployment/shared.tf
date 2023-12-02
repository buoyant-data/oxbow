### Bootstrapping/configuration
###############################
variable "s3_bucket_arn" {
  type        = string
  default     = "*"
  description = "The ARN for the S3 bucket that the oxbow function will watch"
}

variable "aws_access_key" {
  type    = string
  default = ""
}

variable "aws_secret_key" {
  type    = string
  default = ""
}

provider "aws" {
  region     = "us-west-2"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key

  default_tags {
    tags = {
      ManagedBy   = "Terraform"
      environment = terraform.workspace
      workspace   = terraform.workspace
    }
  }
}

data "aws_iam_policy_document" "queue" {
  statement {
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:SendMessage"]
    resources = ["arn:aws:sqs:*:*:*"]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      # Allow all S3 buckets send notifications
      values = ["arn:aws:s3:::*"]
    }
  }
}
