# S3 backend for Terraform state
# Replace BUCKET_NAME and DYNAMODB_TABLE_NAME with your actual values
# 
# To use this backend:
# 1. Create S3 bucket and DynamoDB table manually or via Terraform
# 2. Run: terraform init -backend-config="bucket=YOUR_BUCKET" etc.
# 
# For local testing without S3 backend, comment this out and use local state
terraform {
  backend "s3" {
    bucket         = "reddit-kafka-tf-state-dev"
    key            = "state/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "reddit-kafka-tf-locks-dev"
    encrypt        = true
  }
}
