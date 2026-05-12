terraform {
  backend "s3" {
    bucket         = "reddit-kafka-tf-state-dev"
    key            = "state/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "reddit-kafka-tf-locks-dev"
    encrypt        = true
  }
}
