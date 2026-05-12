data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name = "${var.project_name}-${var.environment}"

  azs = length(var.availability_zones) > 0 ? var.availability_zones : slice(
    data.aws_availability_zones.available.names,
    0,
    max(length(var.public_subnet_cidrs), length(var.private_subnet_cidrs))
  )

  rds_master_password = trimspace(var.rds_master_password) != "" ? var.rds_master_password : random_password.rds_master.result
  redis_auth_token    = trimspace(var.redis_auth_token) != "" ? var.redis_auth_token : random_password.redis_auth.result
  kafka_sasl_password = trimspace(var.kafka_sasl_password) != "" ? var.kafka_sasl_password : random_password.kafka_sasl.result
  enable_https        = var.acm_certificate_arn != ""
}
