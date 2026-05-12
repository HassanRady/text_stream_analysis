output "alb_dns" {
  value       = aws_lb.main.dns_name
  description = "DNS name of the application load balancer"
}
output "rds_endpoint" {
  value       = aws_rds_cluster.main.reader_endpoint
  description = "RDS cluster reader endpoint"
}
output "redis_endpoint" {
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
  description = "Redis primary endpoint address"
}
output "redis_port" {
  value       = aws_elasticache_replication_group.redis.port
  description = "Redis port"
}
output "msk_bootstrap" {
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
  description = "MSK bootstrap brokers (TLS)"
}
output "ecr_repository_url" {
  value       = aws_ecr_repository.app.repository_url
  description = "ECR repository URL for the app image"
}
output "ecs_cluster_name" {
  value       = aws_ecs_cluster.main.name
  description = "ECS cluster name"
}
output "ecs_service_name" {
  value       = aws_ecs_service.app.name
  description = "ECS service name"
}
output "codeclimate_log_group" {
  value       = aws_cloudwatch_log_group.ecs.name
  description = "CloudWatch log group for ECS tasks"
}
