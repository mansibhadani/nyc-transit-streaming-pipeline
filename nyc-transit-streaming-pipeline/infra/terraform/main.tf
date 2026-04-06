# ─────────────────────────────────────────────────────────────────────────────
#  Terraform — Confluent Cloud resources
#  Provisions: Kafka cluster, topics, Schema Registry, API keys
#
#  Usage:
#    export CONFLUENT_CLOUD_API_KEY=...
#    export CONFLUENT_CLOUD_API_SECRET=...
#    terraform init && terraform apply
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.61"
    }
  }
}

provider "confluent" {}    # reads CONFLUENT_CLOUD_API_KEY/SECRET from env

# ── Environment ───────────────────────────────────────────────────────────────
resource "confluent_environment" "transit" {
  display_name = "nyc-transit-pipeline"
}

# ── Kafka cluster (Basic — free tier eligible) ────────────────────────────────
resource "confluent_kafka_cluster" "transit" {
  display_name = "transit-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = "us-east-1"
  basic {}
  environment { id = confluent_environment.transit.id }
}

# ── Service account ───────────────────────────────────────────────────────────
resource "confluent_service_account" "pipeline" {
  display_name = "transit-pipeline-sa"
  description  = "Service account for the NYC transit streaming pipeline"
}

resource "confluent_role_binding" "pipeline_cluster_admin" {
  principal   = "User:${confluent_service_account.pipeline.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.transit.rbac_crn
}

resource "confluent_api_key" "kafka" {
  display_name = "transit-kafka-key"
  owner {
    id          = confluent_service_account.pipeline.id
    api_version = confluent_service_account.pipeline.api_version
    kind        = confluent_service_account.pipeline.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.transit.id
    api_version = confluent_kafka_cluster.transit.api_version
    kind        = confluent_kafka_cluster.transit.kind
    environment { id = confluent_environment.transit.id }
  }
}

# ── Topics ────────────────────────────────────────────────────────────────────
locals {
  topics = {
    "transit.raw"         = { partitions = 3, retention_ms = "604800000" }  # 7 days
    "transit.delays"      = { partitions = 3, retention_ms = "604800000" }
    "transit.alerts"      = { partitions = 3, retention_ms = "259200000" }  # 3 days
    "transit.dead-letter" = { partitions = 1, retention_ms = "2592000000" } # 30 days
  }
}

resource "confluent_kafka_topic" "topics" {
  for_each         = local.topics
  topic_name       = each.key
  partitions_count = each.value.partitions
  rest_endpoint    = confluent_kafka_cluster.transit.rest_endpoint

  config = {
    "retention.ms"        = each.value.retention_ms
    "cleanup.policy"      = "delete"
    "compression.type"    = "snappy"
    "min.insync.replicas" = "1"
  }

  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }

  kafka_cluster { id = confluent_kafka_cluster.transit.id }
  environment   { id = confluent_environment.transit.id }
}

# ── Outputs (copy these into .env) ────────────────────────────────────────────
output "kafka_bootstrap_servers" {
  value = confluent_kafka_cluster.transit.bootstrap_endpoint
}

output "kafka_api_key" {
  value     = confluent_api_key.kafka.id
  sensitive = true
}

output "kafka_api_secret" {
  value     = confluent_api_key.kafka.secret
  sensitive = true
}
