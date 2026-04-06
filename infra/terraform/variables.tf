variable "snowflake_account"  { type = string }
variable "snowflake_username" { type = string }
variable "snowflake_password" {
  type      = string
  sensitive = true
}
