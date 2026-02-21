resource "aws_s3_bucket" "lakehouse" {
  bucket = "fr-transport-lakehouse"

  tags = {
    Name        = "transport-lakehouse"
    Environment = "prod"
    Project     = "realtime-platform"
  }
}
