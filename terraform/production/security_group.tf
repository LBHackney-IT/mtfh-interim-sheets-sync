resource "aws_security_group" "allow_tls" {
  name        = "mtfh_interim_sheets_sync_lambda"
  description = "SG used to allow Lambda to access UH database."
  vpc_id      = "vpc-0ce853ddb64e8fb3c"

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name              = "mtfh-interim-sheets-sync-lambda-${var.environment_name}"
    Environment       = var.environment_name
    terraform-managed = true
    project_name      = var.project_name
    backup_policy     = "Prod"
  }
}