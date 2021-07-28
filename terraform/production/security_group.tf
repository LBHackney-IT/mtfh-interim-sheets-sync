data "aws_vpc" "production_vpc" {
  tags = {
    Name = "vpc-housing-production"
  }
}

resource "aws_security_group" "mtfh_interim_sheets_sync_lambda" {
  name        = "mtfh_interim_sheets_sync_lambda"
  description = "SG used to allow Lambda to access UH database."
  vpc_id      = data.aws_vpc.production_vpc.id

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

resource "aws_ssm_parameter" "mtfh-interim-sheets-sync-lambda" {
  name = "/housing-tl/production/sg-mtfh-interim-sheets-sync-lambda-id"
  type = "String"
  value = aws_security_group.mtfh_interim_sheets_sync_lambda.id
}
