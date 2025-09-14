# Configure the Azure Provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~>0.9"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

# Variables
variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-blob-storage-demo"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "storage_account_name" {
  description = "Name of the storage account"
  type        = string
  default     = "stblobdemo001"
  
  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "Storage account name must be 3-24 characters long and contain only lowercase letters and numbers."
  }
}

variable "container_name" {
  description = "Name of the blob container"
  type        = string
  default     = "demo-container"
}

variable "sas_expiry_days" {
  description = "Number of days until SAS tokens expire"
  type        = number
  default     = 30
}

# Create Resource Group
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    Environment = "Development"
    Purpose     = "Blob Storage Demo"
    CreatedBy   = "Terraform"
  }
}

# Create Storage Account
resource "azurerm_storage_account" "main" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  
  # Enable blob versioning and change feed for better management
  blob_properties {
    versioning_enabled  = true
    change_feed_enabled = true
  }

  tags = {
    Environment = "Development"
    Purpose     = "Blob Storage Demo"
    CreatedBy   = "Terraform"
  }
}

# Create Blob Container
resource "azurerm_storage_container" "main" {
  name                  = var.container_name
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Generate expiry time for SAS tokens
resource "time_offset" "sas_expiry" {
  offset_days = var.sas_expiry_days
}

# SAS Token for Write Operations
data "azurerm_storage_account_blob_container_sas" "write_sas" {
  connection_string = azurerm_storage_account.main.primary_connection_string
  container_name    = azurerm_storage_container.main.name
  https_only        = true

  start  = timestamp()
  expiry = time_offset.sas_expiry.rfc3339

  permissions {
    read   = false
    add    = true
    create = true
    write  = true
    delete = false
    list   = false
  }
}

# SAS Token for Read and Delete Operations
data "azurerm_storage_account_blob_container_sas" "read_delete_sas" {
  connection_string = azurerm_storage_account.main.primary_connection_string
  container_name    = azurerm_storage_container.main.name
  https_only        = true

  start  = timestamp()
  expiry = time_offset.sas_expiry.rfc3339

  permissions {
    read   = true
    add    = false
    create = false
    write  = false
    delete = true
    list   = true
  }
}

# Outputs
output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Name of the created storage account"
  value       = azurerm_storage_account.main.name
}

output "storage_account_primary_key" {
  description = "Primary access key for the storage account"
  value       = azurerm_storage_account.main.primary_access_key
  sensitive   = true
}

output "container_name" {
  description = "Name of the created container"
  value       = azurerm_storage_container.main.name
}

output "write_sas_token" {
  description = "SAS token with write permissions"
  value       = data.azurerm_storage_account_blob_container_sas.write_sas.sas
  sensitive   = true
}

output "read_delete_sas_token" {
  description = "SAS token with read and delete permissions"
  value       = data.azurerm_storage_account_blob_container_sas.read_delete_sas.sas
  sensitive   = true
}

output "storage_account_blob_endpoint" {
  description = "Primary blob endpoint for the storage account"
  value       = azurerm_storage_account.main.primary_blob_endpoint
}

output "container_url" {
  description = "Full URL of the container"
  value       = "${azurerm_storage_account.main.primary_blob_endpoint}${azurerm_storage_container.main.name}"
}
