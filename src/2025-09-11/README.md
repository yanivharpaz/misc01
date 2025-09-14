# Azure Blob Storage with Terraform - Setup Guide

## Overview

This solution creates:
- **Resource Group**: Container for all resources
- **Storage Account**: Azure storage account with blob storage
- **Blob Container**: Private container for storing blobs
- **Write SAS Token**: Allows create/write operations only
- **Read/Delete SAS Token**: Allows read/list/delete operations only

## Prerequisites

1. **Azure CLI** installed and logged in:
   ```bash
   az login
   az account set --subscription "your-subscription-id"
   ```

2. **Terraform** installed (version 1.0+):
   ```bash
   terraform --version
   ```

3. **Python 3.8+** with pip for running tests:
   ```bash
   python3 --version
   pip install azure-storage-blob azure-identity
   ```

## Quick Start

### 1. Initialize and Deploy Infrastructure

```bash
# Clone or create directory with the Terraform files
mkdir azure-blob-terraform && cd azure-blob-terraform

# Save the Terraform configuration as main.tf
# (Copy the Terraform code from the first artifact)

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply the configuration
terraform apply
```

### 2. Customize Variables (Optional)

Create a `terraform.tfvars` file to customize settings:

```hcl
resource_group_name   = "rg-my-blob-storage"
location             = "West US 2"
storage_account_name = "stmyblobdemo001"  # Must be globally unique
container_name       = "my-container"
sas_expiry_days      = 60
```

### 3. Run Tests

```bash
# Save the Python test script as test_terraform.py
# Install required packages
pip install azure-storage-blob azure-identity

# Run the comprehensive test suite
python test_terraform.py
```

## Architecture Details

### SAS Token Permissions

**Write SAS Token:**
- ✅ Add (create new blobs)
- ✅ Create (create new blobs)
- ✅ Write (modify blob content)
- ❌ Read (cannot read blob content)
- ❌ Delete (cannot delete blobs)
- ❌ List (cannot list blobs)

**Read/Delete SAS Token:**
- ✅ Read (read blob content)
- ✅ List (list blobs in container)
- ✅ Delete (delete blobs)
- ❌ Add (cannot create new blobs)
- ❌ Create (cannot create new blobs)
- ❌ Write (cannot modify existing blobs)

### Security Features

1. **HTTPS Only**: All SAS tokens enforce HTTPS-only access
2. **Time-Limited**: Tokens expire after specified days (default: 30 days)
3. **Least Privilege**: Each token has minimal required permissions
4. **Private Container**: Container access is private by default

## Test Suite Features

The Python test script provides comprehensive validation:

### 1. Infrastructure Tests
- ✅ Validates all Terraform outputs exist
- ✅ Checks Azure naming conventions
- ✅ Verifies resource accessibility
- ✅ Tests Terraform state consistency

### 2. Security Tests
- ✅ Enforces HTTPS-only access
- ✅ Validates SAS token security parameters
- ✅ Checks token expiry settings

### 3. Functional Tests
- ✅ Tests write SAS token permissions
- ✅ Tests read/delete SAS token permissions
- ✅ Validates permission boundaries (operations that should fail)

### 4. Integration Tests
- ✅ Real Azure API calls to verify functionality
- ✅ Blob upload/download/delete operations
- ✅ Permission enforcement validation

## Usage Examples

### Using the Write SAS Token (Python)

```python
from azure.storage.blob import BlobClient

# Get values from Terraform outputs
container_url = "https://stblobdemo001.blob.core.windows.net/demo-container"
write_sas = "?sv=2021-06-08&se=2024-..."

# Upload a blob
blob_url = f"{container_url}/myfile.txt{write_sas}"
blob_client = BlobClient.from_blob_url(blob_url)
blob_client.upload_blob(b"Hello World!", overwrite=True)
```

### Using the Read/Delete SAS Token (Python)

```python
from azure.storage.blob import BlobClient

# Get values from Terraform outputs
container_url = "https://stblobdemo001.blob.core.windows.net/demo-container"
read_delete_sas = "?sv=2021-06-08&se=2024-..."

# Read a blob
blob_url = f"{container_url}/myfile.txt{read_delete_sas}"
blob_client = BlobClient.from_blob_url(blob_url)
content = blob_client.download_blob().readall()
print(content.decode())

# Delete a blob
blob_client.delete_blob()
```

## Terraform Commands Reference

```bash
# Initialize
terraform init

# Plan changes
terraform plan

# Apply changes
terraform apply

# Show outputs
terraform output

# Show sensitive outputs
terraform output -json

# Destroy infrastructure
terraform destroy

# Format code
terraform fmt

# Validate configuration
terraform validate
```

## Monitoring and Troubleshooting

### View Storage Account in Azure Portal

1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to Resource Groups → `rg-blob-storage-demo`
3. Click on the storage account
4. Check containers under "Data storage" → "Containers"

### Common Issues

1. **Storage Account Name Conflict**
   ```
   Error: Storage account name must be globally unique
   ```
   Solution: Change `storage_account_name` in variables

2. **SAS Token Access Denied**
   ```
   Error: AuthenticationFailed
   ```
   Solution: Check token permissions and expiry

3. **Terraform State Lock**
   ```
   Error: Resource temporarily unavailable
   ```
   Solution: Wait or force unlock with `terraform force-unlock`

### Debugging Tests

Run tests with more verbose output:
```bash
python -m unittest test_terraform.TestSASTokenFunctionality -v
```

## Security Best Practices

1. **Rotate SAS Tokens Regularly**: Implement token rotation
2. **Monitor Access**: Enable storage analytics and monitoring
3. **Network Security**: Consider firewall rules and private endpoints
4. **Access Logging**: Enable diagnostic settings
5. **Backup**: Implement backup strategies for critical data

## Cleanup

To remove all resources:

```bash
terraform destroy
```

This will remove:
- All blobs in the container
- The blob container
- The storage account
- The resource group

**Warning**: This action is irreversible and will delete all data!

## Next Steps

1. **Add Monitoring**: Implement Azure Monitor alerts
2. **Add Backup**: Configure blob backup policies
3. **Add Network Security**: Implement private endpoints
4. **Add RBAC**: Use Azure AD authentication instead of SAS tokens
5. **Add Lifecycle Management**: Implement automated data lifecycle policies

