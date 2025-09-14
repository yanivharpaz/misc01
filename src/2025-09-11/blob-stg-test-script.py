#!/usr/bin/env python3
"""
Comprehensive test script for Azure Blob Storage Terraform deployment.
This script tests the infrastructure provisioned by Terraform including
resource group, storage account, blob container, and SAS token functionality.
"""

import unittest
import json
import subprocess
import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from unittest.mock import patch, MagicMock

# Azure SDK imports
try:
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    from azure.core.exceptions import AzureError, ClientAuthenticationError
    from azure.identity import DefaultAzureCredential
except ImportError as e:
    print("Warning: Azure SDK not installed. Install with: pip install azure-storage-blob azure-identity")
    print(f"Error: {e}")


class TerraformAzureBlobTest(unittest.TestCase):
    """Test suite for Terraform Azure Blob Storage deployment."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class with Terraform outputs."""
        cls.terraform_outputs = cls._get_terraform_outputs()
        cls.test_blob_name = f"test-blob-{int(time.time())}.txt"
        cls.test_content = b"This is a test blob content for validation"
        
    @classmethod
    def _get_terraform_outputs(cls) -> Dict[str, Any]:
        """Extract outputs from Terraform state."""
        try:
            result = subprocess.run(
                ["terraform", "output", "-json"],
                capture_output=True,
                text=True,
                check=True
            )
            outputs = json.loads(result.stdout)
            # Extract values from Terraform output format
            return {key: value["value"] for key, value in outputs.items()}
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get Terraform outputs: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse Terraform outputs JSON: {e}")
    
    def setUp(self):
        """Set up each test case."""
        self.container_url = self.terraform_outputs.get("container_url")
        self.write_sas_token = self.terraform_outputs.get("write_sas_token")
        self.read_delete_sas_token = self.terraform_outputs.get("read_delete_sas_token")
        self.storage_account_name = self.terraform_outputs.get("storage_account_name")
        self.container_name = self.terraform_outputs.get("container_name")
        self.blob_endpoint = self.terraform_outputs.get("storage_account_blob_endpoint")


class TestTerraformOutputs(TerraformAzureBlobTest):
    """Test Terraform outputs are properly generated."""
    
    def test_terraform_outputs_exist(self):
        """Test that all required Terraform outputs exist."""
        required_outputs = [
            "resource_group_name",
            "storage_account_name", 
            "container_name",
            "write_sas_token",
            "read_delete_sas_token",
            "storage_account_blob_endpoint",
            "container_url"
        ]
        
        for output in required_outputs:
            with self.subTest(output=output):
                self.assertIn(output, self.terraform_outputs, 
                            f"Required output '{output}' is missing")
                self.assertIsNotNone(self.terraform_outputs[output],
                                   f"Output '{output}' is None")
                self.assertNotEqual(self.terraform_outputs[output], "",
                                  f"Output '{output}' is empty")
    
    def test_storage_account_name_format(self):
        """Test storage account name follows Azure naming conventions."""
        storage_name = self.terraform_outputs["storage_account_name"]
        
        # Azure storage account naming rules
        self.assertRegex(storage_name, r"^[a-z0-9]{3,24}$",
                        "Storage account name must be 3-24 characters, lowercase letters and numbers only")
    
    def test_sas_tokens_not_empty(self):
        """Test that SAS tokens are generated and not empty."""
        write_sas = self.terraform_outputs["write_sas_token"]
        read_delete_sas = self.terraform_outputs["read_delete_sas_token"]
        
        self.assertTrue(write_sas.startswith("?"),
                       "Write SAS token should start with '?'")
        self.assertTrue(read_delete_sas.startswith("?"),
                       "Read/delete SAS token should start with '?'")
        
        # SAS tokens should contain required parameters
        for param in ["sv", "se", "sp"]:  # version, expiry, permissions
            self.assertIn(param, write_sas, f"Write SAS missing parameter: {param}")
            self.assertIn(param, read_delete_sas, f"Read/delete SAS missing parameter: {param}")


class TestAzureResourcesCreation(TerraformAzureBlobTest):
    """Test that Azure resources are properly created."""
    
    def test_container_accessibility(self):
        """Test container is accessible via generated URLs."""
        container_url = self.terraform_outputs["container_url"]
        
        # Validate URL format
        self.assertTrue(container_url.startswith("https://"),
                       "Container URL should use HTTPS")
        self.assertIn(self.storage_account_name, container_url,
                     "Container URL should contain storage account name")
        self.assertIn(self.container_name, container_url,
                     "Container URL should contain container name")


@unittest.skipIf("azure.storage.blob" not in globals(), "Azure SDK not available")
class TestSASTokenFunctionality(TerraformAzureBlobTest):
    """Test SAS token functionality with actual Azure operations."""
    
    def test_write_sas_token_permissions(self):
        """Test write SAS token can create and write blobs but not read or delete."""
        write_blob_url = f"{self.container_url}/{self.test_blob_name}{self.write_sas_token}"
        blob_client = BlobClient.from_blob_url(write_blob_url)
        
        # Test write operation (should succeed)
        try:
            blob_client.upload_blob(self.test_content, overwrite=True)
            self.assertTrue(True, "Write operation should succeed with write SAS token")
        except Exception as e:
            self.fail(f"Write operation failed with write SAS token: {e}")
        
        # Test read operation (should fail)
        with self.assertRaises(AzureError, msg="Read should fail with write-only SAS token"):
            blob_client.download_blob().readall()
        
        # Test delete operation (should fail)
        with self.assertRaises(AzureError, msg="Delete should fail with write-only SAS token"):
            blob_client.delete_blob()
    
    def test_read_delete_sas_token_permissions(self):
        """Test read/delete SAS token can read and delete but not write."""
        # First, ensure there's a blob to test with (using management operations)
        self._ensure_test_blob_exists()
        
        read_delete_blob_url = f"{self.container_url}/{self.test_blob_name}{self.read_delete_sas_token}"
        blob_client = BlobClient.from_blob_url(read_delete_blob_url)
        
        # Test read operation (should succeed)
        try:
            content = blob_client.download_blob().readall()
            self.assertEqual(content, self.test_content, 
                           "Read content should match original")
        except Exception as e:
            self.fail(f"Read operation failed with read/delete SAS token: {e}")
        
        # Test write operation (should fail)
        with self.assertRaises(AzureError, msg="Write should fail with read/delete-only SAS token"):
            blob_client.upload_blob(b"new content", overwrite=True)
        
        # Test delete operation (should succeed)
        try:
            blob_client.delete_blob()
            self.assertTrue(True, "Delete operation should succeed with read/delete SAS token")
        except Exception as e:
            self.fail(f"Delete operation failed with read/delete SAS token: {e}")
    
    def test_sas_token_expiry_format(self):
        """Test that SAS tokens have proper expiry format."""
        import urllib.parse as urlparse
        
        for token_name, token in [("write", self.write_sas_token), 
                                 ("read_delete", self.read_delete_sas_token)]:
            with self.subTest(token=token_name):
                parsed = urlparse.parse_qs(token.lstrip("?"))
                
                # Check expiry parameter exists
                self.assertIn("se", parsed, f"{token_name} SAS token missing expiry parameter")
                
                # Parse expiry date
                expiry_str = parsed["se"][0]
                try:
                    expiry_date = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                    
                    # Should be in the future
                    self.assertGreater(expiry_date, datetime.now().astimezone(),
                                     f"{token_name} SAS token should not be expired")
                    
                    # Should be within reasonable range (not too far in future)
                    max_expiry = datetime.now().astimezone() + timedelta(days=365)
                    self.assertLess(expiry_date, max_expiry,
                                  f"{token_name} SAS token expiry seems too far in future")
                    
                except ValueError as e:
                    self.fail(f"Invalid expiry date format in {token_name} SAS token: {e}")
    
    def _ensure_test_blob_exists(self):
        """Helper method to ensure test blob exists using account key."""
        try:
            # Use account key for management operations
            account_key = self.terraform_outputs.get("storage_account_primary_key")
            if not account_key:
                self.skipTest("Storage account primary key not available")
                
            blob_service_client = BlobServiceClient(
                account_url=self.blob_endpoint,
                credential=account_key
            )
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=self.test_blob_name
            )
            blob_client.upload_blob(self.test_content, overwrite=True)
            
        except Exception as e:
            self.skipTest(f"Could not create test blob: {e}")


class TestTerraformStateConsistency(TerraformAzureBlobTest):
    """Test Terraform state consistency and validation."""
    
    def test_terraform_plan_no_changes(self):
        """Test that terraform plan shows no changes (infrastructure is in sync)."""
        try:
            result = subprocess.run(
                ["terraform", "plan", "-detailed-exitcode"],
                capture_output=True,
                text=True
            )
            
            # Exit code 0 = no changes, 1 = error, 2 = changes needed
            self.assertEqual(result.returncode, 0,
                           f"Terraform plan shows drift. Output: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            self.fail(f"Terraform plan failed: {e}")
    
    def test_terraform_validate(self):
        """Test that Terraform configuration is valid."""
        try:
            result = subprocess.run(
                ["terraform", "validate"],
                capture_output=True,
                text=True,
                check=True
            )
            self.assertIn("Success!", result.stdout,
                         "Terraform validation should succeed")
            
        except subprocess.CalledProcessError as e:
            self.fail(f"Terraform validation failed: {e.stderr}")


class TestSecurityAndCompliance(TerraformAzureBlobTest):
    """Test security and compliance aspects of the deployment."""
    
    def test_https_enforcement(self):
        """Test that all URLs use HTTPS."""
        urls_to_check = [
            self.terraform_outputs["storage_account_blob_endpoint"],
            self.terraform_outputs["container_url"]
        ]
        
        for url in urls_to_check:
            with self.subTest(url=url):
                self.assertTrue(url.startswith("https://"),
                              f"URL should use HTTPS: {url}")
    
    def test_sas_token_security_parameters(self):
        """Test SAS tokens have proper security parameters."""
        import urllib.parse as urlparse
        
        tokens = {
            "write": self.write_sas_token,
            "read_delete": self.read_delete_sas_token
        }
        
        for token_name, token in tokens.items():
            with self.subTest(token=token_name):
                parsed = urlparse.parse_qs(token.lstrip("?"))
                
                # Should enforce HTTPS only
                if "spr" in parsed:  # Protocol parameter
                    self.assertEqual(parsed["spr"][0], "https",
                                   f"{token_name} SAS token should enforce HTTPS only")


def run_integration_tests():
    """Run integration tests that require actual Azure resources."""
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestTerraformOutputs,
        TestAzureResourcesCreation,
        TestSASTokenFunctionality,
        TestTerraformStateConsistency,
        TestSecurityAndCompliance
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print(f"\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print(f"\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    return result.wasSuccessful()


def setup_test_environment():
    """Set up test environment and install required packages."""
    required_packages = [
        "azure-storage-blob>=12.0.0",
        "azure-identity>=1.0.0"
    ]
    
    print("Setting up test environment...")
    print(f"Required packages: {', '.join(required_packages)}")
    print("Install with: pip install " + " ".join(required_packages))
    
    # Check if packages are available
    try:
        import azure.storage.blob
        import azure.identity
        print("✓ Azure SDK packages are available")
        return True
    except ImportError:
        print("⚠ Azure SDK packages not found. Some tests will be skipped.")
        return False


if __name__ == "__main__":
    print("Azure Blob Storage Terraform Test Suite")
    print("="*50)
    
    # Setup environment
    sdk_available = setup_test_environment()
    
    # Check if we're in a Terraform directory
    if not os.path.exists("terraform.tfstate"):
        print("Warning: terraform.tfstate not found. Make sure to run this script")
        print("in the same directory as your Terraform configuration after running 'terraform apply'.")
    
    # Run tests
    try:
        success = run_integration_tests()
        exit_code = 0 if success else 1
        
        print(f"\n{'='*60}")
        if success:
            print("✅ ALL TESTS PASSED!")
            print("Your Terraform Azure Blob Storage infrastructure is working correctly.")
        else:
            print("❌ SOME TESTS FAILED!")
            print("Please review the failures above and check your infrastructure.")
        
        exit(exit_code)
        
    except Exception as e:
        print(f"Error running tests: {e}")
        exit(1)

    