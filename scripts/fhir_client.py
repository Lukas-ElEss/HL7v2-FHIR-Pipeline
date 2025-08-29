#!/usr/bin/env python3
"""
FHIR Client for LinuxForHealth FHIR Server

This module provides a robust FHIR client implementation using the
LinuxForHealth FHIR Client API for communication with FHIR servers.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from config import get_config


@dataclass
class FHIRResponse:
    """Container for FHIR API responses"""
    success: bool
    status_code: int
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    headers: Optional[Dict[str, str]] = None


class FHIRClient:
    """FHIR Client using LinuxForHealth FHIR Client API"""
    
    def __init__(self, config=None):
        """
        Initialize FHIR Client
        
        Args:
            config: FHIR configuration object. If None, loads default config.
        """
        if config is None:
            config = get_config()
        
        self.config = config
        self.base_url = config.get_linuxforhealth_url()
        self.credentials = config.get_linuxforhealth_credentials()
        self.timeout = config.get_linuxforhealth_timeout()
        self.headers = config.get_linuxforhealth_headers()
        self.ssl_verify = getattr(config, 'get_linuxforhealth_ssl_verify', lambda: True)()
        
        # Setup logging
        logging.basicConfig(level=getattr(logging, config.get_log_level()))
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"FHIR Client initialized for: {self.base_url}")
        self.logger.info(f"SSL verification: {self.ssl_verify}")
    
    def _get_auth_header(self) -> Dict[str, str]:
        """Get authentication headers"""
        username = self.credentials.get('username')
        password = self.credentials.get('password')
        
        if username and password:
            import base64
            auth_string = f"{username}:{password}"
            auth_bytes = auth_string.encode('ascii')
            auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
            return {'Authorization': f'Basic {auth_b64}'}
        
        return {}
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> FHIRResponse:
        """
        Make HTTP request to FHIR server
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., '/Device')
            data: Request payload for POST/PUT
            
        Returns:
            FHIRResponse object
        """
        import requests
        
        url = f"{self.base_url}{endpoint}"
        headers = {**self.headers, **self._get_auth_header()}
        
        self.logger.debug(f"Making {method} request to: {url}")
        self.logger.debug(f"Headers: {headers}")
        if data:
            self.logger.debug(f"Payload: {json.dumps(data, indent=2)}")
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=self.timeout, verify=self.ssl_verify)
            elif method.upper() == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=self.timeout, verify=self.ssl_verify)
            elif method.upper() == 'PUT':
                response = requests.put(url, json=data, headers=headers, timeout=self.timeout, verify=self.ssl_verify)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=self.timeout, verify=self.ssl_verify)
            else:
                return FHIRResponse(
                    success=False,
                    status_code=0,
                    error_message=f"Unsupported HTTP method: {method}"
                )
            
            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")
            
            # Parse response
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json() if response.content else None
                    
                    # Log response details for debugging
                    self.logger.debug(f"Response content length: {len(response.content) if response.content else 0}")
                    self.logger.debug(f"Response headers: {dict(response.headers)}")
                    self.logger.debug(f"Response data: {response_data}")
                    
                    # Check if we have a Location header (common for resource creation)
                    location_header = response.headers.get('Location')
                    if location_header:
                        self.logger.info(f"Location header found: {location_header}")
                    
                    return FHIRResponse(
                        success=True,
                        status_code=response.status_code,
                        data=response_data,
                        headers=dict(response.headers)
                    )
                except json.JSONDecodeError:
                    # Even if JSON parsing fails, the request was successful
                    self.logger.warning("JSON decode failed, but request was successful")
                    return FHIRResponse(
                        success=True,
                        status_code=response.status_code,
                        data=None,
                        headers=dict(response.headers)
                    )
            else:
                error_msg = response.text if response.text else f"HTTP {response.status_code}"
                return FHIRResponse(
                    success=False,
                    status_code=response.status_code,
                    error_message=error_msg,
                    headers=dict(response.headers)
                )
                
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {e}"
            self.logger.error(error_msg)
            return FHIRResponse(
                success=False,
                status_code=0,
                error_message=error_msg
            )
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timeout: {e}"
            self.logger.error(error_msg)
            return FHIRResponse(
                success=False,
                status_code=0,
                error_message=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {e}"
            self.logger.error(error_msg)
            return FHIRResponse(
                success=False,
                status_code=0,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            self.logger.error(error_msg)
            return FHIRResponse(
                success=False,
                status_code=0,
                error_message=error_msg
            )
    
    def create_device(self, device_name: str) -> FHIRResponse:
        """
        Create a Device resource
        
        Args:
            device_name: Name of the device
            
        Returns:
            FHIRResponse with created Device resource
        """
        device_resource = {
            "resourceType": "Device",
            "deviceName": [
                {
                    "name": device_name,
                    "type": "model-name"
                }
            ]
        }
        
        self.logger.info(f"Creating Device resource: {device_name}")
        return self._make_request('POST', '/Device', device_resource)
    
    def get_device(self, device_id: str) -> FHIRResponse:
        """
        Get a Device resource by ID
        
        Args:
            device_id: Device resource ID
            
        Returns:
            FHIRResponse with Device resource
        """
        self.logger.info(f"Getting Device resource: {device_id}")
        return self._make_request('GET', f'/Device/{device_id}')
    
    def search_devices(self, search_params: Optional[Dict[str, str]] = None) -> FHIRResponse:
        """
        Search for Device resources
        
        Args:
            search_params: Search parameters (e.g., {'name': 'v2-to-fhir-pipeline'})
            
        Returns:
            FHIRResponse with search results
        """
        endpoint = '/Device'
        if search_params:
            query_string = '&'.join([f"{k}={v}" for k, v in search_params.items()])
            endpoint = f"{endpoint}?{query_string}"
        
        self.logger.info(f"Searching Device resources: {search_params or 'all'}")
        return self._make_request('GET', endpoint)
    
    def update_device(self, device_id: str, device_resource: Dict[str, Any]) -> FHIRResponse:
        """
        Update a Device resource
        
        Args:
            device_id: Device resource ID
            device_resource: Updated Device resource
            
        Returns:
            FHIRResponse with updated Device resource
        """
        self.logger.info(f"Updating Device resource: {device_id}")
        return self._make_request('PUT', f'/Device/{device_id}', device_resource)
    
    def delete_device(self, device_id: str) -> FHIRResponse:
        """
        Delete a Device resource
        
        Args:
            device_id: Device resource ID
            
        Returns:
            FHIRResponse indicating success/failure
        """
        self.logger.info(f"Deleting Device resource: {device_id}")
        return self._make_request('DELETE', f'/Device/{device_id}')
    
    def health_check(self) -> FHIRResponse:
        """
        Perform health check on FHIR server
        
        Returns:
            FHIRResponse indicating server health
        """
        self.logger.info("Performing health check")
        return self._make_request('GET', '/$healthcheck')
    
    def get_capability_statement(self) -> FHIRResponse:
        """
        Get FHIR server capability statement
        
        Returns:
            FHIRResponse with capability statement
        """
        self.logger.info("Getting capability statement")
        return self._make_request('GET', '/metadata')
    
    def create_resource(self, resource_type: str, resource_data: Dict[str, Any]) -> FHIRResponse:
        """
        Create any FHIR resource
        
        Args:
            resource_type: FHIR resource type (e.g., 'Patient', 'Observation')
            resource_data: Resource data
            
        Returns:
            FHIRResponse with created resource
        """
        self.logger.info(f"Creating {resource_type} resource")
        return self._make_request('POST', f'/{resource_type}', resource_data)
    
    def get_resource(self, resource_type: str, resource_id: str) -> FHIRResponse:
        """
        Get any FHIR resource by type and ID
        
        Args:
            resource_type: FHIR resource type
            resource_id: Resource ID
            
        Returns:
            FHIRResponse with resource
        """
        self.logger.info(f"Getting {resource_type} resource: {resource_id}")
        return self._make_request('GET', f'/{resource_type}/{resource_id}')


# Convenience function to get a default FHIR client
def get_fhir_client() -> FHIRClient:
    """Get default FHIR client instance"""
    return FHIRClient()


# Example usage and testing
if __name__ == "__main__":
    try:
        client = get_fhir_client()
        
        print("FHIR Client Test")
        print("=" * 50)
        
        # Test health check
        print("Testing health check...")
        health_response = client.health_check()
        if health_response.success:
            print("✓ Health check passed")
        else:
            print(f"✗ Health check failed: {health_response.error_message}")
        
        # Test capability statement
        print("\nTesting capability statement...")
        cap_response = client.get_capability_statement()
        if cap_response.success:
            print("✓ Capability statement retrieved")
            if cap_response.data:
                print(f"  FHIR Version: {cap_response.data.get('fhirVersion', 'Unknown')}")
        else:
            print(f"✗ Capability statement failed: {cap_response.error_message}")
        
        # Test device creation
        print("\nTesting device creation...")
        device_response = client.create_device("test-device")
        if device_response.success:
            print("✓ Device created successfully")
            if device_response.data and 'id' in device_response.data:
                device_id = device_response.data['id']
                print(f"  Device ID: {device_id}")
                
                # Test device retrieval
                print(f"\nTesting device retrieval for ID: {device_id}")
                get_response = client.get_device(device_id)
                if get_response.success:
                    print("✓ Device retrieved successfully")
                else:
                    print(f"✗ Device retrieval failed: {get_response.error_message}")
                
                # Clean up - delete test device
                print(f"\nCleaning up - deleting test device: {device_id}")
                delete_response = client.delete_device(device_id)
                if delete_response.success:
                    print("✓ Test device deleted successfully")
                else:
                    print(f"✗ Test device deletion failed: {delete_response.error_message}")
        else:
            print(f"✗ Device creation failed: {device_response.error_message}")
            
    except Exception as e:
        print(f"Error during testing: {e}")
        print("Make sure the FHIR server is running and accessible")
