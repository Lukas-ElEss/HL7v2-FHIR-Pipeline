#!/usr/bin/env python3
"""
Device Registration Script for v2-to-fhir-pipeline

This script registers a Device resource on both FHIR servers (LinuxForHealth and Matchbox)
with the same identifier, so both servers can find the same device using the same search criteria.

Usage:
    python register_device.py [device_name]

If no device name is provided, it will prompt for input.
"""

import sys
from typing import Optional
from config import get_config
from fhir_client import get_fhir_client, FHIRResponse
from matchbox_client import MatchboxClient
import json


def register_device_with_client(device_name: str, client) -> Optional[str]:
    """
    Register the Device resource using the FHIR client
    
    Args:
        device_name: Name of the device to register
        client: FHIR client instance
        
    Returns:
        str: Full URL of the created Device resource, or None if failed
    """
    try:
        print(f"Registering Device resource: {device_name}")
        
        # Create device using FHIR client
        response = client.create_device(device_name)
        
        if response.success:
            print("✓ Device created successfully")
            
            # Extract device ID from multiple sources
            device_id = None
            full_url = None
            
            # Method 1: Try to get ID from response body
            if response.data and 'id' in response.data:
                device_id = response.data['id']
                print(f"Device ID from response body: {device_id}")
            
            # Method 2: Try to get ID from Location header
            elif response.headers and 'Location' in response.headers:
                location_header = response.headers['Location']
                print(f"Location header found: {location_header}")
                
                # Extract ID from Location header (e.g., "Device/12345" or full URL)
                if '/Device/' in location_header:
                    device_id = location_header.split('/Device/')[-1]
                    print(f"Device ID from Location header: {device_id}")
            
            # Method 3: Try to get ID from Content-Location header
            elif response.headers and 'Content-Location' in response.headers:
                content_location = response.headers['Content-Location']
                print(f"Content-Location header found: {content_location}")
                
                if '/Device/' in content_location:
                    device_id = content_location.split('/Device/')[-1]
                    print(f"Device ID from Content-Location header: {device_id}")
            
            # Build full URL if we have an ID
            if device_id:
                base_url = client.base_url
                full_url = f"{base_url}/Device/{device_id}"
                print(f"Full URL: {full_url}")
                return full_url
            else:
                print("Warning: No ID found in response")
                print(f"Response data: {response.data}")
                print(f"Response headers: {response.headers}")
                
                # Try to search for the device we just created
                print("Attempting to search for newly created device...")
                search_response = client.search_devices({'device-name': device_name})
                if search_response.success and response.data:
                    entries = search_response.data.get('entry', [])
                    if entries:
                        device = entries[0].get('resource', {})
                        if 'id' in device:
                            device_id = device['id']
                            base_url = client.base_url
                            full_url = f"{base_url}/Device/{device_id}"
                            print(f"Found device via search: {device_id}")
                            print(f"Full URL: {full_url}")
                            return full_url
                
                return None
        else:
            print(f"✗ Device creation failed")
            print(f"Status code: {response.status_code}")
            print(f"Error message: {response.error_message}")
            return None
            
    except Exception as e:
        print(f"Unexpected error during device registration: {e}")
        return None


def register_device_on_matchbox(device_name: str, device_identifier: str) -> Optional[str]:
    """
    Register the Device resource on the Matchbox server with the same identifier
    
    Args:
        device_name: Name of the device to register
        device_identifier: Identifier to use for the device (same as on LinuxForHealth)
        
    Returns:
        str: Full URL of the created Device resource on Matchbox, or None if failed
    """
    try:
        print(f"Registering Device resource on Matchbox: {device_name}")
        
        # Initialize Matchbox client
        matchbox_client = MatchboxClient()
        
        # Test connection first
        if not matchbox_client.test_connection()['success']:
            print("✗ Cannot connect to Matchbox server")
            return None
        
        # Create Device resource for Matchbox
        device_resource = {
            "resourceType": "Device",
            "identifier": [
                {
                    "system": "http://hsrt-kkrt.org/fhir/Device",
                    "value": device_identifier
                }
            ],
            "deviceName": [
                {
                    "name": device_name,
                    "type": "model-name"
                }
            ],
            "status": "active"
        }
        
        # Upload the Device resource
        print("Uploading Device resource to Matchbox...")
        print(f"Device data: {json.dumps(device_resource, indent=2)}")
        result = matchbox_client._upload_device_resource(device_resource)
        
        print(f"Upload result: {result}")
        
        if result['success']:
            print("✓ Device created on Matchbox successfully")
            # For Matchbox, we'll use a canonical URL since we don't have the actual server URL
            matchbox_url = f"http://hsrt-kkrt.org/fhir/Device/{device_identifier}"
            print(f"Matchbox Device URL: {matchbox_url}")
            return matchbox_url
        else:
            print(f"✗ Device creation on Matchbox failed: {result.get('error', 'Unknown error')}")
            return None
            
    except Exception as e:
        print(f"Unexpected error during Matchbox device registration: {e}")
        return None


def check_existing_device(device_name: str, client) -> Optional[str]:
    """
    Check if a device with the given name already exists
    
    Args:
        device_name: Name of the device to search for
        client: FHIR client instance
        
    Returns:
        str: Full URL of existing device, or None if not found
    """
    try:
        print(f"Checking for existing device: {device_name}")
        
        # Search for existing device by name
        search_params = {'device-name': device_name}
        response = client.search_devices(search_params)
        
        if response.success and response.data:
            # Check if we have any entries
            entries = response.data.get('entry', [])
            if entries:
                # Get the first device found
                device = entries[0].get('resource', {})
                if 'id' in device:
                    device_id = device['id']
                    base_url = client.base_url
                    full_url = f"{base_url}/Device/{device_id}"
                    
                    print(f"✓ Found existing device with ID: {device_id}")
                    return full_url
        
        print("No existing device found")
        return None
        
    except Exception as e:
        print(f"Warning: Error checking for existing device: {e}")
        return None


def main():
    """Main function to run the device registration"""
    
    print("Device Registration Script for v2-to-fhir-pipeline")
    print("=" * 60)
    
    try:
        # Load configuration
        config = get_config()
        
        # Validate configuration
        if not config.validate_config():
            print("Configuration validation failed. Please check config/fhir_config.yaml")
            sys.exit(1)
        
        print("Configuration loaded successfully")
        print(f"FHIR Server: {config.get_linuxforhealth_url()}")
        print(f"Credentials: {config.get_linuxforhealth_credentials()['username']}")
        print()
        
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        print("Make sure config/fhir_config.yaml exists and is valid")
        sys.exit(1)
    
    try:
        # Initialize FHIR client
        client = get_fhir_client()
        
        # Test connection with health check
        print("Testing FHIR server connection...")
        health_response = client.health_check()
        if health_response.success:
            print("✓ FHIR server connection successful")
        else:
            print(f"✗ FHIR server connection failed: {health_response.error_message}")
            print("Please check if the FHIR server is running and accessible")
            sys.exit(1)
        
        print()
        
    except Exception as e:
        print(f"Failed to initialize FHIR client: {e}")
        sys.exit(1)
    
    # Get device name from command line argument or prompt
    if len(sys.argv) > 1:
        device_name = sys.argv[1]
    else:
        device_name = input(f"Enter device name (default: '{config.get_device_default_name()}'): ").strip()
        if not device_name:
            device_name = config.get_device_default_name()
    
    if not device_name:
        print("Error: Device name cannot be empty")
        sys.exit(1)
    
    print(f"Device name: {device_name}")
    print()
    
    # Generate a common identifier for both servers
    import uuid
    device_identifier = str(uuid.uuid4())
    print(f"Generated device identifier: {device_identifier}")
    print()
    
    # Check if device already exists on LinuxForHealth
    existing_url = check_existing_device(device_name, client)
    if existing_url:
        print(f"\nDevice '{device_name}' already exists on LinuxForHealth!")
        print(f"Existing URL: {existing_url}")
        
        # Ask user if they want to use existing device
        use_existing = input("Use existing device? (y/n): ").strip().lower()
        if use_existing in ['y', 'yes']:
            linuxforhealth_url = existing_url
        else:
            print("Proceeding with new device registration...")
            linuxforhealth_url = register_device_with_client(device_name, client)
    else:
        # Register new device on LinuxForHealth
        linuxforhealth_url = register_device_with_client(device_name, client)
    
    if not linuxforhealth_url:
        print("\n" + "=" * 60)
        print("ERROR: Failed to get device URL from LinuxForHealth")
        print("Please check the error messages above and try again.")
        sys.exit(1)
    
    # Now register device on Matchbox with the same identifier
    print("\n" + "=" * 60)
    print("Registering device on Matchbox server...")
    print("=" * 60)
    
    matchbox_url = register_device_on_matchbox(device_name, device_identifier)
    
    if matchbox_url:
        print("\n" + "=" * 60)
        print("SUCCESS: Device registered on both servers!")
        print("=" * 60)
        print(f"LinuxForHealth Device URL: {linuxforhealth_url}")
        print(f"Matchbox Device URL: {matchbox_url}")
        print(f"Common Identifier: {device_identifier}")
        print("\nUse the LinuxForHealth URL as DEVICE_fullUrl in your InfoWashSource structure.")
        print("Example:")
        print(f'  "DEVICE_fullUrl": "{linuxforhealth_url}"')
        
        # Save the URL to a file if configured to do so
        if config.should_save_device_url():
            output_file = config.get_device_url_file()
            try:
                with open(output_file, 'w') as f:
                    f.write(linuxforhealth_url)
                print(f"\nDevice URL saved to: {output_file}")
            except Exception as e:
                print(f"Warning: Could not save device URL to file: {e}")
        else:
            print("\nDevice URL not saved to file (disabled in configuration)")
    else:
        print("\n" + "=" * 60)
        print("WARNING: Device registration on Matchbox failed")
        print("The device is available on LinuxForHealth but not on Matchbox.")
        print("This may cause issues with the FHIR transformation pipeline.")
        print(f"LinuxForHealth Device URL: {linuxforhealth_url}")
        
        # Still save the LinuxForHealth URL
        if config.should_save_device_url():
            output_file = config.get_device_url_file()
            try:
                with open(output_file, 'w') as f:
                    f.write(linuxforhealth_url)
                print(f"\nDevice URL saved to: {output_file}")
            except Exception as e:
                print(f"Warning: Could not save device URL to file: {e}")


if __name__ == "__main__":
    main()
