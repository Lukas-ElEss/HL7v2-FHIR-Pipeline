#!/usr/bin/env python3
"""
Script to check if FHIR Bundles are present on the LFH server

This script queries the FHIR server to verify that the bundles
sent by the testapp.py are actually stored on the server.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List

# Add scripts directory to path for imports
sys.path.append(str(Path(__file__).parent))

from fhir_client import get_fhir_client
from config import get_config


def check_bundles_on_server():
    """Check if bundles are present on the FHIR server"""
    print("=" * 60)
    print("Checking FHIR Bundles on LFH Server")
    print("=" * 60)
    
    try:
        # Initialize FHIR client
        client = get_fhir_client()
        config = get_config()
        
        print(f"Server URL: {config.get_linuxforhealth_url()}")
        print(f"Credentials: {config.get_linuxforhealth_credentials()['username']}")
        print()
        
        # Test server connectivity
        print("1. Testing server connectivity...")
        health_response = client.health_check()
        if not health_response.success:
            print(f"   ✗ Server health check failed: {health_response.error_message}")
            print("   Make sure the LFH server is running!")
            return False
        else:
            print("   ✓ Server is accessible")
        
        # Get capability statement
        print("\n2. Getting server capability statement...")
        cap_response = client.get_capability_statement()
        if cap_response.success:
            print("   ✓ Capability statement retrieved")
            if cap_response.data:
                fhir_version = cap_response.data.get('fhirVersion', 'Unknown')
                print(f"   FHIR Version: {fhir_version}")
        else:
            print(f"   ✗ Capability statement failed: {cap_response.error_message}")
        
        # Search for Bundles
        print("\n3. Searching for Bundle resources...")
        bundle_response = client._make_request('GET', '/Bundle')
        
        if bundle_response.success:
            print("   ✓ Bundle search successful")
            
            if bundle_response.data and 'entry' in bundle_response.data:
                bundles = bundle_response.data['entry']
                total_bundles = len(bundles)
                print(f"   Found {total_bundles} Bundle(s) on server")
                
                if total_bundles > 0:
                    print("\n4. Bundle Details:")
                    print("-" * 40)
                    
                    for i, bundle_entry in enumerate(bundles, 1):
                        bundle_resource = bundle_entry.get('resource', {})
                        bundle_id = bundle_resource.get('id', 'Unknown')
                        bundle_type = bundle_resource.get('type', 'Unknown')
                        bundle_timestamp = bundle_resource.get('timestamp', 'Unknown')
                        
                        print(f"   Bundle {i}:")
                        print(f"     ID: {bundle_id}")
                        print(f"     Type: {bundle_type}")
                        print(f"     Timestamp: {bundle_timestamp}")
                        
                        # Count entries in bundle
                        entries = bundle_resource.get('entry', [])
                        entry_count = len(entries)
                        print(f"     Entries: {entry_count}")
                        
                        if entry_count > 0:
                            print("     Resource Types in Bundle:")
                            resource_types = set()
                            for entry in entries:
                                resource = entry.get('resource', {})
                                resource_type = resource.get('resourceType', 'Unknown')
                                resource_types.add(resource_type)
                            
                            for resource_type in sorted(resource_types):
                                count = sum(1 for entry in entries 
                                          if entry.get('resource', {}).get('resourceType') == resource_type)
                                print(f"       - {resource_type}: {count}")
                        
                        print()
                else:
                    print("   No bundles found on server")
            else:
                print("   No bundle entries found in response")
                print(f"   Response data: {json.dumps(bundle_response.data, indent=2)}")
        else:
            print(f"   ✗ Bundle search failed: {bundle_response.error_message}")
            return False
        
        # Search for specific resource types that should be in bundles
        print("\n5. Checking for specific resource types...")
        resource_types_to_check = ['Patient', 'Observation', 'ServiceRequest', 'Device', 'Condition']
        
        for resource_type in resource_types_to_check:
            print(f"   Searching for {resource_type} resources...")
            search_response = client._make_request('GET', f'/{resource_type}')
            
            if search_response.success:
                if search_response.data and 'entry' in search_response.data:
                    count = len(search_response.data['entry'])
                    print(f"     ✓ Found {count} {resource_type} resource(s)")
                    
                    # Show details for first few resources
                    if count > 0 and resource_type in ['Patient', 'Condition', 'ServiceRequest']:
                        entries = search_response.data['entry'][:3]  # Show first 3
                        for i, entry in enumerate(entries, 1):
                            resource = entry.get('resource', {})
                            resource_id = resource.get('id', 'Unknown')
                            
                            if resource_type == 'Patient':
                                name = resource.get('name', [{}])[0]
                                family = name.get('family', 'Unknown')
                                given = name.get('given', ['Unknown'])[0]
                                print(f"       {i}. {given} {family} (ID: {resource_id})")
                            elif resource_type == 'Condition':
                                code = resource.get('code', {}).get('coding', [{}])[0]
                                display = code.get('display', 'Unknown')
                                print(f"       {i}. {display} (ID: {resource_id})")
                            elif resource_type == 'ServiceRequest':
                                code = resource.get('code', {}).get('coding', [{}])[0]
                                display = code.get('display', 'Unknown')
                                print(f"       {i}. {display} (ID: {resource_id})")
                else:
                    print(f"     - No {resource_type} resources found")
            else:
                print(f"     ✗ {resource_type} search failed: {search_response.error_message}")
        
        print("\n" + "=" * 60)
        print("Bundle check completed")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"Error during bundle check: {e}")
        return False


def search_bundles_by_criteria():
    """Search for bundles with specific criteria"""
    print("\n" + "=" * 60)
    print("Advanced Bundle Search")
    print("=" * 60)
    
    try:
        client = get_fhir_client()
        
        # Search for bundles with specific parameters
        search_params = [
            {'type': 'transaction'},  # Transaction bundles
            {'_count': '10'},         # Limit results
            {'_sort': '-_lastUpdated'} # Sort by last updated
        ]
        
        for params in search_params:
            print(f"\nSearching with parameters: {params}")
            response = client._make_request('GET', f'/Bundle?{list(params.keys())[0]}={list(params.values())[0]}')
            
            if response.success and response.data and 'entry' in response.data:
                count = len(response.data['entry'])
                print(f"   Found {count} bundles")
            else:
                print(f"   No results or error: {response.error_message}")
        
    except Exception as e:
        print(f"Error during advanced search: {e}")


def main():
    """Main function"""
    print("FHIR Bundle Verification Tool")
    print("This tool checks if bundles sent by testapp.py are stored on the LFH server")
    print()
    
    # Check if server is accessible
    success = check_bundles_on_server()
    
    if success:
        # Perform advanced search
        search_bundles_by_criteria()
    else:
        print("\nBundle check failed. Please ensure:")
        print("1. LFH server is running")
        print("2. Server is accessible at the configured URL")
        print("3. Credentials are correct")
        print("4. testapp.py has been executed successfully")


if __name__ == "__main__":
    main()
