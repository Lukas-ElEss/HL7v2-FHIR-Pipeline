#!/usr/bin/env python3
"""
Script to delete all resources from FHIR server
"""

import requests
import urllib3
import json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# FHIR server configuration
BASE_URL = "https://localhost:9443/fhir-server/api/v4"
USERNAME = "fhiruser"
PASSWORD = "change-password"

# Resource types to delete
RESOURCE_TYPES = ['Bundle', 'ServiceRequest', 'Patient', 'Condition', 'Encounter', 'Provenance']

def get_all_resource_ids(resource_type):
    """Get all resource IDs for a given type, handling pagination"""
    all_ids = []
    url = f"{BASE_URL}/{resource_type}"
    
    while url:
        print(f"  Fetching page from {url}")
        response = requests.get(url, auth=(USERNAME, PASSWORD), verify=False)
        
        if response.status_code != 200:
            print(f"  Failed to get {resource_type} resources: {response.status_code}")
            break
            
        data = response.json()
        
        # Get IDs from current page
        page_ids = []
        for entry in data.get('entry', []):
            resource_id = entry['resource']['id']
            page_ids.append(resource_id)
        
        all_ids.extend(page_ids)
        print(f"  Found {len(page_ids)} resources on this page (total so far: {len(all_ids)})")
        
        # Check for next page
        url = None
        for link in data.get('link', []):
            if link.get('relation') == 'next':
                url = link.get('url')
                break
    
    return all_ids

def delete_all_resources():
    """Delete all resources of specified types"""
    
    for resource_type in RESOURCE_TYPES:
        print(f"\n=== Processing {resource_type} resources ===")
        
        # Get all resource IDs (handling pagination)
        print(f"Getting all {resource_type} resource IDs...")
        resource_ids = get_all_resource_ids(resource_type)
        
        if not resource_ids:
            print(f"No {resource_type} resources to delete")
            continue
        
        print(f"Found {len(resource_ids)} {resource_type} resources total")
        print(f"Deleting all {len(resource_ids)} {resource_type} resources...")
        
        # Delete each resource individually
        deleted_count = 0
        for i, resource_id in enumerate(resource_ids, 1):
            try:
                delete_url = f"{BASE_URL}/{resource_type}/{resource_id}"
                delete_response = requests.delete(delete_url, auth=(USERNAME, PASSWORD), verify=False)
                
                if delete_response.status_code in [200, 204]:
                    deleted_count += 1
                    if i % 10 == 0 or i == len(resource_ids):
                        print(f"  Progress: {i}/{len(resource_ids)} (deleted: {deleted_count})")
                else:
                    print(f"  ✗ Failed to delete {resource_type}/{resource_id}: {delete_response.status_code}")
                    if delete_response.text:
                        print(f"    Error: {delete_response.text}")
                        
            except Exception as e:
                print(f"  ✗ Error deleting {resource_type}/{resource_id}: {e}")
        
        print(f"Successfully deleted {deleted_count}/{len(resource_ids)} {resource_type} resources")
    
    print("\n=== Final check ===")
    for resource_type in RESOURCE_TYPES:
        url = f"{BASE_URL}/{resource_type}"
        response = requests.get(url, auth=(USERNAME, PASSWORD), verify=False)
        if response.status_code == 200:
            data = response.json()
            total = data.get('total', 0)
            print(f"{resource_type}: {total} resources remaining")
        else:
            print(f"{resource_type}: Error checking resources")

if __name__ == "__main__":
    delete_all_resources()
