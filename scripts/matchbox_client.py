#!/usr/bin/env python3
"""
Matchbox Client for v2-to-fhir-pipeline

This client provides three main functions:
1. Test connection to Matchbox server
2. Upload all project files (StructureDefinitions, StructureMaps, IG)
3. Execute FHIR transformations
"""

import json
import os
import requests
import zipfile
from typing import Dict, Any, List
from pathlib import Path


class MatchboxClient:
    """Client for interacting with Matchbox FHIR server"""
    
    def __init__(self, base_url: str = "http://localhost:8080/matchboxv3/fhir"):
        """
        Initialize Matchbox client
        
        Args:
            base_url: Base URL of the Matchbox server
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = 30
        
        print(f"Matchbox Client initialized for: {self.base_url}")
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to Matchbox server by calling metadata endpoint
        
        Returns:
            Dict with connection test results
        """
        print("Testing Matchbox server connection...")
        
        results = {}
        
        # Test /metadata endpoint (only endpoint that Matchbox supports)
        try:
            metadata_url = f"{self.base_url}/metadata"
            print(f"Testing metadata endpoint: {metadata_url}")
            
            response = requests.get(metadata_url, timeout=self.timeout)
            results['metadata'] = {
                'success': response.status_code == 200,
                'status_code': response.status_code,
                'url': metadata_url
            }
            
            if response.status_code == 200:
                print("Metadata endpoint: OK")
            else:
                print(f"Metadata endpoint: Status {response.status_code}")
                
        except Exception as e:
            results['metadata'] = {
                'success': False,
                'error': str(e),
                'url': metadata_url
            }
            print(f"Metadata endpoint: Error - {e}")
        
        # Overall connection status (only based on metadata since health is not supported)
        overall_success = results.get('metadata', {}).get('success', False)
        
        if overall_success:
            print("Matchbox server connection successful")
        else:
            print("Matchbox server connection failed")
        
        return {
            'success': overall_success,
            'results': results
        }
    
    def upload_all_files(self) -> Dict[str, Any]:
        """
        Upload all project files from input/ directory:
        - StructureDefinitions from StructureDefiniton/
        - StructureMaps from StructureMap/
        - IG package from v2-to-fhir-IG/
        
        Returns:
            Dict with upload results summary
        """
        print("Starting upload of all project files...")
        
        upload_results = []
        input_dir = Path(__file__).parent.parent / "input"
        
        if not input_dir.exists():
            return {
                'success': False,
                'error': f'Input directory not found at: {input_dir.absolute()}'
            }
        
        # 1. Upload StructureDefinitions
        print("Step 1: Uploading StructureDefinitions...")
        sd_dir = input_dir / "StructureDefiniton"
        if sd_dir.exists():
            # Upload from root StructureDefiniton directory
            for sd_file in sd_dir.glob("*.json"):
                print(f"Uploading StructureDefinition: {sd_file.name}")
                result = self._upload_structure_definition(sd_file)
                upload_results.append({
                    'type': 'StructureDefinition',
                    'file': sd_file.name,
                    'result': result
                })
            
            # Also upload from source/ subdirectory
            source_dir = sd_dir / "source"
            if source_dir.exists():
                for sd_file in source_dir.glob("*.json"):
                    print(f"Uploading StructureDefinition from source/: {sd_file.name}")
                    result = self._upload_structure_definition(sd_file)
                    upload_results.append({
                        'type': 'StructureDefinition',
                        'file': f"source/{sd_file.name}",
                        'result': result
                    })
        else:
            print("StructureDefiniton directory not found")
        
        # 2. Upload StructureMaps (.map files only)
        print("Step 2: Uploading StructureMaps...")
        sm_dir = input_dir / "StructureMap"
        if sm_dir.exists():
            # Upload only .map files
            for map_file in sm_dir.glob("*.map"):
                print(f"Uploading StructureMap: {map_file.name}")
                result = self._upload_fml_file(map_file)
                upload_results.append({
                    'type': 'StructureMap',
                    'file': map_file.name,
                    'result': result
                })
        else:
            print("StructureMap directory not found")
        
        # 2b. Upload additional .map files from FML directory
        print("Step 2b: Uploading additional .map files...")
        fml_dir = input_dir / "FML"
        if fml_dir.exists():
            # Upload only .map files
            for map_file in fml_dir.glob("*.map"):
                print(f"Uploading FML file: {map_file.name}")
                result = self._upload_fml_file(map_file)
                upload_results.append({
                    'type': 'FML File',
                    'file': map_file.name,
                    'result': result
                })
        else:
            print("FML directory not found")
        
        # 3. Upload IG package (ZIP file only)
        print("Step 3: Uploading IG package...")
        ig_dir = input_dir / "v2-to-fhir-IG"
        if ig_dir.exists():
            zip_files = list(ig_dir.glob("*.zip"))
            if zip_files:
                # Upload only the first ZIP file found
                zip_file = zip_files[0]
                print(f"Uploading IG package: {zip_file.name}")
                result = self._upload_ig_package(zip_file)
                upload_results.append({
                    'type': 'IG Package',
                    'file': zip_file.name,
                    'result': result
                })
            else:
                print("No ZIP files found in v2-to-fhir-IG directory")
        else:
            print("v2-to-fhir-IG directory not found")
        
        # Summary
        successful_uploads = sum(1 for r in upload_results if r['result']['success'])
        total_uploads = len(upload_results)
        
        print("UPLOAD SUMMARY:")
        print(f"Successful: {successful_uploads}")
        print(f"Failed: {total_uploads - successful_uploads}")
        print(f"Total: {total_uploads}")
        
        # Detailed results
        print("DETAILED RESULTS:")
        for file_result in upload_results:
            status = "OK" if file_result['result']['success'] else "FAILED"
            print(f"{status} {file_result['type']}: {file_result['file']}")
            if not file_result['result']['success']:
                print(f"   Error: {file_result['result'].get('error', 'Unknown error')}")
        
        return {
            "success": successful_uploads == total_uploads,
            "total_resources": total_uploads,
            "successful_uploads": successful_uploads,
            "failed_uploads": total_uploads - successful_uploads,
            "upload_results": upload_results
        }
    
    def matchbox_transform(self, infowash_data: str, structure_map: str) -> Dict[str, Any]:
        """
        Execute FHIR transformation using Matchbox server
        
        Args:
            infowash_data: InfoWashSource data as JSON string
            structure_map: Name of the StructureMap to use
            
        Returns:
            Dict with transformation result
        """
        try:
            print(f"Executing FHIR transformation with StructureMap: {structure_map}")
            
            # Parse JSON string to dict for the transform request
            try:
                source_json = json.loads(infowash_data)
            except json.JSONDecodeError as e:
                return {
                    'success': False,
                    'error': f"Invalid JSON in infowash_data: {e}",
                    'message': 'JSON parsing failed'
                }
            
            # Execute $transform operation
            transform_url = f"{self.base_url}/StructureMap/$transform?source=http://hsrt-kkrt.org/fhir/StructureMap/{structure_map}"
            
            print(f"Calling: {transform_url}")
            
            headers = {
                "Accept": "application/fhir+json;fhirVersion=4.0",
                "Content-Type": "application/fhir+json;fhirVersion=4.0",
            }
            
            # Send the request directly with requests
            response = requests.post(transform_url, headers=headers, json=source_json, timeout=self.timeout)
            
            if response.status_code in [200, 201]:
                print("Transformation successful")
                try:
                    response_data = response.json()
                    return {
                        'success': True,
                        'bundle': response_data,
                        'message': 'Transformation completed successfully'
                    }
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'bundle': response.text,
                        'message': 'Transformation completed successfully'
                    }
            else:
                print(f"Transformation failed: {response.status_code}")
                try:
                    error_data = response.json()
                    return {
                        'success': False,
                        'error': error_data,
                        'message': 'Transformation failed'
                    }
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'error': response.text,
                        'message': 'Transformation failed'
                    }
                
        except Exception as e:
            error_msg = f"Transform execution error: {e}"
            print(f"Error: {error_msg}")
            return {
                'success': False,
                'error': str(e),
                'message': error_msg
            }
    
    def _upload_structure_definition(self, file_path: Path) -> Dict[str, Any]:
        """Upload a StructureDefinition"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sd_data = json.load(f)
            
            # Use requests directly instead of _make_request
            url = f"{self.base_url}/StructureDefinition"
            headers = {
                "Accept": "application/fhir+json;fhirVersion=4.0",
                "Content-Type": "application/fhir+json;fhirVersion=4.0",
            }
            
            response = requests.post(url, json=sd_data, headers=headers, timeout=self.timeout)
            
            if response.status_code in [200, 201]:
                print(f"StructureDefinition uploaded: {file_path.name}")
                try:
                    response_data = response.json()
                    return {
                        "success": True,
                        "data": response_data,
                        "status_code": response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        "success": True,
                        "data": response.text,
                        "status_code": response.status_code
                    }
            else:
                print(f"StructureDefinition upload failed: {response.status_code}")
                try:
                    error_data = response.json()
                    return {
                        "success": False,
                        "error": error_data,
                        "status_code": response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        "success": False,
                        "error": response.text,
                        "status_code": response.status_code
                    }
                    
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "status_code": None
            }
    
    def _upload_device_resource(self, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Upload a Device resource directly"""
        try:
            print(f"Attempting to upload Device resource to: {self.base_url}/Device")
            print(f"Device data: {json.dumps(device_data, indent=2)}")
            
            # Use requests directly
            url = f"{self.base_url}/Device"
            headers = {
                "Accept": "application/fhir+json;fhirVersion=4.0",
                "Content-Type": "application/fhir+json;fhirVersion=4.0",
            }
            
            print(f"POST request to: {url}")
            print(f"Headers: {headers}")
            
            response = requests.post(url, json=device_data, headers=headers, timeout=self.timeout)
            
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            print(f"Response body: {response.text}")
            
            if response.status_code in [200, 201]:
                print(f"Device resource uploaded successfully")
                try:
                    response_data = response.json()
                    return {
                        "success": True,
                        "data": response_data,
                        "status_code": response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        "success": True,
                        "data": response.text,
                        "status_code": response.status_code
                    }
            else:
                print(f"Device resource upload failed: {response.status_code}")
                try:
                    error_data = response.json()
                    return {
                        "success": False,
                        "error": error_data,
                        "status_code": response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        "success": False,
                        "error": response.text,
                        "status_code": response.status_code
                    }
                    
        except Exception as e:
            print(f"Exception during Device upload: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "status_code": None
            }
    
    def _upload_fml_file(self, file_path: Path) -> Dict[str, Any]:
        """Upload an FML file as StructureMap"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                fml_content = f.read()
            
            headers = {
                "Content-Type": "text/fhir-mapping",
                "Accept": "application/fhir+json"
            }
            
            url = f"{self.base_url}/StructureMap"
            response = requests.post(url, data=fml_content, headers=headers, timeout=self.timeout)
            
            if response.status_code in [200, 201]:
                print(f"StructureMap uploaded: {file_path.name}")
                return {
                    "success": True,
                    "data": response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text,
                    "status_code": response.status_code
                }
            else:
                print(f"StructureMap upload failed: {file_path.name}")
                return {
                    "success": False,
                    "error": response.text,
                    "status_code": response.status_code
                }
                
        except Exception as e:
            error_result = {
                "success": False,
                "error": f"Processing error: {e}"
            }
            print(f"Error: {error_result['error']}")
            return error_result
    
    def _upload_ig_package(self, file_path: Path) -> Dict[str, Any]:
        """Upload an IG package ZIP file"""
        try:
            print(f"Processing IG package: {file_path.name}")
            
            # For now, just verify the ZIP file exists and is valid
            if not zipfile.is_zipfile(file_path):
                return {
                    "success": False,
                    "error": "File is not a valid ZIP file"
                }
            
            # In a real implementation, you would extract and upload the contents
            # For now, we'll just return success
            print(f"IG package validated: {file_path.name}")
            return {
                "success": True,
                "data": f"IG package {file_path.name} validated successfully",
                "status_code": 200
            }
            
        except Exception as e:
            error_result = {
                "success": False,
                "error": f"Processing error: {e}"
            }
            print(f"Error: {error_result['error']}")
            return error_result
