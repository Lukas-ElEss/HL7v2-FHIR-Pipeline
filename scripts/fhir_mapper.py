#!/usr/bin/env python3
"""
FHIR Mapper for v2-to-fhir-pipeline

This module handles the transformation of InfoWashSource data to FHIR resources
using the Matchbox server's $transform operation.
"""

import json
import logging
from typing import Dict, Any
from matchbox_client import MatchboxClient
from hl7_parser import HL7Parser
from fhir_client import FHIRClient
from fhir_deduplicator import FHIRDeduplicationClient
from config import get_config

# Global configuration
STRUCTURE_MAP = "InfoWashSource-to-Bundle"


class FHIRMapper:
    """Mapper for transforming InfoWashSource to FHIR resources"""
    
    def __init__(self):
        """Initialize FHIR Mapper"""
        self.matchbox_client = MatchboxClient()
        self.hl7_parser = HL7Parser()
        self.logger = logging.getLogger(__name__)
        
        # Initialize FHIR clients
        self.config = get_config()
        self.fhir_client = FHIRClient(self.config)
        
        # Initialize deduplication client with credentials and SSL settings
        credentials = self.config.get_linuxforhealth_credentials()
        self.deduplication_client = FHIRDeduplicationClient(
            base_url=self.config.get_linuxforhealth_url(),
            device_identifier=f"Device/{self.config.get_device_default_name()}",
            username=credentials.get('username'),
            password=credentials.get('password'),
            ssl_verify=self.config.get_linuxforhealth_ssl_verify()
        )
    
    def complete_transformation_pipeline(self, hl7_message: str):
        """
        Complete transformation pipeline: HL7 → InfoWashSource → FHIR Bundle → Deduplication → Send to Server
        
        Args:
            hl7_message: Raw HL7 message string
            
        Returns:
            Dict with pipeline results
        """
        try:
            # Step 1: Parse HL7 message to InfoWashSource
            self.logger.info("Step 1: Parsing HL7 message to InfoWashSource")
            infowash_data = self.hl7_parser.parse_message(hl7_message)
            
            if not infowash_data:
                return {
                    "success": False,
                    "error": "Failed to parse HL7 message"
                }
            
            # Step 2: Convert to FHIR source format
            self.logger.info("Step 2: Converting to FHIR source format")
            fhir_source = infowash_data.to_fhir_source_format()
            
            # Log the InfoWashSource data
            self.logger.info("InfoWashSource data:")
            self.logger.info(json.dumps(fhir_source, indent=2))
            
            # Step 3: Transform to FHIR Bundle
            self.logger.info("Step 3: Transforming to FHIR Bundle")
            bundle_result = self.transform_infowash_to_bundle(json.dumps(fhir_source))
            
            if not bundle_result["success"]:
                return {
                    "success": False,
                    "error": f"Transformation failed: {bundle_result.get('error', 'Unknown error')}"
                }
            
            # Step 4: Perform deduplication
            self.logger.info("Step 4: Performing deduplication")
            deduplication_result = self.perform_deduplication(bundle_result["bundle"])
            
            if not deduplication_result["success"]:
                self.logger.warning(f"Deduplication failed: {deduplication_result.get('error', 'Unknown error')}")
                # Continue with pipeline even if deduplication fails
            
            # Step 5: Send bundle to LFH FHIR Server
            self.logger.info("Step 5: Sending bundle to LFH FHIR Server")
            send_result = self.send_bundle_to_server(bundle_result["bundle"])
            
            if not send_result["success"]:
                return {
                    "success": False,
                    "error": f"Failed to send bundle to server: {send_result.get('error_message', 'Unknown error')}",
                    "infowash_source": fhir_source,
                    "bundle": bundle_result["bundle"],
                    "deduplication_result": deduplication_result
                }
            
            return {
                "success": True,
                "infowash_source": fhir_source,
                "bundle": bundle_result["bundle"],
                "deduplication_result": deduplication_result,
                "send_result": send_result,
                "message": "Complete pipeline successful - Bundle sent to LFH FHIR Server"
            }
            
        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }
    
    def transform_infowash_to_bundle(self, infowash_data: str) -> Dict[str, Any]:
        """
        Transform InfoWashSource data to FHIR Bundle using StructureMap
        
        Args:
            infowash_data: InfoWashSource data as JSON string
            
        Returns:
            Dict with transformation result
        """
        try:
            self.logger.info("Starting transformation: InfoWashSource → FHIR Bundle")
            
            # Use the new matchbox_transform method from MatchboxClient with the global structure_map
            result = self.matchbox_client.matchbox_transform(infowash_data, STRUCTURE_MAP)
            
            if result["success"]:
                self.logger.info("Transformation successful")
                return {
                    "success": True,
                    "bundle": result["bundle"],
                    "message": "Transformation completed successfully"
                }
            else:
                self.logger.error(f"Transformation failed: {result.get('error', 'Unknown error')}")
                return {
                    "success": False,
                    "error": result.get('error', 'Unknown error'),
                    "message": "Transformation failed"
                }
                
        except Exception as e:
            error_msg = f"Transform execution error: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }

    def perform_deduplication(self, bundle_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform deduplication on the FHIR Bundle before sending to server
        
        Args:
            bundle_data: The FHIR Bundle data
            
        Returns:
            Dict with deduplication result
        """
        try:
            # Extract Patient and Encounter IDs from bundle
            patient_id = None
            encounter_id = None
            
            entries = bundle_data.get('entry', [])
            for entry in entries:
                resource = entry.get('resource', {})
                resource_type = resource.get('resourceType')
                
                if resource_type == 'Patient' and not patient_id:
                    patient_id = resource.get('id')
                elif resource_type == 'Encounter' and not encounter_id:
                    encounter_id = resource.get('id')
                
                # Stop if we have both IDs
                if patient_id and encounter_id:
                    break
            
            if not patient_id or not encounter_id:
                return {
                    "success": True,
                    "message": "No Patient/Encounter IDs found in bundle, skipping deduplication",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id
                }
            
            self.logger.info(f"Performing deduplication for Patient: {patient_id}, Encounter: {encounter_id}")
            
            # Search for existing provenance entries
            provenance_entries = self.deduplication_client.search_existing_provenance(patient_id, encounter_id)
            
            if provenance_entries:
                self.logger.info(f"Found {len(provenance_entries)} existing provenance entries, deleting old resources")
                
                # Delete old resources
                self.deduplication_client.delete_resources_from_provenance(provenance_entries)
                
                return {
                    "success": True,
                    "message": f"Successfully deleted {len(provenance_entries)} existing provenance entries and their resources",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "deleted_provenance_count": len(provenance_entries)
                }
            else:
                return {
                    "success": True,
                    "message": "No existing resources found, no deduplication needed",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "deleted_provenance_count": 0
                }
                
        except Exception as e:
            error_msg = f"Deduplication failed: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }
    
    def send_bundle_to_server(self, bundle_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send the FHIR Bundle to the LFH FHIR Server
        
        Args:
            bundle_data: The FHIR Bundle data
            
        Returns:
            Dict with send result
        """
        try:
            self.logger.info("Sending bundle to LFH FHIR Server")
            
            # Send bundle using FHIR client
            response = self.fhir_client.send_bundle_to_server(bundle_data)
            
            if response.success:
                self.logger.info("Bundle sent successfully to LFH FHIR Server")
                return {
                    "success": True,
                    "status_code": response.status_code,
                    "message": "Bundle sent successfully to LFH FHIR Server",
                    "response_data": response.data
                }
            else:
                error_msg = f"Failed to send bundle: {response.error_message}"
                self.logger.error(error_msg)
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "error": response.error_message,
                    "message": error_msg
                }
                
        except Exception as e:
            error_msg = f"Failed to send bundle to server: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }



