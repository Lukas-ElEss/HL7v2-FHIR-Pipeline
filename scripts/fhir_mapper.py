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

# Global configuration
STRUCTURE_MAP = "InfoWashSource-to-Bundle"


class FHIRMapper:
    """Mapper for transforming InfoWashSource to FHIR resources"""
    
    def __init__(self):
        """Initialize FHIR Mapper"""
        self.matchbox_client = MatchboxClient()
        self.hl7_parser = HL7Parser()
        self.logger = logging.getLogger(__name__)
    
    def complete_transformation_pipeline(self, hl7_message: str):
        """
        Complete transformation pipeline: HL7 → InfoWashSource → FHIR Bundle
        
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
            
            # Step 4: Save bundle to local folder
            self.logger.info("Step 4: Saving bundle to local folder")
            save_result = self.save_bundle_to_folder(bundle_result["bundle"])
            
            return {
                "success": True,
                "infowash_source": fhir_source,
                "bundle": bundle_result["bundle"],
                "save_result": save_result,
                "message": "Complete pipeline successful"
            }
            
        except Exception as e:
            error_msg = f"Pipeline failed at transformation step: {e}"
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

    def save_bundle_to_folder(self, bundle_data: Dict[str, Any], folder_path: str = "output/bundles") -> Dict[str, Any]:
        """
        Save the transformed FHIR Bundle to a local folder
        
        Args:
            bundle_data: The FHIR Bundle data to save
            folder_path: Path to the folder where bundles should be saved
            
        Returns:
            Dict with save result
        """
        try:
            import os
            from datetime import datetime
            
            # Create output directory if it doesn't exist
            os.makedirs(folder_path, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bundle_{timestamp}.json"
            file_path = os.path.join(folder_path, filename)
            
            # Save bundle to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(bundle_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Bundle saved to: {file_path}")
            
            return {
                "success": True,
                "file_path": file_path,
                "filename": filename,
                "message": f"Bundle saved successfully to {file_path}"
            }
            
        except Exception as e:
            error_msg = f"Failed to save bundle to folder: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }



