#!/usr/bin/env python3
"""
Configuration management for FHIR server settings

This module loads configuration from config/fhir_config.yaml and provides
easy access to FHIR server URLs, credentials, and other settings.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class FHIRConfig:
    """Configuration manager for FHIR server settings"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration from YAML file
        
        Args:
            config_file: Path to configuration file. If None, uses default location.
        """
        if config_file is None:
            # Default config file location relative to scripts directory
            script_dir = Path(__file__).parent
            config_file = script_dir.parent / "config" / "fhir_config.yaml"
        
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            if not self.config_file.exists():
                raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            return config or {}
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {e}")
    
    def get_linuxforhealth_url(self) -> str:
        """Get Linux for Health FHIR server URL"""
        return self.config.get('linuxforhealth', {}).get('fhir', {}).get('server_url', 'http://10.0.2.2:5555/fhir')
    
    def get_linuxforhealth_credentials(self) -> Dict[str, str]:
        """Get Linux for Health FHIR server credentials"""
        creds = self.config.get('linuxforhealth', {}).get('fhir', {}).get('credentials', {})
        return {
            'username': creds.get('username', 'fhiruser'),
            'password': creds.get('password', 'change-password')
        }
    
    def get_linuxforhealth_timeout(self) -> int:
        """Get Linux for Health FHIR server timeout in seconds"""
        return self.config.get('linuxforhealth', {}).get('fhir', {}).get('timeout', 30)
    
    def get_linuxforhealth_headers(self) -> Dict[str, str]:
        """Get Linux for Health FHIR server headers"""
        headers = self.config.get('linuxforhealth', {}).get('fhir', {}).get('headers', {})
        return {
            'Content-Type': headers.get('content_type', 'application/fhir+json'),
            'Accept': headers.get('accept', 'application/fhir+json')
        }
    
    def get_linuxforhealth_ssl_verify(self) -> bool:
        """Get Linux for Health FHIR server SSL verification setting"""
        return self.config.get('linuxforhealth', {}).get('fhir', {}).get('ssl_verify', True)
    
    def get_matchbox_url(self) -> str:
        """Get Matchbox server URL"""
        return self.config.get('matchbox', {}).get('server_url', 'http://localhost:8080/matchboxv3/fhir')
    
    def get_matchbox_context_path(self) -> str:
        """Get Matchbox context path"""
        return self.config.get('matchbox', {}).get('context_path', '/matchboxv3')
    
    def get_matchbox_port(self) -> int:
        """Get Matchbox server port"""
        return self.config.get('matchbox', {}).get('port', 8080)
    
    def get_device_default_name(self) -> str:
        """Get default device name"""
        return self.config.get('device', {}).get('default_name', 'v2-to-fhir-pipeline')
    
    def get_device_resource_type(self) -> str:
        """Get device resource type"""
        return self.config.get('device', {}).get('resource_type', 'Device')
    
    def get_device_name_type(self) -> str:
        """Get device name type"""
        return self.config.get('device', {}).get('name_type', 'model-name')
    
    def should_save_device_url(self) -> bool:
        """Check if device URL should be saved to file"""
        return self.config.get('output', {}).get('save_device_url', True)
    
    def get_device_url_file(self) -> str:
        """Get device URL output file name"""
        return self.config.get('output', {}).get('device_url_file', 'device_url.txt')
    
    def get_log_level(self) -> str:
        """Get logging level"""
        return self.config.get('output', {}).get('log_level', 'INFO')
    
    def reload_config(self):
        """Reload configuration from file"""
        self.config = self._load_config()
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get complete configuration dictionary"""
        return self.config.copy()
    
    def validate_config(self) -> bool:
        """Validate that required configuration values are present"""
        required_sections = ['linuxforhealth', 'matchbox']
        required_linuxforhealth = ['fhir']
        required_fhir = ['server_url', 'credentials']
        required_credentials = ['username', 'password']
        
        try:
            # Check required sections
            for section in required_sections:
                if section not in self.config:
                    print(f"Missing required section: {section}")
                    return False
            
            # Check linuxforhealth.fhir section
            fhir_section = self.config['linuxforhealth'].get('fhir', {})
            if not fhir_section:
                print("Missing linuxforhealth.fhir section")
                return False
            
            # Check required fhir fields
            for field in required_fhir:
                if field not in fhir_section:
                    print(f"Missing required field: linuxforhealth.fhir.{field}")
                    return False
            
            # Check required credentials
            credentials = fhir_section['credentials']
            for cred in required_credentials:
                if cred not in credentials:
                    print(f"Missing required credential: linuxforhealth.fhir.credentials.{cred}")
                    return False
            
            return True
            
        except Exception as e:
            print(f"Configuration validation error: {e}")
            return False


# Convenience function to get a default config instance
def get_config() -> FHIRConfig:
    """Get default configuration instance"""
    return FHIRConfig()


# Example usage and testing
if __name__ == "__main__":
    try:
        config = get_config()
        
        print("FHIR Configuration:")
        print("=" * 50)
        print(f"Linux for Health URL: {config.get_linuxforhealth_url()}")
        print(f"Linux for Health Credentials: {config.get_linuxforhealth_credentials()}")
        print(f"Linux for Health Timeout: {config.get_linuxforhealth_timeout()}s")
        print(f"Linux for Health Headers: {config.get_linuxforhealth_headers()}")
        print(f"Linux for Health SSL Verify: {config.get_linuxforhealth_ssl_verify()}")
        print(f"Matchbox URL: {config.get_matchbox_url()}")
        print(f"Matchbox Context Path: {config.get_matchbox_context_path()}")
        print(f"Matchbox Port: {config.get_matchbox_port()}")
        print(f"Device Default Name: {config.get_device_default_name()}")
        print(f"Device Resource Type: {config.get_device_resource_type()}")
        print(f"Device Name Type: {config.get_device_name_type()}")
        print(f"Save Device URL: {config.should_save_device_url()}")
        print(f"Device URL File: {config.get_device_url_file()}")
        print(f"Log Level: {config.get_log_level()}")
        
        print("\nConfiguration Validation:")
        print("=" * 50)
        if config.validate_config():
            print("✓ Configuration is valid")
        else:
            print("✗ Configuration has errors")
            
    except Exception as e:
        print(f"Error loading configuration: {e}")
        print("Make sure config/fhir_config.yaml exists and is valid YAML")
