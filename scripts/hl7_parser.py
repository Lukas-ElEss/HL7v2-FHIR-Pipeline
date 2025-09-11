import hl7
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import re
import json


@dataclass
class InfoWashSourceData:
    """Data structure exactly matching the InfoWashSource StructureDefinition"""
    
    # Fields exactly as defined in InfoWashSource.json StructureDefinition
    PID_3_1: Optional[str] = None                     # PID-3 CX.1 (ID Number) - string
    PID_5_1_1: Optional[str] = None                   # PID-5 XPN.1 Family - string
    PID_5_2: Optional[str] = None                     # PID-5 XPN.2 Given - string
    PID_7: Optional[str] = None                        # PID-7 DTM - date
    PID_8_1: Optional[str] = None                     # PID-8 Administrative Sex - code
    PID_3_4_1: Optional[str] = None                   # PID-3.4 HD.1 namespaceId - string
    PID_3_4_2: Optional[str] = None                   # PID-3.4 HD.2 universalId - uri
    PID_3_4_3: Optional[str] = None                   # PID-3.4 HD.3 universalIdType - code
    PID_3_6_1: Optional[str] = None                   # PID-3.6 HD.1 namespaceId - string
    PID_3_6_2: Optional[str] = None                   # PID-3.6 HD.2 universalId - uri
    PID_3_6_3: Optional[str] = None                   # PID-3.6 HD.3 universalIdType - code
    DG1_3_1: Optional[str] = None                     # DG1-3 CWE.1 - code
    DG1_3_2: Optional[str] = None                     # DG1-3 CWE.2 - string
    DG1_3_3: Optional[str] = None                     # DG1-3 CWE.3 - uri
    OBR_4_1: Optional[str] = None                     # OBR-4 CWE.1 - code
    OBR_4_2: Optional[str] = None                     # OBR-4 CWE.2 - string
    OBR_4_3: Optional[str] = None                     # OBR-4 CWE.3 - uri
    TQ1_7: Optional[str] = None                       # TQ1-7 DTM - dateTime
    TQ1_8: Optional[str] = None                       # TQ1-8 DTM - dateTime
    ORC_1: Optional[str] = None                       # ORC-1 Order Control - code
    ORC_5: Optional[str] = None                       # ORC-5 Order Status - code
    ORC_9: Optional[str] = None                       # ORC-9 DTM - dateTime
    
    # PV1 segment fields
    PV1_2_1: Optional[str] = None                     # PV1-2.1 Patient Class - string
    PV1_2_2: Optional[str] = None                     # PV1-2.2 Patient Class - string
    PV1_2_3: Optional[str] = None                     # PV1-2.3 Patient Class - string
    PV1_2_7: Optional[str] = None                     # PV1-2.7 Patient Class - string
    PV1_4_1: Optional[str] = None                     # PV1-4.1 Assigned Patient Location - string
    PV1_4_2: Optional[str] = None                     # PV1-4.2 Assigned Patient Location - string
    PV1_4_3: Optional[str] = None                     # PV1-4.3 Assigned Patient Location - string
    PV1_4_7: Optional[str] = None                     # PV1-4.7 Assigned Patient Location - string
    PV1_4_9: Optional[str] = None                     # PV1-4.9 Assigned Patient Location - string
    PV1_10_1: Optional[str] = None                    # PV1-10.1 Admitting Doctor - string
    PV1_10_2: Optional[str] = None                    # PV1-10.2 Admitting Doctor - string
    PV1_10_3: Optional[str] = None                    # PV1-10.3 Admitting Doctor - string
    PV1_10_7: Optional[str] = None                    # PV1-10.7 Admitting Doctor - string
    PV1_10_9: Optional[str] = None                    # PV1-10.9 Admitting Doctor - string
    PV1_19: Optional[str] = None                      # PV1-19 Visit Number - string
    PV1_19_1: Optional[str] = None                    # PV1-19.1 Visit Number - string
    PV1_19_4_1: Optional[str] = None                  # PV1-19.4.1 Visit Number - string
    PV1_19_5: Optional[str] = None                    # PV1-19.5 Visit Number - string
    PV1_44: Optional[str] = None                      # PV1-44 Admit Date/Time - string
    PV1_45: Optional[str] = None                      # PV1-45 Discharge Date/Time - string
    
    RAW_message: Optional[str] = None                  # RAW HL7 v2 message - string
    DEVICE_id: Optional[str] = None                   # Device id - uri
    
    def to_fhir_source_format(self) -> Dict[str, Any]:
        """
        Convert the parsed data to FHIR source format exactly matching the StructureDefinition
        
        Returns:
            dict: FHIR source format dictionary with flat structure
        """
        fhir_data = {
            "resourceType": "InfoWashSource",
            
            # PID segment - flat structure
            "PIDSegment": {
                "PID_3_1": self.PID_3_1,
                "PID_3_4_1": self.PID_3_4_1,
                "PID_5_1_1": self.PID_5_1_1,
                "PID_5_2": self.PID_5_2,
                "PID_7": self.PID_7,
                "PID_8_1": self.PID_8_1
            },
            
            # DG1 segment - flat structure
            "DG1Segment": {
                "DG1_3_1": self.DG1_3_1,
                "DG1_3_2": self.DG1_3_2,
                "DG1_3_3": self.DG1_3_3
            },
            
            # OBR segment - flat structure
            "OBRSegment": {
                "OBR_4_1": self.OBR_4_1,
                "OBR_4_2": self.OBR_4_2,
                "OBR_4_3": self.OBR_4_3
            },
            
            # TQ1 segment - flat structure
            "TQ1Segment": {
                "TQ1_7": self.TQ1_7,
                "TQ1_8": self.TQ1_8
            },
            
            # ORC segment - flat structure
            "ORCSegment": {
                "ORC_1": self.ORC_1,
                "ORC_5": self.ORC_5,
                "ORC_9": self.ORC_9
            },
            
            # PV1 segment - flat structure
            "PV1Segment": {
                "PV1_2_1": self.PV1_2_1,
                "PV1_2_2": self.PV1_2_2,
                "PV1_2_3": self.PV1_2_3,
                "PV1_2_7": self.PV1_2_7,
                "PV1_4_1": self.PV1_4_1,
                "PV1_4_2": self.PV1_4_2,
                "PV1_4_3": self.PV1_4_3,
                "PV1_4_7": self.PV1_4_7,
                "PV1_4_9": self.PV1_4_9,
                "PV1_10_1": self.PV1_10_1,
                "PV1_10_2": self.PV1_10_2,
                "PV1_10_3": self.PV1_10_3,
                "PV1_10_7": self.PV1_10_7,
                "PV1_10_9": self.PV1_10_9,
                "PV1_19": self.PV1_19,
                "PV1_19_1": self.PV1_19_1,
                "PV1_19_4_1": self.PV1_19_4_1,
                "PV1_19_5": self.PV1_19_5,
                "PV1_44": self.PV1_44,
                "PV1_45": self.PV1_45
            },
            
            # CTX segment - flat structure (matching InfoWashSource.json format)
            "CTXSegment": {
                "CTX_DEVICE_id": self.DEVICE_id,
                "CTX_RAW_message": self.RAW_message,
                "CTX_role": "source"
            }
        }
        
        return fhir_data


class HL7Parser:
    """Parser for HL7 messages to extract InfoWashSource relevant fields"""
    
    def __init__(self):
        self.message = None
    
    def parse_message(self, hl7_text: str) -> InfoWashSourceData:
        """
        Parse HL7 message and extract relevant fields
        
        Args:
            hl7_text: Raw HL7 message as string
            
        Returns:
            InfoWashSourceData object with extracted fields
        """
        try:
            # Parse HL7 message
            self.message = hl7.parse(hl7_text)
            
            # Initialize data structure
            data = InfoWashSourceData()
            
            # Set the raw message for provenance
            data.RAW_message = hl7_text
            
            # Extract segment data
            self._extract_pid_data(data)
            self._extract_dg1_data(data)
            self._extract_obr_data(data)
            self._extract_tq1_data(data)
            self._extract_orc_data(data)
            self._extract_pv1_data(data)
            self._extract_device_data(data)
            
            return data
            
        except Exception as e:
            raise ValueError(f"Failed to parse HL7 message: {str(e)}")
    
    def load_device_url_from_file(self, device_url_file: str = "device_url.txt") -> str:
        """Load DEVICE_id from file"""
        try:
            import os
            from pathlib import Path
            
            script_dir = Path(__file__).parent
            file_path = script_dir / device_url_file
            
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    device_url = f.read()
                
                device_url = device_url.strip().replace('\n', '').replace('\r', '').strip()
                
                if device_url:
                    print(f"✅ Device URL loaded: {device_url}")
                    return device_url
                else:
                    print("⚠️ Device URL file is empty")
                    return None
            else:
                print(f"⚠️ Device URL file not found at: {file_path}")
                return None
                
        except Exception as e:
            print(f"⚠️ Error loading device URL: {e}")
            return None
    
    def _extract_pid_data(self, data: InfoWashSourceData):
        """Extract PID segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'PID':
                    # PID-3: Patient Identifier (CX) - already parsed by python-hl7
                    if len(segment) > 3 and segment[3] and len(segment[3]) > 0:
                        cx_field = segment[3][0]  # Get the first repetition
                        if len(cx_field) > 0 and cx_field[0]:
                            data.PID_3_1 = cx_field[0][0] if cx_field[0] else None  # ID
                        if len(cx_field) > 3 and cx_field[3]:
                            data.PID_3_4_1 = cx_field[3][0] if cx_field[3] else None  # Assigning Authority
                            data.PID_3_4_2 = cx_field[3][0] if cx_field[3] else None  # Using assigningAuthority as universalId
                            data.PID_3_4_3 = 'ISO'  # Default universalIdType
                        if len(cx_field) > 5 and cx_field[5]:
                            data.PID_3_6_1 = cx_field[5][0] if cx_field[5] else None  # Assigning Facility
                            data.PID_3_6_2 = cx_field[5][0] if cx_field[5] else None  # Using assigningFacility as universalId
                            data.PID_3_6_3 = 'ISO'  # Default universalIdType
                    
                    # PID-5: Patient Name (XCN) - already parsed by python-hl7
                    if len(segment) > 5 and segment[5] and len(segment[5]) > 0:
                        xcn_field = segment[5][0]  # Get the first repetition
                        if len(xcn_field) > 1 and xcn_field[1]:
                            data.PID_5_1_1 = xcn_field[1][0] if xcn_field[1] else None  # Family Name
                        if len(xcn_field) > 2 and xcn_field[2]:
                            data.PID_5_2 = xcn_field[2][0] if xcn_field[2] else None  # Given Name
                    
                    # PID-7: Birth Date
                    if len(segment) > 7 and segment[7]:
                        birth_value = segment[7][0] if segment[7] else None
                        data.PID_7 = self._format_date(birth_value) if birth_value else None
                    
                    # PID-8: Gender
                    if len(segment) > 8 and segment[8]:
                        data.PID_8_1 = segment[8][0] if segment[8] else None
                    break
        except Exception as e:
            print(f"Warning: Error extracting PID data: {e}")
    
    def _extract_dg1_data(self, data: InfoWashSourceData):
        """Extract DG1 segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'DG1':
                    # DG1-3: Diagnosis (CWE) - already parsed by python-hl7
                    if len(segment) > 3 and segment[3] and len(segment[3]) > 0:
                        cwe_field = segment[3][0]  # Get the first repetition
                        if len(cwe_field) > 0 and cwe_field[0]:
                            data.DG1_3_1 = cwe_field[0][0] if cwe_field[0] else None  # Identifier
                        if len(cwe_field) > 1 and cwe_field[1]:
                            data.DG1_3_2 = cwe_field[1][0] if cwe_field[1] else None  # Text
                        if len(cwe_field) > 2 and cwe_field[2]:
                            data.DG1_3_3 = cwe_field[2][0] if cwe_field[2] else None  # Coding System
                    break
        except Exception as e:
            print(f"Warning: Error extracting DG1 data: {e}")
    
    def _extract_obr_data(self, data: InfoWashSourceData):
        """Extract OBR segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'OBR':
                    # OBR-4: Universal Service Identifier (CWE) - already parsed by python-hl7
                    if len(segment) > 4 and segment[4] and len(segment[4]) > 0:
                        cwe_field = segment[4][0]  # Get the first repetition
                        if len(cwe_field) > 0 and cwe_field[0]:
                            data.OBR_4_1 = cwe_field[0][0] if cwe_field[0] else None  # Identifier
                        if len(cwe_field) > 1 and cwe_field[1]:
                            data.OBR_4_2 = cwe_field[1][0] if cwe_field[1] else None  # Text
                        if len(cwe_field) > 2 and cwe_field[2]:
                            data.OBR_4_3 = cwe_field[2][0] if cwe_field[2] else None  # Coding System
                    break
        except Exception as e:
            print(f"Warning: Error extracting OBR data: {e}")
    
    def _extract_tq1_data(self, data: InfoWashSourceData):
        """Extract TQ1 segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'TQ1':
                    # TQ1-5: Start Date/Time (TQ1_7)
                    if len(segment) > 5 and segment[5]:
                        start_value = str(segment[5])
                        data.TQ1_7 = self._format_datetime(start_value)
                    
                    # TQ1-6: End Date/Time (TQ1_8)
                    if len(segment) > 6 and segment[6]:
                        end_value = str(segment[6])
                        data.TQ1_8 = self._format_datetime(end_value)
                    
                    break
        except Exception as e:
            print(f"Warning: Error extracting TQ1 data: {e}")
    
    def _extract_orc_data(self, data: InfoWashSourceData):
        """Extract ORC segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'ORC':
                    # ORC-1: Order Control
                    if len(segment) > 1 and segment[1]:
                        control_value = str(segment[1])
                        data.ORC_1 = control_value if control_value else None
                    
                    # ORC-5: Order Status
                    if len(segment) > 5 and segment[5]:
                        status_value = str(segment[5])
                        data.ORC_5 = status_value if status_value else None
                    
                    # ORC-9: Date/Time of Transaction
                    if len(segment) > 9 and segment[9]:
                        time_value = str(segment[9])
                        data.ORC_9 = self._format_datetime(time_value)
                    
                    break
        except Exception as e:
            print(f"Warning: Error extracting ORC data: {e}")
    
    def _extract_pv1_data(self, data: InfoWashSourceData):
        """Extract PV1 segment fields"""
        try:
            for segment in self.message:
                if segment[0][0] == 'PV1':
                    # PV1-2: Patient Class (CWE) - already parsed by python-hl7
                    if len(segment) > 2 and segment[2] and len(segment[2]) > 0:
                        cwe_field = segment[2][0]  # Get the first repetition
                        if len(cwe_field) > 0 and cwe_field[0]:
                            data.PV1_2_1 = cwe_field[0][0] if cwe_field[0] else None  # Identifier
                        if len(cwe_field) > 1 and cwe_field[1]:
                            data.PV1_2_3 = cwe_field[1][0] if cwe_field[1] else None  # Text
                        if len(cwe_field) > 2 and cwe_field[2]:
                            data.PV1_2_2 = cwe_field[2][0] if cwe_field[2] else None  # Coding System
                        if len(cwe_field) > 3 and cwe_field[3]:
                            data.PV1_2_7 = cwe_field[3][0] if cwe_field[3] else None  # Version
                    
                    # PV1-4: Admission Type (CWE) - already parsed by python-hl7
                    if len(segment) > 4 and segment[4] and len(segment[4]) > 0:
                        cwe_field = segment[4][0]  # Get the first repetition
                        if len(cwe_field) > 0 and cwe_field[0]:
                            data.PV1_4_1 = cwe_field[0][0] if cwe_field[0] else None  # Identifier
                        if len(cwe_field) > 1 and cwe_field[1]:
                            data.PV1_4_2 = cwe_field[1][0] if cwe_field[1] else None  # Text
                        if len(cwe_field) > 2 and cwe_field[2]:
                            data.PV1_4_3 = cwe_field[2][0] if cwe_field[2] else None  # Coding System
                        if len(cwe_field) > 3 and cwe_field[3]:
                            data.PV1_4_7 = cwe_field[3][0] if cwe_field[3] else None  # Version
                        if len(cwe_field) > 4 and cwe_field[4]:
                            data.PV1_4_9 = cwe_field[4][0] if cwe_field[4] else None  # Original Text
                    
                    # PV1-10: Hospital Service (CWE) - already parsed by python-hl7
                    if len(segment) > 10 and segment[10] and len(segment[10]) > 0:
                        cwe_field = segment[10][0]  # Get the first repetition
                        if len(cwe_field) > 0 and cwe_field[0]:
                            data.PV1_10_1 = cwe_field[0][0] if cwe_field[0] else None  # Identifier
                        if len(cwe_field) > 1 and cwe_field[1]:
                            data.PV1_10_2 = cwe_field[1][0] if cwe_field[1] else None  # Text
                        if len(cwe_field) > 2 and cwe_field[2]:
                            data.PV1_10_3 = cwe_field[2][0] if cwe_field[2] else None  # Coding System
                        if len(cwe_field) > 3 and cwe_field[3]:
                            data.PV1_10_7 = cwe_field[3][0] if cwe_field[3] else None  # Version
                        if len(cwe_field) > 4 and cwe_field[4]:
                            data.PV1_10_9 = cwe_field[4][0] if cwe_field[4] else None  # Original Text
                    
                    # PV1-19: Visit Number (CX) - already parsed by python-hl7
                    if len(segment) > 19 and segment[19] and len(segment[19]) > 0:
                        cx_field = segment[19][0]  # Get the first repetition
                        if len(cx_field) > 0 and cx_field[0]:
                            data.PV1_19 = cx_field[0][0] if cx_field[0] else None  # ID
                            data.PV1_19_1 = cx_field[0][0] if cx_field[0] else None  # ID (same as above)
                        if len(cx_field) > 3 and cx_field[3]:
                            data.PV1_19_4_1 = cx_field[3][0] if cx_field[3] else None  # Assigning Authority
                        if len(cx_field) > 4 and cx_field[4]:
                            data.PV1_19_5 = cx_field[4][0] if cx_field[4] else None  # ID Type
                    
                    # PV1-44: Admit Date/Time
                    if len(segment) > 44 and segment[44]:
                        data.PV1_44 = self._format_datetime(segment[44][0]) if segment[44] else None
                    
                    # PV1-45: Discharge Date/Time
                    if len(segment) > 45 and segment[45]:
                        data.PV1_45 = self._format_datetime(segment[45][0]) if segment[45] else None
                    
                    break
        except Exception as e:
            print(f"Warning: Error extracting PV1 data: {e}")
    
    def _extract_device_data(self, data: InfoWashSourceData):
        """Extract device information and set DEVICE_id"""
        try:
            # Load device URL from file
            device_url = self.load_device_url_from_file()
            if device_url:
                data.DEVICE_id = device_url
            else:
                # Fallback to default device URL
                data.DEVICE_id = "http://localhost:8080/matchboxv3/fhir/Device/v2-to-fhir-pipeline"
        except Exception as e:
            print(f"Warning: Error setting device URL: {e}")
            # Fallback to default device URL
            data.DEVICE_id = "http://localhost:8080/matchboxv3/fhir/Device/v2-to-fhir-pipeline"
    
    def _format_date(self, date_str: str) -> Optional[str]:
        """Format HL7 date to FHIR date format (YYYY-MM-DD)"""
        if not date_str or date_str == '':
            return None
        
        # If already in ISO format, return as is
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # Handle HL7 format (YYYYMMDD)
        date_str = re.sub(r'[^0-9]', '', date_str)
        
        if len(date_str) >= 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        elif len(date_str) >= 6:
            year = "20" + date_str[:2] if int(date_str[:2]) < 50 else "19" + date_str[:2]
            return f"{year}-{date_str[2:4]}-{date_str[4:6]}"
        
        return None
    
    def _format_datetime(self, datetime_str: str) -> Optional[str]:
        """Format HL7 datetime to FHIR datetime format (YYYY-MM-DDTHH:MM:SS)"""
        if not datetime_str or datetime_str == '':
            return None
        
        # If already in ISO format without timezone, add timezone
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', datetime_str):
            return datetime_str + '+00:00'
        
        # Handle HL7 format (YYYYMMDDHHMMSS or YYYYMMDDHHMM)
        datetime_str = re.sub(r'[^0-9]', '', datetime_str)
        
        if len(datetime_str) >= 14:
            # YYYYMMDDHHMMSS -> YYYY-MM-DDTHH:MM:SS+00:00
            year = datetime_str[0:4]
            month = datetime_str[4:6]
            day = datetime_str[6:8]
            hour = datetime_str[8:10]
            minute = datetime_str[10:12]
            second = datetime_str[12:14]
            return f"{year}-{month}-{day}T{hour}:{minute}:{second}+00:00"
        elif len(datetime_str) >= 12:
            # YYYYMMDDHHMM -> YYYY-MM-DDTHH:MM:00+00:00
            year = datetime_str[0:4]
            month = datetime_str[4:6]
            day = datetime_str[6:8]
            hour = datetime_str[8:10]
            minute = datetime_str[10:12]
            return f"{year}-{month}-{day}T{hour}:{minute}:00+00:00"
        elif len(datetime_str) >= 8:
            # YYYYMMDD -> YYYY-MM-DDTHH:MM:00+00:00
            year = datetime_str[0:4]
            month = datetime_str[4:6]
            day = datetime_str[6:8]
            return f"{year}-{month}-{day}T00:00:00+00:00"
        
        return None
    
 
 

def parse_hl7_file(file_path: str) -> InfoWashSourceData:
    """Parse HL7 message from file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        hl7_text = f.read().strip()
    
    parser = HL7Parser()
    return parser.parse_message(hl7_text)


def parse_hl7_string(hl7_text: str) -> InfoWashSourceData:
    """Parse HL7 message from string"""
    parser = HL7Parser()
    return parser.parse_message(hl7_text)

