# HL7 Parser and Device Registration Scripts

This directory contains scripts for parsing HL7 messages and registering Device resources on the FHIR server.

## Files

### 1. `hl7_parser.py` - Updated HL7 Parser

The HL7 parser has been updated to match the `InfoWashSource` StructureDefinition and now extracts:

**PID Fields:**
- `PID_3_id` - Patient Identifier
- `PID_5_1_family` - Family Name
- `PID_5_2_given` - Given Name
- `PID_7` - Birth Date
- `PID_8_code` - Administrative Sex
- `PID_3_4_*` - Assigning Authority (HD components)
- `PID_3_6_*` - Assigning Facility (HD components)

**DG1 Fields:**
- `DG1_3_code` - Diagnosis Code
- `DG1_3_text` - Diagnosis Text
- `DG1_3_system` - Diagnosis Coding System

**OBR Fields:**
- `OBR_4_code` - Service Code
- `OBR_4_text` - Service Text
- `OBR_4_system` - Service Coding System

**TQ1 Fields:**
- `TQ1_7` - Start Date/Time
- `TQ1_8` - End Date/Time

**ORC Fields:**
- `ORC_1` - Order Control
- `ORC_5` - Order Status
- `ORC_9` - Order Date/Time

**Additional Fields:**
- `RAW_message` - Full HL7 v2 message for provenance
- `DEVICE_fullUrl` - Device resource URL (set after registration)

### 2. `register_device.py` - Device Registration Script

This script registers a Device resource on the FHIR server using the robust FHIR client.

**Features:**
- **Automatic duplicate detection** - Checks if device already exists
- **Health check** - Verifies FHIR server connectivity before proceeding
- **Robust error handling** - Uses FHIR client with proper error handling
- **Interactive mode** - Asks user if they want to use existing device

**Usage:**
```bash
# With device name as argument
python register_device.py "v2-to-fhir-pipeline"

# Or interactive mode
python register_device.py
```

**What it does:**
1. Checks FHIR server connectivity with health check
2. Searches for existing device with same name
3. Creates new Device resource if none exists
4. Returns the full URL of the Device resource
5. Saves the URL to a file if configured to do so

### 3. `fhir_client.py` - FHIR Client Implementation

Robust FHIR client that provides a clean API for FHIR server communication.

**Features:**
- **LinuxForHealth FHIR Client API** - Based on official documentation
- **Comprehensive error handling** - Connection, timeout, and request errors
- **Authentication support** - Basic auth with configurable credentials
- **Resource operations** - Create, read, update, delete, search
- **Health monitoring** - Server health checks and capability statements
- **Structured responses** - FHIRResponse objects with success/error information

**Available methods:**
- `create_device()` - Create Device resource
- `get_device()` - Retrieve Device by ID
- `search_devices()` - Search Device resources
- `update_device()` - Update Device resource
- `delete_device()` - Delete Device resource
- `health_check()` - Server health check
- `get_capability_statement()` - Server capabilities
- `create_resource()` - Create any FHIR resource
- `get_resource()` - Retrieve any FHIR resource

### 4. `config.py` - Configuration Management

Configuration management class that loads settings from `config/fhir_config.yaml`.

**Features:**
- Loads FHIR server URLs and credentials
- Configurable timeouts and headers
- Device registration settings
- Output configuration
- Configuration validation

### 5. `config/fhir_config.yaml` - Configuration File

Central configuration file containing all FHIR server settings.

**Configuration sections:**
- `linuxforhealth.fhir` - Linux for Health FHIR server settings
- `matchbox` - Matchbox server configuration
- `device` - Device registration settings
- `output` - Output and logging configuration

## Configuration

### FHIR Server Configuration

The scripts use a centralized configuration file at `config/fhir_config.yaml`:

```yaml
linuxforhealth:
  fhir:
    server_url: "http://10.0.2.2:5555/fhir"
    credentials:
      username: "fhiruser"
      password: "change-password"
    timeout: 30
    headers:
      content_type: "application/fhir+json"
      accept: "application/fhir+json"

matchbox:
  server_url: "http://localhost:8080/matchboxv3/fhir"
  context_path: "/matchboxv3"
  port: 8080
```

### Environment-Specific Configuration

You can create environment-specific configuration files:
- `config/fhir_config-dev.yaml` - Development environment
- `config/fhir_config-prod.yaml` - Production environment

## Workflow

1. **Configure the FHIR server:**
   Edit `config/fhir_config.yaml` with your server details.

2. **Register the Device:**
   ```bash
   cd scripts
   python register_device.py "v2-to-fhir-pipeline"
   ```

3. **Use the returned URL in your HL7 parsing:**
   ```python
   from hl7_parser import parse_hl7_string
   from config import get_config
   
   # Parse HL7 message
   data = parse_hl7_string(hl7_message)
   
   # Set the device URL from configuration or registration
   config = get_config()
   data.set_device_url("http://10.0.2.2:5555/fhir/Device/your-device-id")
   
   # Convert to JSON
   json_output = data.to_json()
   ```

## Dependencies

- `hl7` - HL7 message parsing
- `requests` - HTTP requests for FHIR server communication
- `pyyaml` - YAML configuration file parsing
- `typing` - Type hints (Python 3.5+)

## FHIR Server

The scripts are configured to use the FHIR server specified in `config/fhir_config.yaml`.

**Default configuration:**
- Linux for Health: `http://10.0.2.2:5555/fhir`
- Matchbox: `http://localhost:8080/matchboxv3/fhir`

Make sure the FHIR server is running and accessible before using the device registration script.

## Testing

### Test FHIR Client:
```bash
cd scripts
python fhir_client.py
```

### Test Configuration:
```bash
cd scripts
python config.py
```

### Test Device Registration:
```bash
cd scripts
python register_device.py "test-device"
```

## Output Format

The parser now outputs data in the exact structure defined by `InfoWashSource.json`:

```json
{
  "resourceType": "InfoWashSource",
  "PID_3_id": "12345",
  "PID_5_1_family": "SMITH",
  "PID_5_2_given": "JOHN",
  "PID_7": "1980-01-01",
  "PID_8_code": "M",
  "PID_3_4_namespaceId": "HOSPITAL",
  "PID_3_4_universalId": "urn:oid:1.2.3.4.5",
  "PID_3_4_universalIdType": "ISO",
  "PID_3_6_namespaceId": "LAB",
  "PID_3_6_universalId": "urn:oid:1.2.3.4.6",
  "PID_3_6_universalIdType": "ISO",
  "DG1_3_code": "I10",
  "DG1_3_text": "Essential hypertension",
  "DG1_3_system": "http://hl7.org/fhir/sid/icd-10",
  "OBR_4_code": "EKG",
  "OBR_4_text": "Electrocardiogram",
  "OBR_4_system": "http://loinc.org",
  "TQ1_7": "2025-01-01T12:00:00",
  "TQ1_8": "2025-01-01T13:00:00",
  "ORC_1": "NW",
  "ORC_5": "A",
  "ORC_9": "2025-01-01T12:00:00",
  "RAW_message": "MSH|^~\\&|...",
  "DEVICE_fullUrl": "http://10.0.2.2:5555/fhir/Device/your-device-id"
}
```

## Error Handling

The FHIR client provides comprehensive error handling:

- **Connection errors** - Network connectivity issues
- **Timeout errors** - Request timeouts
- **Authentication errors** - Invalid credentials
- **HTTP errors** - Server-side errors with detailed messages
- **Validation errors** - Invalid resource data

All errors are captured in the `FHIRResponse` object with appropriate error messages and status codes.
