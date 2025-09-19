# HL7 v2 to FHIR R4 Pipeline

Eine vollständige Pipeline zur Transformation von HL7 v2 OMG_O19 Nachrichten in FHIR R4 Ressourcen mit automatischer Bundle-Erstellung, Deduplication und InfoWash-Integration.

## Übersicht

Diese Pipeline implementiert eine komplette Transformation von HL7 v2 OMG_O19 Nachrichten über eine spezialisierte InfoWashSource-Struktur zu FHIR R4 Transaction Bundles. Sie unterstützt kontinuierliche Verarbeitung, automatische Deduplication und spezialisierte FHIR-Suchen für InfoWash.

## Architektur

```
HL7 v2 OMG_O19 → InfoWashSource → FHIR Bundle → Deduplication → FHIR Server
     ↓              ↓               ↓             ↓              ↓
  Parser      Structure Map    Transaction    Duplicate      Linux for Health
             Transformation      Bundle        Detection         FHIR Server
```

## Hauptkomponenten

### 1. **HL7 Parser** (`hl7_parser.py`)
- **InfoWashSourceData**: Dataclass mit allen HL7 v2 Segmentfeldern
- **HL7Parser**: Extrahiert PID, DG1, OBR, TQ1, ORC, PV1 Segmente
- **Unterstützte Felder**: 50+ spezifische HL7 v2 Felder nach InfoWashSource-Struktur

### 2. **FHIR Mapper** (`fhir_mapper.py`)
- **FHIRMapper**: Orchestriert komplette Transformation
- **Matchbox Integration**: Verwendet Matchbox für Structure Map Transformation
- **Pipeline**: HL7 → InfoWashSource → FHIR Bundle → Deduplication → Server

### 3. **FHIR Client** (`fhir_client.py`)
- **FHIRClient**: Linux for Health FHIR Server Integration
- **FHIRResponse**: Strukturierte Response-Objekte
- **Transaction Support**: Bundle-Übertragung mit korrekten Endpoints

### 4. **Deduplication Engine** (`fhir_deduplicator.py`)
- **FHIRDeduplicationClient**: Intelligente Duplikat-Erkennung
- **Provenance-basiert**: Sucht nach existierenden Provenance-Ressourcen
- **Resource-Merging**: Kombiniert Duplikate intelligent

### 5. **Pipeline Server** (`pipeline_server.py`)
- **PipelineServer**: Kontinuierliche HL7-Nachrichten-Verarbeitung
- **MLLP/TCP Server**: Port 2100 für Orchestra-Integration
- **Async Processing**: Asynchrone Nachrichtenverarbeitung
- **Graceful Shutdown**: Signal-Handling für sauberes Herunterfahren

### 6. **Matchbox Client** (`matchbox_client.py`)
- **MatchboxClient**: Integration mit Matchbox FHIR Server
- **File Upload**: Upload von StructureDefinitions, StructureMaps, IG
- **Transformation**: FHIR Structure Map Ausführung

### 7. **HL7 Simulator** (`hl7_simulator.py`)
- **OMG_O19_Generator**: Generiert realistische HL7 v2 Test-Nachrichten
- **Dummy Data**: Konfigurierbare Test-Daten

### 8. **Test & Validation Tools**
- **testapp.py**: Komplette Pipeline-Tests mit MLLP-Integration
- **check_bundles.py**: Bundle-Validierung und Server-Überprüfung
- **infowash_search.py**: Spezialisierte FHIR-Suche für InfoWash
- **delete_all_resources.py**: Server-Bereinigung

### 9. **Device Management** (`register_device.py`)
- **Dual Registration**: Registriert Device auf Linux for Health und Matchbox
- **Common Identifier**: Gleiche Device-ID auf beiden Servern
- **Health Checks**: Verbindungstests vor Registrierung

### 10. **File Management** (`upload_files.py`)
- **Batch Upload**: Upload aller Projekt-Dateien zu Matchbox
- **Progress Tracking**: Upload-Fortschritt und Fehlerbehandlung

## Projektstruktur

```
v2-to-fhir-pipeline/
├── scripts/                     # Python-Pipeline-Skripte
│   ├── fhir_mapper.py          # Hauptpipeline-Orchestrator
│   ├── hl7_parser.py           # HL7 v2 Parser mit InfoWashSourceData
│   ├── fhir_client.py          # FHIR Server Client
│   ├── fhir_deduplicator.py    # Deduplication Engine
│   ├── pipeline_server.py      # MLLP/TCP Server (Port 2100)
│   ├── matchbox_client.py      # Matchbox Server Integration
│   ├── hl7_simulator.py        # OMG_O19 Message Generator
│   ├── testapp.py              # Pipeline Test mit MLLP
│   ├── check_bundles.py        # Bundle-Validierung
│   ├── infowash_search.py      # InfoWash FHIR-Suche
│   ├── register_device.py      # Dual Device Registration
│   ├── upload_files.py         # File Upload Utility
│   ├── delete_all_resources.py # Server-Bereinigung
│   └── config.py               # Konfigurationsmanagement
├── input/
│   ├── StructureMap/
│   │   └── InfoWashSource-to-Bundle.map  # FHIR Structure Map
│   ├── StructureDefiniton/source/
│   │   └── InfoWashSource.json          # InfoWashSource Strukturdefinition
│   └── v2-to-fhir-IG/                   # FHIR Implementation Guide
│       ├── full-ig.zip
│       └── package.r4.tgz
└── config/
    ├── fhir_config.yaml        # FHIR Server Konfiguration
    └── application.yaml        # Matchbox Server Konfiguration

```

## Installation

### Voraussetzungen
- Python 3.8+
- Java 8+ (für Matchbox Engine)
- Linux for Health FHIR Server (Port 9443)
- Matchbox FHIR Server (Port 8080)

### Setup
```bash
# Repository klonen
git clone <repository-url>
cd v2-to-fhir-pipeline

# Dependencies installieren
cd scripts
pip install -r requirements.txt

# Device auf beiden Servern registrieren
python register_device.py "v2-to-fhir-pipeline"

# Dateien zu Matchbox hochladen
python upload_files.py
```

## Konfiguration

### FHIR Server (`config/fhir_config.yaml`)
```yaml
linuxforhealth:
  fhir:
    server_url: "https://localhost:9443/fhir-server/api/v4"
    credentials:
      username: "fhiruser"
      password: "change-password"
    ssl_verify: false
    timeout: 30

matchbox:
  server_url: "http://localhost:8080/matchboxv3/fhir"
  port: 8080

device:
  default_name: "v2-to-fhir-pipeline"
  resource_type: "Device"
  name_type: "model-name"

output:
  save_device_url: true
  device_url_file: "device_url.txt"
  log_level: "DEBUG"
```

## Verwendung

### 1. Vollständige Pipeline testen
```bash
cd scripts
python testapp.py
```

### 2. Kontinuierlicher MLLP-Server
```bash
cd scripts
python pipeline_server.py
# Server läuft auf Port 2100
```

### 3. Bundle-Validierung
```bash
cd scripts
python check_bundles.py
```

### 4. InfoWash-Suche
```bash
cd scripts
python infowash_search.py
```

### 5. HL7-Nachrichten generieren
```bash
cd scripts
python hl7_simulator.py
```

### 6. Programmatische Verwendung
```python
from fhir_mapper import FHIRMapper

mapper = FHIRMapper()
result = mapper.complete_transformation_pipeline(hl7_message)

if result["success"]:
    print(f"Bundle ID: {result['bundle_id']}")
    print(f"Resources: {result['resource_count']}")
```


## Transformation Pipeline

### 1. HL7 v2 Parsing
- **Input**: Raw HL7 v2 OMG_O19 Nachricht
- **Output**: InfoWashSource JSON-Struktur
- **Parser**: `hl7_parser.py` mit `InfoWashSourceData`

### 2. FHIR Transformation
- **Input**: InfoWashSource JSON
- **Output**: FHIR Transaction Bundle
- **Mapper**: `fhir_mapper.py` mit Matchbox Structure Map

### 3. Deduplication
- **Input**: FHIR Bundle
- **Output**: Dedupliziertes Bundle
- **Engine**: `fhir_deduplicator.py` mit Provenance-basierter Suche

### 4. Server-Integration
- **Input**: FHIR Bundle
- **Output**: Server-Response mit Ressourcen-IDs
- **Client**: `fhir_client.py` mit Transaction-Support

## InfoWashSource Struktur

Die Pipeline verwendet eine spezialisierte InfoWashSource-Struktur mit 50+ HL7 v2 Feldern:

### PID-Segment Felder
- `PID_3_1`: Patient Identifier
- `PID_5_1_1`: Family Name
- `PID_5_2`: Given Name
- `PID_7`: Birth Date
- `PID_8_1`: Administrative Sex
- `PID_3_4_*`: Assigning Authority (HD components)
- `PID_3_6_*`: Assigning Facility (HD components)

### DG1-Segment Felder
- `DG1_3_1`: Diagnosis Code
- `DG1_3_2`: Diagnosis Text
- `DG1_3_3`: Diagnosis Coding System

### OBR-Segment Felder
- `OBR_4_1`: Service Code
- `OBR_4_2`: Service Text
- `OBR_4_3`: Service Coding System

### TQ1-Segment Felder
- `TQ1_7`: Start Date/Time
- `TQ1_8`: End Date/Time

### ORC-Segment Felder
- `ORC_1`: Order Control
- `ORC_5`: Order Status
- `ORC_9`: Order Date/Time

### PV1-Segment Felder
- `PV1_2_*`: Patient Class
- `PV1_4_*`: Assigned Patient Location
- `PV1_10_*`: Admitting Doctor

## Mapping-Regeln

### InfoWashSource → FHIR Bundle
- **Patient**: Aus PID-Segment mit Identifikatoren
- **Condition**: Aus DG1-Segment mit Diagnose-Codes
- **ServiceRequest**: Aus OBR-Segment mit TQ1-Timing
- **Encounter**: Aus PV1-Segment mit Aufnahme-Informationen
- **Provenance**: Vollständige Nachverfolgung mit Original-HL7

### Timing-Mapping
- **TQ1.7/TQ1.8** → ServiceRequest.occurrencePeriod
- **ORC.9** → ServiceRequest.authoredOn
- **PV1.44/PV1.45** → Encounter.period

## Testing

```bash
# Vollständige Pipeline mit MLLP
python testapp.py

# Bundle-Validierung
python check_bundles.py

# InfoWash-Suche
python infowash_search.py

# HL7-Simulator
python hl7_simulator.py

# Server bereinigen
python delete_all_resources.py
```

## FHIR Server

### Unterstützte Server
- **Linux for Health FHIR**: `https://localhost:9443/fhir-server/api/v4`
- **Matchbox**: `http://localhost:8080/matchboxv3/fhir`

### Authentifizierung
- **Username**: `fhiruser`
- **Password**: `change-password`
- **SSL**: Deaktiviert für localhost

### MLLP Server
- **Port**: 2100
- **Protokoll**: MLLP/TCP
