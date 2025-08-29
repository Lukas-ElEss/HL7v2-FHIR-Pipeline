# HL7v2 to FHIR Pipeline

Eine umfassende Pipeline zur Transformation von HL7 v2 Nachrichten in FHIR R4 Ressourcen mit Unterstützung für verschiedene HL7 Segmente und Datentypen.

## Übersicht

Dieses Projekt implementiert eine robuste Pipeline zur Konvertierung von HL7 v2 Nachrichten in FHIR R4 Ressourcen. Es unterstützt eine breite Palette von HL7 Segmenten, Datentypen und Nachrichtentypen und bietet eine flexible Konfiguration für verschiedene FHIR-Server.

## Features

### HL7 v2 Unterstützung
- **Nachrichtentypen**: ADT, MDM, OML, ORM, ORU, SIU, VXU
- **Segmente**: MSH, PID, DG1, OBR, ORC, TQ1, PV1, NK1, NTE, PR1, SCH, SFT, SPM
- **Datentypen**: CE, CF, CNE, CNN, CQ, CWE, CX, DLN, DR, DTM, ED, EI, FN, FT, HD, ID, IS, MSG, NA, NDL, NM, NR, OG, PL, PLN, PT, RI, RP, RPT, SAD, SN, SPS, ST, TQ, TS, XAD, XCN, XON, XPN, XTN

### FHIR R4 Transformation
- **Ressourcen**: Patient, Condition, ServiceRequest, Encounter, Device, Organization, Practitioner, Observation, DiagnosticReport, MedicationRequest, Immunization, DocumentReference, Provenance
- **Strukturdefinitionen**: Custom InfoWashSource für HL7 v2 Mapping
- **Concept Maps**: Umfassende Mapping-Regeln für HL7 v2 zu FHIR R4

### Pipeline-Komponenten
- **HL7 Parser**: Robuste Parsing-Logik für HL7 v2 Nachrichten
- **FHIR Client**: Vollständige FHIR R4 Client-Implementierung
- **Matchbox Integration**: Unterstützung für Matchbox FHIR-Server
- **Device Registration**: Automatische Device-Ressourcen-Registrierung
- **Configuration Management**: Zentrale Konfiguration für verschiedene Umgebungen

## Projektstruktur

```
v2-to-fhir-pipeline/
├── input/
│   ├── StructureDefinition/     # FHIR Strukturdefinitionen
│   ├── StructureMap/            # HL7 zu FHIR Mapping-Regeln
│   ├── FML/                     # FML Mapping-Dateien
│   └── v2-to-fhir-IG/          # FHIR Implementation Guide
├── scripts/                     # Python-Skripte für die Pipeline
├── config/                      # Konfigurationsdateien
├── Archive/                     # Archivierte Ressourcen und Concept Maps
└── #save/                       # Gespeicherte Mapping-Dateien
```

## Installation

### Voraussetzungen
- Python 3.8+
- Java 8+ (für Matchbox Engine)
- Zugriff auf einen FHIR R4 Server

### Abhängigkeiten installieren
```bash
cd scripts
pip install -r requirements.txt
```

## Konfiguration

### FHIR Server konfigurieren
Bearbeiten Sie `config/fhir_config.yaml` mit Ihren Server-Details:

```yaml
linuxforhealth:
  fhir:
    server_url: "http://your-fhir-server:port/fhir"
    credentials:
      username: "your-username"
      password: "your-password"
    timeout: 30

matchbox:
  server_url: "http://localhost:8080/matchboxv3/fhir"
  port: 8080
```

### Umgebungsspezifische Konfiguration
- `config/fhir_config-dev.yaml` - Entwicklungsumgebung
- `config/fhir_config-prod.yaml` - Produktionsumgebung

## Verwendung

### 1. Device registrieren
```bash
cd scripts
python register_device.py "v2-to-fhir-pipeline"
```

### 2. HL7 Nachricht parsen
```python
from hl7_parser import parse_hl7_string
from config import get_config

# HL7 Nachricht parsen
data = parse_hl7_string(hl7_message)

# Device URL setzen
config = get_config()
data.set_device_url("http://server/fhir/Device/device-id")

# Zu JSON konvertieren
json_output = data.to_json()
```

### 3. FHIR Ressourcen erstellen
```python
from fhir_client import FHIRClient

client = FHIRClient()
response = client.create_resource("Patient", patient_data)

if response.success:
    print(f"Patient erstellt: {response.resource_id}")
else:
    print(f"Fehler: {response.error_message}")
```

## Mapping-Regeln

### PID Segment → Patient
- `PID.3` → Patient.identifier
- `PID.5` → Patient.name
- `PID.7` → Patient.birthDate
- `PID.8` → Patient.gender

### DG1 Segment → Condition
- `DG1.3` → Condition.code
- `DG1.4` → Condition.onsetDateTime
- `DG1.6` → Condition.severity

### OBR Segment → ServiceRequest
- `OBR.4` → ServiceRequest.code
- `OBR.7` → ServiceRequest.quantity
- `OBR.16` → ServiceRequest.requester

## FHIR Server

### Unterstützte Server
- **Linux for Health FHIR**: Standard FHIR R4 Server
- **Matchbox**: FHIR R4 Server mit HL7 v2 Unterstützung
- **HAPI FHIR**: Open Source FHIR Server

### Server-Konfiguration
```yaml
# Linux for Health
server_url: "http://10.0.2.2:5555/fhir"

# Matchbox
server_url: "http://localhost:8080/matchboxv3/fhir"
```

## Testing

### FHIR Client testen
```bash
cd scripts
python fhir_client.py
```

### Konfiguration testen
```bash
cd scripts
python config.py
```

### Device-Registrierung testen
```bash
cd scripts
python register_device.py "test-device"
```

## Entwicklung

### Projekt aufsetzen
```bash
# Repository klonen
git clone https://github.com/Lukas-ElEss/HL7v2-FHIR-Pipeline.git
cd HL7v2-FHIR-Pipeline

# Dependencies installieren
pip install -r scripts/requirements.txt

# Konfiguration anpassen
cp config/fhir_config.yaml config/fhir_config-dev.yaml
# Bearbeiten Sie die dev-Konfiguration
```

### Neue Mapping-Regeln hinzufügen
1. Strukturdefinition in `input/StructureDefinition/` erstellen
2. Mapping-Regeln in `input/StructureMap/` definieren
3. Concept Maps in `Archive/ConceptMaps/resources/` hinzufügen
4. Tests schreiben und ausführen

## Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert.

## Beitragen

1. Fork des Repositories
2. Feature-Branch erstellen (`git checkout -b feature/AmazingFeature`)
3. Änderungen committen (`git commit -m 'Add some AmazingFeature'`)
4. Branch pushen (`git push origin feature/AmazingFeature`)
5. Pull Request öffnen

## Support

Bei Fragen oder Problemen:
- GitHub Issues öffnen
- Dokumentation in `scripts/README.md` konsultieren
- FHIR Server-Logs überprüfen

## Changelog

### Version 1.0.0
- Initiale Implementierung der HL7 v2 zu FHIR R4 Pipeline
- Unterstützung für alle gängigen HL7 Segmente
- Umfassende Concept Maps und Mapping-Regeln
- Robuste FHIR Client-Implementierung
- Automatische Device-Registrierung
- Zentrale Konfigurationsverwaltung
