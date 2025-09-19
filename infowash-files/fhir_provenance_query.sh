#!/bin/bash

# FHIR Provenance Query Script
# Abfrage von Provenance-Ressourcen vom LinuxForHealth FHIR-Server

# Konfiguration
FHIR_BASE_URL="https://localhost:9443/fhir-server/api/v4"
USERNAME="fhiruser"
PASSWORD="change-password"

# Standard-Parameter
AGENT="Device/v2-to-fhir-pipeline"
OCCURRENCE_DATE="2025-09-24"
COUNT="200"

# Kommandozeilen-Argumente
if [ $# -ge 1 ]; then
    OCCURRENCE_DATE="$1"
fi

if [ $# -ge 2 ]; then
    AGENT="$2"
fi

echo "FHIR Provenance Query Script"
echo "============================"
echo "URL: $FHIR_BASE_URL/Provenance"
echo "Agent: $AGENT"
echo "Datum: $OCCURRENCE_DATE"
echo "Count: $COUNT"
echo "----------------------------"

# Curl-Befehl ausführen
curl -k -u "$USERNAME:$PASSWORD" \
     -H "Content-Type: application/fhir+json" \
     -H "Accept: application/fhir+json" \
     "$FHIR_BASE_URL/Provenance?agent=$AGENT&target:ServiceRequest.occurrence=$OCCURRENCE_DATE&_include=Provenance:target&_count=$COUNT" \
     -w "\n\nHTTP Status: %{http_code}\nTotal Time: %{time_total}s\n"

echo "============================"
echo "Abfrage abgeschlossen!"
