#!/usr/bin/env python3
"""
FHIR Provenance Query Script
Abfrage von Provenance-Ressourcen vom LinuxForHealth FHIR-Server
"""

import requests
from requests.auth import HTTPBasicAuth
import json
import sys
from datetime import datetime
from collections import defaultdict

# Konfiguration
FHIR_BASE_URL = "https://localhost:9443/fhir-server/api/v4"
USERNAME = "fhiruser"
PASSWORD = "change-password"

def extract_targets(bundle_data):
    """
    Extrahiert und analysiert die Target-Ressourcen aus dem FHIR Bundle
    
    Args:
        bundle_data (dict): FHIR Bundle Response
    """
    entries = bundle_data.get('entry', [])
    
    # Sammle alle Ressourcen nach Typ
    resources_by_type = defaultdict(list)
    provenance_targets = []
    
    for entry in entries:
        resource = entry.get('resource', {})
        resource_type = resource.get('resourceType', 'Unknown')
        resource_id = resource.get('id', 'N/A')
        
        if resource_type == 'Provenance':
            # Extrahiere Targets aus Provenance
            targets = resource.get('target', [])
            for target in targets:
                target_ref = target.get('reference', 'N/A')
                provenance_targets.append({
                    'provenance_id': resource_id,
                    'target_reference': target_ref
                })
        else:
            # Andere Ressourcen (wahrscheinlich die Targets)
            resources_by_type[resource_type].append({
                'id': resource_id,
                'resource': resource
            })
    
    print(f"Gefundene Ressourcentypen: {list(resources_by_type.keys())}")
    print(f"Anzahl Provenance-Targets: {len(provenance_targets)}")
    
    # Zeige Provenance-Target-Mappings
    print("\nProvenance -> Target Mappings:")
    print("-" * 40)
    for mapping in provenance_targets:
        print(f"Provenance {mapping['provenance_id']} -> {mapping['target_reference']}")
    
    # Zeige Details der Target-Ressourcen
    print("\nTarget-Ressourcen Details:")
    print("-" * 40)
    for resource_type, resources in resources_by_type.items():
        print(f"\n{resource_type} ({len(resources)} gefunden):")
        for resource_info in resources:
            resource = resource_info['resource']
            resource_id = resource_info['id']
            
            # Vollständige JSON-Ausgabe für jede Ressource
            print(f"  - ID: {resource_id}")
            print(f"    Type: {resource_type}")
            print(f"    Vollständige Ressource:")
            print(json.dumps(resource, indent=6, ensure_ascii=False))
            print("    " + "-" * 60)
    
    return provenance_targets, resources_by_type

def query_provenance(agent=None, occurrence_date="2025-09-24", count=200):
    """
    Führt eine Provenance-Abfrage am FHIR-Server durch
    
    Args:
        agent (str): Der Agent für die Suche (optional)
        occurrence_date (str): Datum im Format YYYY-MM-DD
        count (int): Maximale Anzahl Ergebnisse
    
    Returns:
        dict: FHIR Bundle Response oder None bei Fehler
    """
    
    # Suchparameter
    search_params = {
        'target:ServiceRequest.occurrence': occurrence_date,
        '_include': 'Provenance:target',
        '_count': str(count)
    }
    
    # Agent nur hinzufügen wenn angegeben
    if agent:
        search_params['agent'] = agent
    
    # Headers
    headers = {
        'Content-Type': 'application/fhir+json',
        'Accept': 'application/fhir+json'
    }
    
    try:
        print(f"Führe FHIR-Abfrage durch...")
        print(f"URL: {FHIR_BASE_URL}/Provenance")
        if agent:
            print(f"Agent: {agent}")
        else:
            print("Agent: (nicht angegeben)")
        print(f"Datum: {occurrence_date}")
        print(f"Count: {count}")
        print("-" * 50)
        
        # Request ausführen
        response = requests.get(
            f"{FHIR_BASE_URL}/Provenance",
            params=search_params,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            verify=False,  # SSL-Verifikation deaktiviert für self-signed certificate
            headers=headers,
            timeout=30
        )
        
        print(f"HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            provenances = data.get('entry', [])
            total = data.get('total', 0)
            
            print(f"Erfolgreich! Gefunden: {len(provenances)} Provenances (total: {total})")
            
            if provenances:
                print("\nProvenance-Ressourcen:")
                for i, entry in enumerate(provenances, 1):
                    resource = entry.get('resource', {})
                    resource_id = resource.get('id', 'N/A')
                    recorded = resource.get('recorded', 'N/A')
                    print(f"  {i}. ID: {resource_id}, Recorded: {recorded}")
                
                # Targets extrahieren
                print("\n" + "="*50)
                print("TARGET-EXTRAKTION:")
                print("="*50)
                extract_targets(data)
            else:
                print("Keine Provenance-Ressourcen gefunden für die angegebenen Kriterien.")
            
            return data
            
        else:
            print(f"Fehler: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.SSLError as e:
        print(f"SSL-Fehler: {e}")
        print("Hinweis: SSL-Verifikation ist deaktiviert, aber es gab trotzdem einen SSL-Fehler.")
        return None
        
    except requests.exceptions.ConnectionError as e:
        print(f"Verbindungsfehler: {e}")
        print("Stellen Sie sicher, dass der FHIR-Server läuft und erreichbar ist.")
        return None
        
    except requests.exceptions.Timeout as e:
        print(f"Timeout-Fehler: {e}")
        return None
        
    except Exception as e:
        print(f"Unerwarteter Fehler: {e}")
        return None

def main():
    """Hauptfunktion"""
    print("FHIR Provenance Query Script")
    print("=" * 40)
    
    # Standard-Parameter oder Kommandozeilen-Argumente
    if len(sys.argv) > 1:
        occurrence_date = sys.argv[1]
    else:
        occurrence_date = "2025-09-24"
    
    if len(sys.argv) > 2:
        agent = sys.argv[2]
    else:
        agent = None  # Kein Agent-Parameter
    
    # Abfrage ausführen
    result = query_provenance(agent=agent, occurrence_date=occurrence_date)
    
    if result:
        print("\n" + "=" * 40)
        print("Abfrage erfolgreich abgeschlossen!")
        
        # Optional: Vollständige JSON-Ausgabe
        if len(sys.argv) > 3 and sys.argv[3] == "--full":
            print("\nVollständige JSON-Response:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("\n" + "=" * 40)
        print("Abfrage fehlgeschlagen!")
        sys.exit(1)

if __name__ == "__main__":
    main()
