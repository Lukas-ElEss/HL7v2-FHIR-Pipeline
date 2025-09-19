#!/usr/bin/env python3
"""
Script für InfoWash FHIR-Suche nach Provenances mit ServiceRequest occurrence
"""

import sys
from pathlib import Path
from typing import Dict, Any, List

# Add scripts directory to path for imports
sys.path.append(str(Path(__file__).parent))

from fhir_client import get_fhir_client


def search_provenances_for_date_range(start_date: str, end_date: str) -> None:
    """
    Suche nach Provenances mit ServiceRequests in einem bestimmten Zeitraum
    
    Args:
        start_date: Start-Datum im Format YYYY-MM-DDTHH:MM:SS+02:00
        end_date: End-Datum im Format YYYY-MM-DDTHH:MM:SS+02:00
    """
    print("=" * 60)
    print("InfoWash FHIR-Suche nach Provenances")
    print("=" * 60)
    
    try:
        # Initialize FHIR client
        client = get_fhir_client()
        
        # FHIR-Suchparameter für InfoWash
        search_url = (
            f"/Provenance?"
            f"agent=Device/v2-to-fhir-pipeline&"
            f"target:ServiceRequest.occurrence=ge{start_date}&"
            f"target:ServiceRequest.occurrence=lt{end_date}&"
            f"_include=Provenance:target&"
            f"_count=200"
        )
        
        print(f"Such-URL: {search_url}")
        print()
        
        # Führe die Suche durch
        print("Führe FHIR-Suche durch...")
        response = client._make_request('GET', search_url)
        
        if response.success and response.data:
            print("Suche erfolgreich!")
            
            if 'entry' in response.data:
                provenances = response.data['entry']
                print(f"Gefunden: {len(provenances)} Provenance-Ressourcen")
                print()
                
                for i, prov_entry in enumerate(provenances, 1):
                    prov = prov_entry.get('resource', {})
                    prov_id = prov.get('id', 'Unknown')
                    recorded = prov.get('recorded', 'Unknown')
                    targets = prov.get('target', [])
                    
                    print(f"Provenance {i}:")
                    print(f"  ID: {prov_id}")
                    print(f"  Recorded: {recorded}")
                    print(f"  Targets:")
                    for target in targets:
                        ref = target.get('reference', 'Unknown')
                        print(f"    -> {ref}")
                    print()
            else:
                print("Keine Einträge in der Antwort gefunden")
                print(f"Response: {response.data}")
        else:
            print("Suche fehlgeschlagen!")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.data}")
            
    except Exception as e:
        print(f"Fehler bei der Suche: {e}")


if __name__ == "__main__":
    # Beispiel-Suche für den 10. September 2025
    start_date = "2025-09-10T00:00:00+02:00"
    end_date = "2025-09-11T00:00:00+02:00"
    
    print(f"Suche nach Provenances für ServiceRequests zwischen {start_date} und {end_date}")
    print()
    
    search_provenances_for_date_range(start_date, end_date)
