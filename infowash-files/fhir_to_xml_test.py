#!/usr/bin/env python3
"""
FHIR to XML Test Script
Testet die Synchronisation einmalig ohne Loop
"""

import requests
from requests.auth import HTTPBasicAuth
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import sys
import os
from collections import defaultdict

# Konfiguration
FHIR_BASE_URL = "https://localhost:9443/fhir-server/api/v4"
USERNAME = "fhiruser"
PASSWORD = "change-password"
XML_FILE = "Dummys.xml"

def query_fhir_for_today():
    """
    Führt eine FHIR-Abfrage für den heutigen Tag durch
    
    Returns:
        dict: FHIR Bundle Response oder None bei Fehler
    """
    today = "2025-09-24"  # Test mit bekanntem Datum
    
    # Suchparameter für heute
    search_params = {
        'target:ServiceRequest.occurrence': today,
        '_include': 'Provenance:target',
        '_count': '200'
    }
    
    headers = {
        'Content-Type': 'application/fhir+json',
        'Accept': 'application/fhir+json'
    }
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Führe FHIR-Abfrage für {today} durch...")
        
        response = requests.get(
            f"{FHIR_BASE_URL}/Provenance",
            params=search_params,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            verify=False,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            provenances = data.get('entry', [])
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Erfolgreich! Gefunden: {len(provenances)} Provenances")
            return data
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Fehler: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fehler bei FHIR-Abfrage: {e}")
        return None

def extract_fhir_data(bundle_data):
    """
    Extrahiert die relevanten Daten aus FHIR-Ressourcen
    
    Args:
        bundle_data (dict): FHIR Bundle Response
    
    Returns:
        dict: Extrahierte Daten nach Ressourcentyp
    """
    entries = bundle_data.get('entry', [])
    fhir_data = {
        'patients': [],
        'service_requests': [],
        'conditions': []
    }
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Extrahiere FHIR-Daten...")
    
    for entry in entries:
        resource = entry.get('resource', {})
        resource_type = resource.get('resourceType', '')
        
        if resource_type == 'Patient':
            patient_data = {
                'id': resource.get('id', ''),
                'firstname': '',
                'lastname': '',
                'gender': '',
                'birth_date': '',
                'age': 0
            }
            
            # Name extrahieren
            name = resource.get('name', [])
            if name:
                patient_data['firstname'] = ' '.join(name[0].get('given', []))
                patient_data['lastname'] = name[0].get('family', '')
            
            # Geschlecht
            patient_data['gender'] = resource.get('gender', '')
            
            # Geburtsdatum und Alter berechnen
            birth_date = resource.get('birthDate', '')
            if birth_date:
                patient_data['birth_date'] = birth_date
                try:
                    birth_year = int(birth_date.split('-')[0])
                    current_year = datetime.now().year
                    patient_data['age'] = current_year - birth_year
                except:
                    patient_data['age'] = 0
            
            fhir_data['patients'].append(patient_data)
            print(f"  - Patient gefunden: {patient_data['firstname']} {patient_data['lastname']} ({patient_data['gender']}, {patient_data['age']} Jahre)")
            
        elif resource_type == 'ServiceRequest':
            service_data = {
                'id': resource.get('id', ''),
                'type': '',
                'begin': '',
                'duration': 0
            }
            
            # Code/Type extrahieren
            code = resource.get('code', {})
            if code and 'coding' in code and code['coding']:
                service_data['type'] = code['coding'][0].get('display', '')
            
            # Zeitraum extrahieren
            occurrence = resource.get('occurrencePeriod', {})
            if occurrence:
                start = occurrence.get('start', '')
                end = occurrence.get('end', '')
                if start and end:
                    try:
                        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        service_data['begin'] = start_dt.strftime('%H:%M') + ' Uhr'
                        duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
                        service_data['duration'] = duration_minutes
                    except:
                        pass
            
            fhir_data['service_requests'].append(service_data)
            print(f"  - ServiceRequest gefunden: {service_data['type']} ({service_data['begin']}, {service_data['duration']} Min)")
            
        elif resource_type == 'Condition':
            condition_data = {
                'id': resource.get('id', ''),
                'diagnosis': ''
            }
            
            # Diagnose extrahieren
            code = resource.get('code', {})
            if code and 'coding' in code and code['coding']:
                condition_data['diagnosis'] = code['coding'][0].get('display', '')
            
            fhir_data['conditions'].append(condition_data)
            print(f"  - Condition gefunden: {condition_data['diagnosis']}")
    
    return fhir_data

def update_xml_with_fhir_data(xml_file, fhir_data):
    """
    Aktualisiert die XML-Datei mit FHIR-Daten
    
    Args:
        xml_file (str): Pfad zur XML-Datei
        fhir_data (dict): Extrahierte FHIR-Daten
    """
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Lade XML-Datei...")
        
        # XML-Datei laden
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        updated_count = 0
        
        # Patient-Daten in XML aktualisieren (erste verfügbare Tags)
        for i, patient in enumerate(fhir_data['patients']):
            if not patient['firstname'] and not patient['lastname']:
                continue
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Aktualisiere Patient {i+1}: {patient['firstname']} {patient['lastname']}")
            
            # Erste verfügbare surgery in XML finden
            surgeries = root.findall('surgery')
            if i < len(surgeries):
                surgery = surgeries[i]
                topics = surgery.find('topics')
                if topics is not None:
                    patient_info = topics.find('topic[@name="Patienteninformationen"]')
                    if patient_info is not None:
                        basis_info = patient_info.find('subtopic[@name="Basisinformationen"]')
                        if basis_info is not None:
                            # Patient-Daten direkt aktualisieren
                            if patient['firstname']:
                                firstname_elem = basis_info.find('firstname[@name="Vorname"]')
                                if firstname_elem is not None:
                                    firstname_elem.text = patient['firstname']
                                    updated_count += 1
                                    print(f"  - Vorname aktualisiert: {patient['firstname']}")
                            
                            if patient['lastname']:
                                lastname_elem = basis_info.find('lastname[@name="Nachname"]')
                                if lastname_elem is not None:
                                    lastname_elem.text = patient['lastname']
                                    updated_count += 1
                                    print(f"  - Nachname aktualisiert: {patient['lastname']}")
                            
                            if patient['gender']:
                                gender_elem = basis_info.find('gender[@name="Geschlecht"]')
                                if gender_elem is not None:
                                    gender_text = "Weiblich" if patient['gender'] == 'female' else "Männlich"
                                    gender_elem.text = gender_text
                                    updated_count += 1
                                    print(f"  - Geschlecht aktualisiert: {gender_text}")
                            
                            if patient['age'] > 0:
                                age_elem = basis_info.find('age[@name="Alter"]')
                                if age_elem is not None:
                                    age_elem.text = str(patient['age'])
                                    updated_count += 1
                                    print(f"  - Alter aktualisiert: {patient['age']} Jahre")
        
        # ServiceRequest-Daten aktualisieren (nur erste Operation)
        for i, service in enumerate(fhir_data['service_requests']):
            if not service['type']:
                continue
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Aktualisiere ServiceRequest {i+1}: {service['type']}")
            
            # Nur die erste Operation aktualisieren
            surgeries = root.findall('surgery')
            if i < len(surgeries):
                surgery = surgeries[i]
                general = surgery.find('general')
                if general is not None:
                    type_elem = general.find('type')
                    if type_elem is not None:
                        type_elem.text = service['type']
                        updated_count += 1
                        print(f"  - OP-Typ aktualisiert: {service['type']}")
                    
                    # Begin und Duration aktualisieren
                    if service['begin']:
                        begin_elem = general.find('begin')
                        if begin_elem is not None:
                            begin_elem.text = service['begin']
                            updated_count += 1
                            print(f"  - OP-Beginn aktualisiert: {service['begin']}")
                    
                    if service['duration'] > 0:
                        duration_elem = general.find('duration')
                        if duration_elem is not None:
                            duration_elem.text = str(service['duration'])
                            updated_count += 1
                            print(f"  - OP-Dauer aktualisiert: {service['duration']} Min")
        
        # Condition-Daten aktualisieren (nur erste Operation)
        for i, condition in enumerate(fhir_data['conditions']):
            if not condition['diagnosis']:
                continue
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Aktualisiere Condition {i+1}: {condition['diagnosis']}")
            
            # Nur die erste Operation aktualisieren
            surgeries = root.findall('surgery')
            if i < len(surgeries):
                surgery = surgeries[i]
                topics = surgery.find('topics')
                if topics is not None:
                    exam_results = topics.find('topic[@name="Untersuchungsergebnisse"]')
                    if exam_results is not None:
                        diagnosis = exam_results.find('subtopic[@name="Diagnose"]')
                        if diagnosis is not None:
                            long_version = diagnosis.find('longVersion')
                            if long_version is not None:
                                # Sowohl Text als auch name-Attribut aktualisieren
                                long_version.text = condition['diagnosis']
                                long_version.set('name', condition['diagnosis'])
                                updated_count += 1
                                print(f"  - Diagnose aktualisiert: {condition['diagnosis']}")
        
        # XML-Datei speichern
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Speichere XML-Datei...")
        tree.write(xml_file, encoding='UTF-8', xml_declaration=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] XML-Datei aktualisiert: {updated_count} Felder geändert")
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fehler beim XML-Update: {e}")

def main():
    """Hauptfunktion - führt Test einmalig aus"""
    print("FHIR to XML Test Script")
    print(f"XML-Datei: {XML_FILE}")
    print(f"FHIR-Server: {FHIR_BASE_URL}")
    print("=" * 60)
    
    # Prüfen ob XML-Datei existiert
    if not os.path.exists(XML_FILE):
        print(f"Fehler: XML-Datei {XML_FILE} nicht gefunden!")
        sys.exit(1)
    
    # FHIR-Daten abfragen
    fhir_data = query_fhir_for_today()
    
    if fhir_data:
        # Daten extrahieren
        extracted_data = extract_fhir_data(fhir_data)
        
        # XML aktualisieren
        update_xml_with_fhir_data(XML_FILE, extracted_data)
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Keine FHIR-Daten erhalten, XML nicht aktualisiert")

if __name__ == "__main__":
    main()
