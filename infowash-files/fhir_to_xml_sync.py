#!/usr/bin/env python3
"""
FHIR to XML Synchronization Script
Synchronisiert FHIR-Ressourcen mit XML-Datei alle 5 Minuten
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
SYNC_INTERVAL = 300  # 5 Minuten in Sekunden

def query_fhir_for_today():
    """
    Führt eine FHIR-Abfrage für den heutigen Tag durch
    
    Returns:
        dict: FHIR Bundle Response oder None bei Fehler
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
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
    
    return fhir_data

def update_xml_with_fhir_data(xml_file, fhir_data):
    """
    Aktualisiert die XML-Datei mit FHIR-Daten
    
    Args:
        xml_file (str): Pfad zur XML-Datei
        fhir_data (dict): Extrahierte FHIR-Daten
    """
    try:
        # XML-Datei laden
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        updated_count = 0
        
        # Für jeden Patienten in FHIR-Daten
        for patient in fhir_data['patients']:
            if not patient['firstname'] and not patient['lastname']:
                continue
                
            # Patient in XML finden (nach Name)
            for surgery in root.findall('surgery'):
                topics = surgery.find('topics')
                if topics is not None:
                    patient_info = topics.find('topic[@name="Patienteninformationen"]')
                    if patient_info is not None:
                        basis_info = patient_info.find('subtopic[@name="Basisinformationen"]')
                        if basis_info is not None:
                            firstname_elem = basis_info.find('firstname[@name="Vorname"]')
                            lastname_elem = basis_info.find('lastname[@name="Nachname"]')
                            
                            if firstname_elem is not None and lastname_elem is not None:
                                # Prüfen ob Name übereinstimmt
                                xml_firstname = firstname_elem.text or ''
                                xml_lastname = lastname_elem.text or ''
                                
                                if (patient['firstname'].lower() in xml_firstname.lower() and 
                                    patient['lastname'].lower() in xml_lastname.lower()):
                                    
                                    # Patient-Daten aktualisieren
                                    if patient['firstname']:
                                        firstname_elem.text = patient['firstname']
                                    if patient['lastname']:
                                        lastname_elem.text = patient['lastname']
                                    if patient['gender']:
                                        gender_elem = basis_info.find('gender[@name="Geschlecht"]')
                                        if gender_elem is not None:
                                            gender_text = "Weiblich" if patient['gender'] == 'female' else "Männlich"
                                            gender_elem.text = gender_text
                                    if patient['age'] > 0:
                                        age_elem = basis_info.find('age[@name="Alter"]')
                                        if age_elem is not None:
                                            age_elem.text = str(patient['age'])
                                    
                                    updated_count += 1
                                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Patient {patient['firstname']} {patient['lastname']} aktualisiert")
        
        # ServiceRequest-Daten aktualisieren
        for service in fhir_data['service_requests']:
            if not service['type']:
                continue
                
            # OP-Typ in XML finden und aktualisieren
            for surgery in root.findall('surgery'):
                general = surgery.find('general')
                if general is not None:
                    type_elem = general.find('type')
                    if type_elem is not None:
                        type_elem.text = service['type']
                        updated_count += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] OP-Typ aktualisiert: {service['type']}")
                
                # Begin und Duration aktualisieren
                if service['begin']:
                    begin_elem = general.find('begin')
                    if begin_elem is not None:
                        begin_elem.text = service['begin']
                        updated_count += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] OP-Beginn aktualisiert: {service['begin']}")
                
                if service['duration'] > 0:
                    duration_elem = general.find('duration')
                    if duration_elem is not None:
                        duration_elem.text = str(service['duration'])
                        updated_count += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] OP-Dauer aktualisiert: {service['duration']} Min")
        
        # Condition-Daten aktualisieren
        for condition in fhir_data['conditions']:
            if not condition['diagnosis']:
                continue
                
            # Diagnose in XML finden und aktualisieren
            for surgery in root.findall('surgery'):
                topics = surgery.find('topics')
                if topics is not None:
                    exam_results = topics.find('topic[@name="Untersuchungsergebnisse"]')
                    if exam_results is not None:
                        diagnosis = exam_results.find('subtopic[@name="Diagnose"]')
                        if diagnosis is not None:
                            long_version = diagnosis.find('longVersion')
                            if long_version is not None:
                                long_version.text = condition['diagnosis']
                                updated_count += 1
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] Diagnose aktualisiert: {condition['diagnosis']}")
        
        # XML-Datei speichern
        tree.write(xml_file, encoding='UTF-8', xml_declaration=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] XML-Datei aktualisiert: {updated_count} Felder geändert")
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fehler beim XML-Update: {e}")

def main():
    """Hauptfunktion - läuft kontinuierlich alle 5 Minuten"""
    print("FHIR to XML Synchronization Script gestartet")
    print(f"XML-Datei: {XML_FILE}")
    print(f"Sync-Intervall: {SYNC_INTERVAL} Sekunden")
    print(f"FHIR-Server: {FHIR_BASE_URL}")
    print("=" * 60)
    
    # Prüfen ob XML-Datei existiert
    if not os.path.exists(XML_FILE):
        print(f"Fehler: XML-Datei {XML_FILE} nicht gefunden!")
        sys.exit(1)
    
    try:
        while True:
            # FHIR-Daten abfragen
            fhir_data = query_fhir_for_today()
            
            if fhir_data:
                # Daten extrahieren
                extracted_data = extract_fhir_data(fhir_data)
                
                # XML aktualisieren
                update_xml_with_fhir_data(XML_FILE, extracted_data)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Keine FHIR-Daten erhalten, XML nicht aktualisiert")
            
            # Warten bis zur nächsten Ausführung
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Warte {SYNC_INTERVAL} Sekunden bis zur nächsten Synchronisation...")
            time.sleep(SYNC_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Script beendet durch Benutzer")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Unerwarteter Fehler: {e}")

if __name__ == "__main__":
    main()
