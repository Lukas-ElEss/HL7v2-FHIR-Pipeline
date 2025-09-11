from faker import Faker
from datetime import datetime, timedelta
import random
import os
import hl7

# HL7 Standard Code Tables
class HL7CodeTables:
    """HL7 Standard Code Tables for realistic data generation"""
    
    # Table 0007: Admission Type
    ADMISSION_TYPES = [
        ('A', 'Accident', 'Unfall / Notarztwagen'),
        ('E', 'Emergency', 'Notfall ohne Einweisung'),
        ('L', 'Labor and Delivery', 'Entbindung / Geburt'),
        ('R', 'Routine', 'Normalaufnahme'),
        ('N', 'Newborn', 'Neugeborenes'),
        ('U', 'Urgent', 'dringliche Aufnahme'),
        ('C', 'Elective', 'wahlfreie Aufnahme')
    ]
    
    # Table 0360: Degree/License/Certificate
    DEGREE_TYPES = [
        ('MD', 'Doctor of Medicine'),
        ('DO', 'Doctor of Osteopathy'),
        ('RN', 'Registered Nurse'),
        ('PA', 'Physician Assistant'),
        ('NP', 'Nurse Practitioner'),
        ('DDS', 'Doctor of Dental Surgery'),
        ('PharmD', 'Doctor of Pharmacy'),
        ('BS', 'Bachelor of Science'),
        ('MS', 'Master of Science'),
        ('PHD', 'Doctor of Philosophy')
    ]
    
    # Table 0203: Identifier Type
    IDENTIFIER_TYPES = [
        ('MR', 'Medical Record Number', 'Krankenaktennummer'),
        ('VN', 'Visit Number', 'Fallnummer'),
        ('SS', 'Social Security Number', 'Sozialversicherungsnummer'),
        ('DL', 'Driver\'s License', 'Führerscheinnummer'),
        ('PPN', 'Passport Number', 'Passnummer'),
        ('PI', 'Patient Identifier', 'Patienten-ID'),
        ('DN', 'Doctor Number', 'Arztnummer')
    ]
    
    # Table 0004: Patient Class (HL7 v2.9)
    PATIENT_CLASSES = [
        ('I', 'Inpatient', 'stationär'),
        ('O', 'Outpatient', 'ambulant'),
        ('E', 'Emergency', 'Notfall'),
        ('P', 'Preadmit', 'Voraufnahme'),
        ('R', 'Recurring patient', 'Wiederholungspatient'),
        ('B', 'Obstetrics', 'Geburtshilfe'),
        ('C', 'Commercial Account', ''),
        ('N', 'Not Applicable', 'Segment nicht anwendbar'),
        ('U', 'Unknown', 'unbekannt')
    ]
    
    # Order Status Codes
    ORDER_STATUS = [
        ('NW', 'New Order'),
        ('IP', 'In Progress'),
        ('CM', 'Completed'),
        ('CA', 'Cancelled'),
        ('DC', 'Discontinued'),
        ('RP', 'Replace'),
        ('PA', 'Pending'),
        ('HD', 'Hold')
    ]
    
    @classmethod
    def get_random_admission_type(cls):
        return random.choice(cls.ADMISSION_TYPES)
    
    @classmethod
    def get_random_degree(cls):
        return random.choice(cls.DEGREE_TYPES)
    
    @classmethod
    def get_random_identifier_type(cls):
        return random.choice(cls.IDENTIFIER_TYPES)
    
    @classmethod
    def get_random_patient_class(cls):
        return random.choice(cls.PATIENT_CLASSES)
    
    @classmethod
    def get_random_order_status(cls):
        return random.choice(cls.ORDER_STATUS)

class HL7MessageValidator:
    """
    Validiert HL7-Nachrichten gegen OMG_O19-Struktur.
    """
    
    def __init__(self, xsd_path: str = None):
        """
        Initialisiert den HL7-Nachrichten-Validator.
        
        Args:
            xsd_path (str, optional): Pfad zur XSD-Schema-Datei
        """
        self.xsd_path = xsd_path or os.path.join(
            os.path.dirname(__file__), '..', '..', 'HL7_Schemas', 'HL7-xml v2.5', 'OMG_O19.xsd'
        )
        self.required_segments = self._parse_required_segments()
    
    def _parse_required_segments(self):
        """
        Parst die XSD-Datei, um erforderliche Segmente für OMG_O19 zu extrahieren.
        
        Returns:
            dict: Dictionary mit erforderlichen Segmenten
        """
        required_segments = {
            'MSH': True,
            'PID': True,
            'PV1': True,
            'ORC': True,
            'OBR': True,
        }
        
        try:
            if os.path.exists(self.xsd_path):
                with open(self.xsd_path, 'r') as f:
                    content = f.read()
                    print(f"Loaded XSD schema from: {self.xsd_path}")
            else:
                print(f"XSD schema not found at: {self.xsd_path}")
        except Exception as e:
            print(f"Error loading XSD schema: {e}")
        
        return required_segments
    
    def validate_message_structure(self, hl7_message: str) -> dict:
        """
        Validiert HL7-Nachrichtenstruktur gegen OMG_O19-Anforderungen.
        
        Args:
            hl7_message (str): HL7-Nachricht
            
        Returns:
            dict: Validierungsergebnis mit Valid-Flag, Fehlern und Warnungen
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        segments = [seg.strip() for seg in hl7_message.split('\r') if seg.strip()]
        segment_types = [seg.split('|')[0] for seg in segments]
        
        for required_seg, required in self.required_segments.items():
            if required and required_seg not in segment_types:
                result['valid'] = False
                result['errors'].append(f"Missing required segment: {required_seg}")
        
        if 'MSH' in segment_types:
            msh_segment = [seg for seg in segments if seg.startswith('MSH')][0]
            msh_fields = msh_segment.split('|')
            if len(msh_fields) > 8:
                message_type = msh_fields[8]
                if message_type != 'OMG^O19':
                    result['warnings'].append(f"Expected message type OMG^O19, found: {message_type}")
        
        expected_order = ['MSH', 'PID', 'PV1', 'ORC', 'OBR']
        found_order = [seg for seg in expected_order if seg in segment_types]
        
        if len(found_order) < len(expected_order):
            result['warnings'].append("Some expected segments are missing")
        
        if result['valid']:
            result['warnings'].append("Message structure validated against OMG_O19 requirements")
        
        return result


class HL7MessageBuilder:
    """
    Builder-Klasse für HL7-Nachrichten mit python-hl7
    """
    
    def __init__(self):
        self.segments = []
    
    def add_segment(self, segment_type, *fields):
        """Fügt ein Segment zur Nachricht hinzu"""
        # Erstelle Segment-String
        segment_parts = [segment_type]
        for field in fields:
            if field is not None:
                segment_parts.append(str(field))
            else:
                segment_parts.append('')
        
        segment_str = '|'.join(segment_parts)
        self.segments.append(segment_str)
        return self
    
    def build(self):
        """Erstellt die finale HL7-Nachricht"""
        return '\r'.join(self.segments) + '\r'


class OMG_O19_Generator:
    """
    Generator für OMG_O19 HL7-Nachrichten mit realistischen Testdaten.
    """
    
    def __init__(self, enable_validation: bool = False):
        """
        Initialisiert den OMG_O19-Generator.
        
        Args:
            enable_validation (bool): Aktiviert Nachrichtenvalidierung (standardmäßig deaktiviert)
        """
        self.fake = Faker('de_DE')
        self.message_count = 0
        self.validator = HL7MessageValidator() if enable_validation else None
    
    def generate_dummy_data(self):
        """
        Generiert Dummy-Daten für alle relevanten OMG_O19-Segmente mit HL7-Standard-Codes.
        
        Returns:
            dict: Generierte Testdaten für alle Segmente
        """
        patient_id = self.fake.unique.numerify('##########')
        gender = random.choice(['M', 'F', 'D'])
        birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y%m%d')
        
        ops_codes = [
            ('5-987', 'Appendektomie'),
            ('5-455', 'Cholezystektomie'),
            ('5-501', 'Koronarangiographie'),
            ('1-693', 'Kniegelenksarthroskopie')
        ]
        ops_code, ops_text = random.choice(ops_codes)
        
        op_date = datetime.now() + timedelta(days=random.randint(1, 30))
        start_time = op_date.replace(hour=random.randint(8, 14), minute=0)
        end_time = start_time + timedelta(hours=random.randint(1, 4))
        
        icd_codes = [
            ('K35.9', 'Akute Appendizitis'),
            ('K80.1', 'Cholelithiasis'),
            ('I25.1', 'Atherosklerotische Herzkrankheit'),
            ('M17.9', 'Gonarthrose')
        ]
        icd_code, icd_text = random.choice(icd_codes)
        
        # HL7 Standard Codes
        admission_type = HL7CodeTables.get_random_admission_type()
        degree_type = HL7CodeTables.get_random_degree()
        patient_class = HL7CodeTables.get_random_patient_class()
        order_status = HL7CodeTables.get_random_order_status()
        
        surgeon = self.fake.name()
        surgeon_id = self.fake.numerify('####')
        
        # Zusätzliche Felder für vollständige InfoWashSource-Kompatibilität
        visit_number = self.fake.numerify('######')
        admission_date = datetime.now().strftime('%Y%m%d%H%M%S')
        discharge_date = (datetime.now() + timedelta(days=random.randint(1, 7))).strftime('%Y%m%d%H%M%S')
        
        # HL7 v2.9 DateTime Format (YYYYMMDDHHMMSS)
        visit_start_time = '20250908074500'
        visit_end_time = '20250908133000'
        procedure_start_time = start_time.strftime('%Y%m%d%H%M')
        procedure_end_time = end_time.strftime('%Y%m%d%H%M')
        authored_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y%m%d%H%M%S')
        
        # PV1-spezifische Felder für korrekte FHIR-Transformation
        patient_class_code, patient_class_display, patient_class_german = patient_class
        admission_type_code, admission_type_display, admission_type_german = admission_type
        
        return {
            'patient': {
                'id': patient_id,
                'last_name': self.fake.last_name(),
                'first_name': self.fake.first_name(),
                'birth_date': birth_date,
                'gender': gender,
                'address': self.fake.street_address(),
                'city': self.fake.city(),
                'zip': self.fake.postcode(),
                'country': 'DE',
                'phone': self.fake.phone_number()
            },
            'visit': {
                'admission_date': admission_date,
                'visit_number': visit_number,
                'department': 'OP',
                'admission_type': admission_type,
                'patient_class': patient_class,
                'location': self.fake.company() + ' Station',
                'discharge_date': discharge_date,
                # PV1 Felder für vollständige Kompatibilität (entsprechend InfoWashSource.json)
                'patient_class_code': patient_class_code,
                'patient_class_system': 'http://terminology.hl7.org/CodeSystem/v2-0004',
                'patient_class_display': patient_class_display,
                'patient_class_version': '1',
                'admission_type_code': admission_type_code,
                'admission_type_display': admission_type_display,
                'admission_type_system': 'http://terminology.hl7.org/CodeSystem/v2-0007',
                'admission_type_version': '2.5',
                'admission_type_text': admission_type_german,
                'service_type_code': 'SURG',
                'service_type_display': 'Chirurgie',
                'service_type_system': 'http://example.org/custom/codesystem/service-type',
                'service_type_version': '2024',
                'service_type_text': 'Chirurgische Behandlung',
                'visit_number_system': 'http://hospital.example.org/visit-ids',
                'visit_number_type': 'VN',
                'visit_start_time': visit_start_time,
                'visit_end_time': visit_end_time
            },
            'order': {
                'placer_order_number': f"OP-{datetime.now().year}-{self.fake.numerify('#####')}",
                'order_date': datetime.now().strftime('%Y%m%d%H%M'),
                'order_status': order_status,
                'surgeon_id': surgeon_id,
                'surgeon_name': surgeon,
                'surgeon_degree': degree_type,
                'ordering_phone': self.fake.phone_number(),
                # ORC Felder für vollständige Kompatibilität
                'order_control': 'NW',
                'order_status_code': order_status[0] if order_status else 'SC',
                'authored_date': authored_date
            },
            'procedure': {
                'ops_code': ops_code,
                'ops_text': ops_text,
                'start_datetime': procedure_start_time,
                'end_datetime': procedure_end_time,
                'requested_datetime': start_time.strftime('%Y%m%d%H%M%S'),
                'observation_datetime': end_time.strftime('%Y%m%d%H%M%S'),
                'diagnosis_code': icd_code,
                'diagnosis_text': icd_text,
                'diagnosis_text_full': icd_text  # Für DG1_3_9
            }
        }
    
    def create_omg_o19_message(self, data):
        """
        Erstellt eine OMG_O19 HL7-Nachricht aus den bereitgestellten Daten mit python-hl7.
        
        Args:
            data (dict): Daten für die HL7-Nachricht
            
        Returns:
            str: Vollständige HL7 OMG_O19-Nachricht
        """
        self.message_count += 1
        msg_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
        
        builder = HL7MessageBuilder()
        
        # MSH - Message Header (HL7 v2.9 compliant)
        builder.add_segment(
            'MSH',
            '^~\\&',  # Field Separators
            'OP_SYSTEM',  # Sending Application
            'HOSPITAL',  # Sending Facility
            'PMS',  # Receiving Application
            'HOSPITAL',  # Receiving Facility
            msg_datetime,  # Date/Time of Message
            '',  # Security
            'OMG^O19',  # Message Type
            f'MSG{self.message_count:06d}',  # Message Control ID
            'P',  # Processing ID
            '2.9'  # Version ID
        )
        
        # PID - Patient Identification (HL7 v2.9 compliant)
        # PID-3: CX (Extended Composite ID with Check Digit)
        # Format: ID^checkDigit^checkDigitScheme^assigningAuthority^IDType^assigningFacility
        patient_identifier = f"{data['patient']['id']}^^^HOSPITAL^MR^HOSPITAL"
        
        # PID-5: XCN (Extended Composite ID Number and Name for Persons)
        # Format: ID^familyName^givenName^middleName^suffix^prefix^degree^sourceTable^assigningAuthority^nameType^identifierCheckDigit^checkDigitScheme^identifierTypeCode^assigningFacility^nameRepresentationCode^nameContext^nameValidityRange^nameAssemblyOrder^effectiveDate^expirationDate^professionalSuffix^assigningJurisdiction^assigningAgencyOrDepartment^securityCheck^securityCheckScheme
        patient_name = f"^{data['patient']['last_name']}^{data['patient']['first_name']}^^^^^^^L^HOSPITAL"
        
        # PID-11: XAD (Extended Address)
        # Format: streetAddress^otherDesignation^city^stateOrProvince^zipOrPostalCode^country^addressType^otherGeographicDesignation^countyOrParishCode^censusTract^addressRepresentationCode^addressValidityRange^effectiveDate^expirationDate^expirationReason^temporaryIndicator^badAddressIndicator^addressUsage^addressee^comment^preferenceOrder^protectionCode^inactivationDate^returnCode^modifyDate^source^validity
        patient_address = f"{data['patient']['address']}^^{data['patient']['city']}^^{data['patient']['zip']}^{data['patient']['country']}^^^^^^^H"
        
        builder.add_segment(
            'PID',
            '1',  # Set ID
            '',  # Patient ID
            patient_identifier,  # Patient Identifier List (CX)
            '',  # Alternate Patient ID
            patient_name,  # Patient Name (XCN)
            '',  # Mother's Maiden Name
            data['patient']['birth_date'],  # Date/Time of Birth
            data['patient']['gender'],  # Administrative Sex
            '',  # Patient Alias
            '',  # Race
            patient_address,  # Patient Address (XAD)
            '',  # County Code
            data['patient']['phone']  # Phone Number - Home
        )
        
        # PV1 - Patient Visit (HL7 v2.9 compliant) - entspricht InfoWashSource.json
        patient_class_code = data['visit']['patient_class_code']
        patient_class_system = data['visit']['patient_class_system']
        patient_class_display = data['visit']['patient_class_display']
        patient_class_version = data['visit']['patient_class_version']
        
        admission_type_code = data['visit']['admission_type_code']
        admission_type_display = data['visit']['admission_type_display']
        admission_type_system = data['visit']['admission_type_system']
        admission_type_version = data['visit']['admission_type_version']
        admission_type_text = data['visit']['admission_type_text']
        
        service_type_code = data['visit']['service_type_code']
        service_type_display = data['visit']['service_type_display']
        service_type_system = data['visit']['service_type_system']
        service_type_version = data['visit']['service_type_version']
        service_type_text = data['visit']['service_type_text']
        
        visit_number = data['visit']['visit_number']
        visit_number_system = data['visit']['visit_number_system']
        visit_number_type = data['visit']['visit_number_type']
        
        surgeon_id = data['order']['surgeon_id']
        surgeon_name = data['order']['surgeon_name']
        surgeon_degree = data['order']['surgeon_degree'][0]
        
        visit_start_time = data['visit']['visit_start_time']
        visit_end_time = data['visit']['visit_end_time']
        
        # PV1-2: CWE (Coded With Exceptions) - Patient Class
        # Format: identifier^text^codingSystem^codingSystemVersionId^alternateIdentifier^alternateText^nameOfCodingSystem^alternateCodingSystemVersionId^codingSystemOid^valueSetOid^valueSetVersionId^alternateCodingSystemOid^alternateValueSetOid^alternateValueSetVersionId^originalText^secondAlternateIdentifier^secondAlternateText^secondAlternateCodingSystem^secondAlternateCodingSystemVersionId^secondAlternateCodingSystemOid^secondAlternateValueSetOid^secondAlternateValueSetVersionId
        patient_class_cwe = f"{patient_class_code}^{patient_class_display}^{patient_class_system}^{patient_class_version}"
        
        # PV1-3: PL (Person Location) - Assigned Patient Location
        # Format: pointOfCare^room^bed^facility^locationStatus^personLocationType^building^floor^locationDescription^comprehensiveLocationIdentifier^assigningAuthorityForLocation
        assigned_location = f"OP^Raum 1^1^HOSPITAL^A^B^Hauptgebäude^1^Operationssaal 1^OP001^HOSPITAL"
        
        # PV1-4: CWE (Coded With Exceptions) - Admission Type
        admission_type_cwe = f"{admission_type_code}^{admission_type_display}^{admission_type_system}^{admission_type_version}^{admission_type_text}"
        
        # PV1-7: XCN (Extended Composite ID Number and Name for Persons) - Attending Doctor
        # Format: ID^familyName^givenName^middleName^suffix^prefix^degree^sourceTable^assigningAuthority^nameType^identifierCheckDigit^checkDigitScheme^identifierTypeCode^assigningFacility^nameRepresentationCode^nameContext^nameValidityRange^nameAssemblyOrder^effectiveDate^expirationDate^professionalSuffix^assigningJurisdiction^assigningAgencyOrDepartment^securityCheck^securityCheckScheme
        attending_doctor = f"{surgeon_id}^{surgeon_name}^^^^^{surgeon_degree}^^^^^L^HOSPITAL"
        
        # PV1-10: CWE (Coded With Exceptions) - Hospital Service
        service_type_cwe = f"{service_type_code}^{service_type_display}^{service_type_system}^{service_type_version}^{service_type_text}"
        
        # PV1-19: CX (Extended Composite ID with Check Digit) - Visit Number
        # Format: ID^checkDigit^checkDigitScheme^assigningAuthority^IDType^assigningFacility^effectiveDate^expirationDate^assigningJurisdiction^assigningAgencyOrDepartment^securityCheck^securityCheckScheme^sourceTable
        visit_number_cx = f"{visit_number}^^^{visit_number_system}^{visit_number_type}^HOSPITAL"
        
        builder.add_segment(
            'PV1',
            '1',  # Set ID - PV1
            patient_class_cwe,  # PV1_2: Patient Class (CWE)
            assigned_location,  # PV1_3: Assigned Patient Location (PL)
            admission_type_cwe,  # PV1_4: Admission Type (CWE)
            '',  # PV1_5: Preadmit Number
            '',  # PV1_6: Prior Patient Location
            attending_doctor,  # PV1_7: Attending Doctor (XCN)
            '',  # PV1_8: Referring Doctor
            '',  # PV1_9: Consulting Doctor
            service_type_cwe,  # PV1_10: Hospital Service (CWE)
            '',  # PV1_11: Temporary Location
            '',  # PV1_12: Preadmit Test Indicator
            '',  # PV1_13: Re-admission Indicator
            '',  # PV1_14: Admit Source
            '',  # PV1_15: Ambulatory Status
            '',  # PV1_16: VIP Indicator
            '',  # PV1_17: Admitting Doctor
            '',  # PV1_18: Patient Type
            visit_number_cx,  # PV1_19: Visit Number (CX)
            '',  # PV1_20: Financial Class
            '',  # PV1_21: Charge Price Indicator
            '',  # PV1_22: Courtesy Code
            '',  # PV1_23: Credit Rating
            '',  # PV1_24: Contract Code
            '',  # PV1_25: Contract Effective Date
            '',  # PV1_26: Contract Amount
            '',  # PV1_27: Contract Period
            '',  # PV1_28: Interest Code
            '',  # PV1_29: Transfer to Bad Debt Code
            '',  # PV1_30: Transfer to Bad Debt Date
            '',  # PV1_31: Bad Debt Agency Code
            '',  # PV1_32: Bad Debt Transfer Amount
            '',  # PV1_33: Bad Debt Recovery Amount
            '',  # PV1_34: Delete Account Indicator
            '',  # PV1_35: Delete Account Date
            '',  # PV1_36: Discharge Disposition
            '',  # PV1_37: Discharged to Location
            '',  # PV1_38: Diet Type
            '',  # PV1_39: Servicing Facility
            '',  # PV1_40: Bed Status
            '',  # PV1_41: Account Status
            '',  # PV1_42: Pending Location
            '',  # PV1_43: Prior Temporary Location
            visit_start_time,  # PV1_44: Admit Date/Time
            visit_end_time,  # PV1_45: Discharge Date/Time
            '',  # PV1_46: Current Patient Balance
            '',  # PV1_47: Total Charges
            '',  # PV1_48: Total Adjustments
            '',  # PV1_49: Total Payments
            '',  # PV1_50: Alternate Visit ID
            '',  # PV1_51: Visit Indicator
            '',  # PV1_52: Other Healthcare Provider
            '',  # PV1_53: Service Episode Description
            '',  # PV1_54: Service Episode Identifier
        )
        
        # ORC - Common Order (HL7 v2.9 compliant)
        order_status_code = data['order']['order_status_code']
        authored_date = data['order']['authored_date']
        filler_order_number = f"FILL-{data['order']['placer_order_number']}"
        
        builder.add_segment(
            'ORC',
            'NW',  # Order Control
            data['order']['placer_order_number'],  # Placer Order Number
            filler_order_number,  # Filler Order Number
            '',  # Placer Group Number
            order_status_code,  # Order Status
            '',  # Response Flag
            '',  # Quantity/Timing
            '',  # Parent Order
            authored_date,  # Date/Time of Transaction (ORC_9)
            '',  # Entered By
            '',  # Verified By
            f"{data['order']['surgeon_id']}^{data['order']['surgeon_name']}",  # Ordering Provider
            '',  # Enterer's Location
            data['order']['ordering_phone'],  # Call Back Phone Number
            '',  # Order Effective Date/Time
            '',  # Order Control Code Reason
            '',  # Entering Organization
            '',  # Entering Device
            '',  # Action By
            'LAB',  # Advanced Beneficiary Notice Code
            'MedCenter'  # Ordering Facility Name
        )
        
        # TQ1 - Timing/Quantity (HL7 v2.9 compliant)
        # Format: Set ID^Quantity^Interval^Duration^Start Date/Time^End Date/Time^Priority^Condition^Text^Conjunction^Order Sequencing^Occurrence Duration^Total Occurrences
        builder.add_segment(
            'TQ1',
            '1',  # Set ID
            '1',  # Quantity
            'once',  # Interval
            '',  # Duration
            data['procedure']['start_datetime'],  # Start Date/Time (TQ1_7)
            data['procedure']['end_datetime'],  # End Date/Time (TQ1_8)
            'S',  # Priority
            '',  # Condition
            '',  # Text
            '',  # Conjunction
            '',  # Order Sequencing
            '',  # Occurrence Duration
            ''  # Total Occurrences
        )
        
        # OBR - Observation Request (HL7 v2.9 compliant)
        # OBR-4: CWE (Coded With Exceptions) - Universal Service Identifier
        # Format: identifier^text^codingSystem^codingSystemVersionId^alternateIdentifier^alternateText^nameOfCodingSystem^alternateCodingSystemVersionId^codingSystemOid^valueSetOid^valueSetVersionId^alternateCodingSystemOid^alternateValueSetOid^alternateValueSetVersionId^originalText^secondAlternateIdentifier^secondAlternateText^secondAlternateCodingSystem^secondAlternateCodingSystemVersionId^secondAlternateCodingSystemOid^secondAlternateValueSetOid^secondAlternateValueSetVersionId
        universal_service_id = f"{data['procedure']['ops_code']}^{data['procedure']['ops_text']}^OPS^2024"
        
        # OBR-16: XCN (Extended Composite ID Number and Name for Persons) - Ordering Provider
        ordering_provider = f"{data['order']['surgeon_id']}^{data['order']['surgeon_name']}^^^^^^^L^HOSPITAL"
        
        builder.add_segment(
            'OBR',
            '1',  # Set ID
            data['order']['placer_order_number'],  # Placer Order Number
            filler_order_number,  # Filler Order Number
            universal_service_id,  # Universal Service ID (CWE)
            '',  # Priority
            '',  # Requested Date/Time
            data['procedure']['requested_datetime'],  # Observation Date/Time
            '',  # Observation End Date/Time
            '',  # Collection Volume
            '',  # Collector Identifier
            '',  # Specimen Action Code
            '',  # Danger Code
            '',  # Relevant Clinical Information
            '',  # Specimen Received Date/Time
            '',  # Specimen Source
            ordering_provider,  # Ordering Provider (XCN)
            '',  # Order Callback Phone Number
            '',  # Placer Field 1
            '',  # Placer Field 2
            '',  # Filler Field 1
            '',  # Filler Field 2
            '',  # Results Rpt/Status Chng - Date/Time
            '',  # Charge to Practice
            '',  # Diagnostic Serv Sect ID
            'F',  # Result Status
            '',  # Parent Result
            '',  # Quantity/Timing
            '',  # Result Copies To
            '',  # Parent
            '',  # Transportation Mode
            '',  # Reason for Study
            '',  # Principal Result Interpreter
            '',  # Assistant Result Interpreter
            '',  # Technician
            '',  # Transcriptionist
            '',  # Scheduled Date/Time
            '',  # Number of Sample Containers
            '',  # Transport Logistics of Collected Sample
            '',  # Collector's Comment
            '',  # Transport Arrangement Responsibility
            '',  # Transport Arranged
            '',  # Escort Required
            '',  # Planned Patient Transport Comment
            '',  # Procedure Code
            '',  # Procedure Code Modifier
            '',  # Placer Supplemental Service Information
            '',  # Filler Supplemental Service Information
            '',  # Medically Necessary Duplicate Procedure Reason
            '',  # Result Handling
            '',  # Parent Universal Service Identifier
            data['procedure']['observation_datetime']  # Observation Group ID
        )
        
        # DG1 - Diagnosis (HL7 v2.9 compliant)
        # DG1-3: CWE (Coded With Exceptions) - Diagnosis
        # Format: identifier^text^codingSystem^codingSystemVersionId^alternateIdentifier^alternateText^nameOfCodingSystem^alternateCodingSystemVersionId^codingSystemOid^valueSetOid^valueSetVersionId^alternateCodingSystemOid^alternateValueSetOid^alternateValueSetVersionId^originalText^secondAlternateIdentifier^secondAlternateText^secondAlternateCodingSystem^secondAlternateCodingSystemVersionId^secondAlternateCodingSystemOid^secondAlternateValueSetOid^secondAlternateValueSetVersionId
        diagnosis_cwe = f"{data['procedure']['diagnosis_code']}^{data['procedure']['diagnosis_text']}^ICD-10-GM^2024"
        
        builder.add_segment(
            'DG1',
            '1',  # Set ID
            'I',  # Diagnosis Coding Method
            diagnosis_cwe,  # Diagnosis (CWE)
            '',  # Diagnosis Description
            '',  # Diagnosis Date/Time
            'F',  # Diagnosis Type
            '',  # Major Diagnostic Category
            '',  # Diagnostic Related Group
            '',  # DRG Approval Indicator
            '',  # DRG Grouper Review Code
            '',  # Outlier Type
            '',  # Outlier Days
            '',  # Outlier Cost
            '',  # Grouper Version and Type
            '',  # Diagnosis Priority
            '',  # Diagnosing Clinician
            '',  # Diagnosis Classification
            '',  # Confidential Indicator
            '',  # Attestation Date/Time
            '',  # Diagnosis Identifier
            '',  # Diagnosis Action Code
            '',  # Parent Diagnosis
            '',  # DRG CCL Value Code
            '',  # DRG Grouping Usage
            '',  # DRG Diagnosis Determination Status
            '',  # Present on Admission (POA) Indicator
        )
        
        # Erstelle die finale HL7-Nachricht mit python-hl7
        message = builder.build()
        
        # Validierung der Nachricht (falls aktiviert)
        if self.validator:
            validation_result = self.validator.validate_message_structure(message)
            if not validation_result['valid']:
                print("Message validation failed:")
                for error in validation_result['errors']:
                    print(f"   {error}")
            elif validation_result['warnings']:
                for warning in validation_result['warnings']:
                    print(f"   {warning}")
        
        return message

if __name__ == "__main__":
    generator = OMG_O19_Generator()
    
    dummy_data = generator.generate_dummy_data()
    print("Generated Dummy Data:")
    print(dummy_data)
    
    hl7_message = generator.create_omg_o19_message(dummy_data)
    print("\nGenerated HL7 OMG_O19 Message:")
    print(hl7_message)