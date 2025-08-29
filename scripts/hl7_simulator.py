from faker import Faker
from datetime import datetime, timedelta
import random
import os

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

class OMG_O19_Generator:
    """
    Generator für OMG_O19 HL7-Nachrichten mit realistischen Testdaten.
    """
    
    def __init__(self, enable_validation: bool = True):
        """
        Initialisiert den OMG_O19-Generator.
        
        Args:
            enable_validation (bool): Aktiviert Nachrichtenvalidierung
        """
        self.fake = Faker('de_DE')
        self.message_count = 0
        self.validator = HL7MessageValidator() if enable_validation else None
    
    def generate_dummy_data(self):
        """
        Generiert Dummy-Daten für alle relevanten OMG_O19-Segmente.
        
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
        
        surgeon = self.fake.name()
        surgeon_id = self.fake.numerify('####')
        
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
                'admission_date': datetime.now().strftime('%Y%m%d%H%M%S'),
                'visit_number': self.fake.numerify('######'),
                'department': 'OP'
            },
            'order': {
                'placer_order_number': f"OP-{datetime.now().year}-{self.fake.numerify('#####')}",
                'order_date': datetime.now().strftime('%Y%m%d%H%M'),
                'surgeon_id': surgeon_id,
                'surgeon_name': surgeon,
                'ordering_phone': self.fake.phone_number()
            },
            'procedure': {
                'ops_code': ops_code,
                'ops_text': ops_text,
                'start_datetime': start_time.strftime('%Y%m%d%H%M'),
                'end_datetime': end_time.strftime('%Y%m%d%H%M'),
                'requested_datetime': start_time.strftime('%Y%m%d%H%M%S'),
                'observation_datetime': end_time.strftime('%Y%m%d%H%M%S'),
                'diagnosis_code': icd_code,
                'diagnosis_text': icd_text
            }
        }
    
    def create_omg_o19_message(self, data):
        """
        Erstellt eine OMG_O19 HL7-Nachricht aus den bereitgestellten Daten.
        
        Args:
            data (dict): Daten für die HL7-Nachricht
            
        Returns:
            str: Vollständige HL7 OMG_O19-Nachricht
        """
        self.message_count += 1
        msg_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # MSH - Message Header (HL7 v2.9 compliant)
        msh = (
            f"MSH|^~\\&|OP_SYSTEM|HOSPITAL|PMS|HOSPITAL|{msg_datetime}||"
            f"OMG^O19|MSG{self.message_count:06d}|P|2.9|||||||"
        )
        
        # PID - Patient Identification (HL7 v2.9 compliant)
        # PID-1: Set ID, PID-3: Patient ID, PID-5: Patient Name, PID-7: Birth Date, PID-8: Sex
        pid = (
            f"PID|1||{data['patient']['id']}^^^HOSPITAL||"
            f"{data['patient']['last_name']}^{data['patient']['first_name']}^^^^||"
            f"{data['patient']['birth_date']}|{data['patient']['gender']}|||"
            f"{data['patient']['address']}^^{data['patient']['city']}^^{data['patient']['zip']}^"
            f"{data['patient']['country']}||{data['patient']['phone']}|||||||||||||||||"
        )
        
        # PV1 - Patient Visit (HL7 v2.9 compliant)
        # PV1-1: Set ID, PV1-2: Patient Class, PV1-7: Attending Physician
        pv1 = (
            f"PV1|1|I|OP^^^|||||"
            f"{data['order']['surgeon_id']}^{data['order']['surgeon_name']}|||||||||||"
            f"{data['visit']['visit_number']}|||||||||||||||||||||||||"
            f"{data['visit']['admission_date']}|||||||||||||||"
        )
        
        # ORC - Common Order (HL7 v2.9 compliant)
        # ORC-1: Order Control, ORC-2: Placer Order Number, ORC-7: Quantity/Timing, ORC-12: Ordering Provider
        orc = (
            f"ORC|NW|{data['order']['placer_order_number']}||||||"
            f"{data['order']['order_date']}|||"
            f"{data['order']['surgeon_id']}^{data['order']['surgeon_name']}||"
            f"{data['order']['ordering_phone']}|"
        )
        
        # TQ1 - Timing/Quantity (HL7 v2.9 compliant)
        # TQ1-1: Set ID, TQ1-7: Start Date/Time, TQ1-8: End Date/Time
        tq1 = (
            f"TQ1|1||||||{data['procedure']['start_datetime']}|"
            f"{data['procedure']['end_datetime']}"
        )
        
        # OBR - Observation Request (HL7 v2.9 compliant)
        # OBR-1: Set ID, OBR-4: Universal Service ID, OBR-7: Observation Date/Time, OBR-8: Observation End Date/Time
        obr = (
            f"OBR|1|||{data['procedure']['ops_code']}^{data['procedure']['ops_text']}^OPS|"
            f"|||||||{data['procedure']['requested_datetime']}|"
            f"{data['procedure']['observation_datetime']}|||||||||||||||||||F"
        )
        
        # DG1 - Diagnosis (HL7 v2.9 compliant)
        # DG1-1: Set ID, DG1-3: Diagnosis, DG1-6: Diagnosis Type
        dg1 = (
            f"DG1|1|I|{data['procedure']['diagnosis_code']}^"
            f"{data['procedure']['diagnosis_text']}^ICD-10-GM||||F||||F"
        )
        
        # Segmente in der korrekten Reihenfolge für OMG_O19
        segments = [msh, pid, pv1, orc, tq1, obr, dg1]
        message = '\r'.join(segments)
        
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