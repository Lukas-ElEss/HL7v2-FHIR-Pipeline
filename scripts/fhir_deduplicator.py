import requests
import logging

class FHIRDeduplicationClient:
    def __init__(self, base_url: str, device_identifier: str = "Device/v2-to-fhir-pipeline", 
                 username: str = None, password: str = None, ssl_verify: bool = False):
        self.base_url = base_url.rstrip("/")
        self.device_identifier = device_identifier
        self.logger = logging.getLogger(__name__)
        
        # Session für SSL-Konfiguration und Authentifizierung
        self.session = requests.Session()
        self.session.verify = ssl_verify
        
        if username and password:
            self.session.auth = (username, password)
            self.logger.debug(f"Authentication configured for user: {username}")
        
        self.logger.debug(f"FHIR Deduplication Client initialized for: {base_url}")
        self.logger.debug(f"SSL verification: {ssl_verify}")

    def search_resource_by_identifier(self, resource_type: str, system: str, value: str) -> str | None:
        params = {
            "identifier": f"{system}|{value}"
        }
        response = self.session.get(f"{self.base_url}/{resource_type}", params=params)
        response.raise_for_status()
        bundle = response.json()
        entries = bundle.get("entry", [])
        if entries:
            return entries[0]["resource"]["id"]
        return None

    def search_existing_provenance(self, patient_id: str, encounter_id: str) -> list[dict]:
        # Use correct FHIR search parameters for Provenance
        # Search for Provenance resources that target both Patient and Encounter
        params = [
            ("target", f"Patient/{patient_id}"),
            ("target", f"Encounter/{encounter_id}")
        ]
        response = self.session.get(f"{self.base_url}/Provenance", params=params)
        response.raise_for_status()
        bundle = response.json()
        return bundle.get("entry", [])

    def delete_resources_from_provenance(self, provenance_entries: list[dict]) -> None:
        for entry in provenance_entries:
            prov_resource = entry["resource"]
            for target in prov_resource.get("target", []):
                ref = target.get("reference")
                if ref:
                    delete_url = f"{self.base_url}/{ref}"
                    self.logger.info(f"Deleting {delete_url} ...")
                    res = self.session.delete(delete_url)
                    res.raise_for_status()
            prov_id = prov_resource.get("id")
            if prov_id:
                del_url = f"{self.base_url}/Provenance/{prov_id}"
                self.logger.info(f"Deleting Provenance {del_url} ...")
                res = self.session.delete(del_url)
                res.raise_for_status()