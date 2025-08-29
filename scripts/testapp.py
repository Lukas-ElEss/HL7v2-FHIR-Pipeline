#!/usr/bin/env python3
"""
Test Application for v2-to-fhir-pipeline

This script tests the complete pipeline by:
1. Generating a test HL7 message using the simulator
2. Sending it to the pipeline server via MLLP
3. Verifying the server response
"""

import socket
from hl7_simulator import OMG_O19_Generator


class PipelineTester:
    """Test class for the complete v2-to-fhir pipeline"""
    
    def __init__(self):
        """Initialize the pipeline tester"""
        self.hl7_generator = OMG_O19_Generator(enable_validation=True)
        self.server_host = "localhost"
        self.server_port = 2100  # MLLP port from pipeline_server.py
        
    def generate_test_message(self) -> str:
        """Generate a test HL7 message using the simulator"""
        print("Generating test HL7 message...")
        
        # Generate dummy data
        dummy_data = self.hl7_generator.generate_dummy_data()
        print(f"Generated patient data for: {dummy_data['patient']['first_name']} {dummy_data['patient']['last_name']}")
        
        # Create HL7 message
        hl7_message = self.hl7_generator.create_omg_o19_message(dummy_data)
        print(f"Generated HL7 message (length: {len(hl7_message)} characters)")
        
        return hl7_message
    
    def send_mllp_message(self, hl7_message: str) -> dict:
        """Send HL7 message to pipeline server using MLLP protocol"""
        print(f"Sending HL7 message to {self.server_host}:{self.server_port}")
        
        try:
            # Create socket connection
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(30)  # 30 second timeout
                sock.connect((self.server_host, self.server_port))
                
                # Create MLLP message with framing
                mllp_message = self._create_mllp_message(hl7_message)
                
                # Send message
                sock.send(mllp_message)
                print("Message sent, waiting for response...")
                
                # Receive response
                response = sock.recv(1024)
                response_text = response.decode('utf-8', errors='ignore')
                
                # Parse MLLP response
                ack_nack = self._parse_mllp_response(response_text)
                
                return {
                    "success": True,
                    "response": ack_nack,
                    "raw_response": response_text
                }
                
        except socket.timeout:
            return {
                "success": False,
                "error": "Connection timeout"
            }
        except ConnectionRefusedError:
            return {
                "success": False,
                "error": "Connection refused - server not running"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Socket error: {e}"
            }
    
    def _create_mllp_message(self, hl7_message: str) -> bytes:
        """Create MLLP message with proper framing"""
        # MLLP framing: VT (0x0B) at start, FS (0x1C) at end
        return b'\x0B' + hl7_message.encode('utf-8') + b'\x1C'
    
    def _parse_mllp_response(self, response: str) -> str:
        """Parse MLLP response to determine ACK/NACK"""
        # Remove MLLP framing
        if response.startswith('\x0B') and response.endswith('\x1C'):
            response_content = response[1:-1]
            
            if response_content.startswith('MSA|AA'):
                return "ACK - Message accepted"
            elif response_content.startswith('MSA|AE'):
                return "NACK - Message rejected"
            else:
                return f"Unknown response: {response_content}"
        else:
            return f"Invalid MLLP response: {response}"
    
    def run_complete_test(self):
        """Run the complete test workflow"""
        print("=" * 60)
        print("Starting v2-to-fhir Pipeline Test")
        print("=" * 60)
        
        # Step 1: Generate test message
        hl7_message = self.generate_test_message()
        print(f"\nGenerated HL7 Message:\n{hl7_message}")
        
        # Step 2: Send to pipeline server
        print("\n" + "-" * 40)
        print("Testing with pipeline server...")
        
        # Send message to server
        server_result = self.send_mllp_message(hl7_message)
        
        if server_result["success"]:
            print(f"Server test: SUCCESS")
            print(f"Response: {server_result['response']}")
            print(f"Raw response: {server_result['raw_response']}")
        else:
            print(f"Server test: FAILED")
            print(f"Error: {server_result['error']}")
            print("\nNote: Make sure the pipeline server is running:")
            print("python3 scripts/pipeline_server.py")
        
        print("\n" + "=" * 60)
        print("Test completed")
        print("=" * 60)


def main():
    """Main function"""
    tester = PipelineTester()
    tester.run_complete_test()


if __name__ == "__main__":
    main()
