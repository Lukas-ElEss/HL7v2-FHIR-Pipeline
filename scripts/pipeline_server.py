#!/usr/bin/env python3
"""
Pipeline Server for v2-to-fhir-pipeline

This server continuously listens for MLLP/TCP messages from Orchestra
and processes them through the complete transformation pipeline.
"""

import asyncio
import json
import logging
import signal
import sys
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from config import get_config
from fhir_mapper import FHIRMapper


class PipelineServer:
    """Server that continuously processes HL7 messages through the transformation pipeline"""
    
    def __init__(self, config=None):
        """
        Initialize Pipeline Server
        
        Args:
            config: FHIR configuration object. If None, loads default config.
        """
        if config is None:
            config = get_config()
        
        self.config = config
        self.fhir_mapper = FHIRMapper()
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config.get_log_level()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Pipeline Server initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        if self.running:  # Only handle signal once
            self.logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False
            # Cancel the server task to break out of serve_forever()
            if hasattr(self, 'server_task') and self.server_task and not self.server_task.done():
                self.server_task.cancel()
    
    async def process_hl7_message(self, hl7_message: str) -> Dict[str, Any]:
        """
        Process a single HL7 message through the complete pipeline
        
        Args:
            hl7_message: Raw HL7 message
            
        Returns:
            Dict with processing result
        """
        try:
            self.logger.info(f"Processing HL7 message (length: {len(hl7_message)})")
            
            # Execute complete transformation pipeline
            result = self.fhir_mapper.complete_transformation_pipeline(hl7_message)
            
            # Debug: Show the InfoWashSource data from the result
            if result.get("success") and "infowash_source" in result:
                self.logger.info("InfoWashSource data that was processed:")
                self.logger.info(json.dumps(result["infowash_source"], indent=2))
            
            if result["success"]:
                self.processed_count += 1
                self.logger.info(f"✅ Pipeline successful (processed: {self.processed_count})")
                
                # Log deduplication result if available
                if "deduplication_result" in result:
                    dedup_result = result["deduplication_result"]
                    if dedup_result.get("success"):
                        self.logger.info(f"✅ Deduplication: {dedup_result.get('message', 'unknown')}")
                    else:
                        self.logger.warning(f"⚠️ Deduplication warning: {dedup_result.get('message', 'unknown')}")
                
                # Log send result if available
                if "send_result" in result:
                    send_result = result["send_result"]
                    if send_result.get("success"):
                        self.logger.info(f"✅ Bundle sent to server: {send_result.get('message', 'unknown')}")
                    else:
                        self.logger.error(f"❌ Send failed: {send_result.get('message', 'unknown')}")
                
                # Return successful result
                return {
                    "success": True,
                    "pipeline_result": result,
                    "message": "Complete pipeline successful"
                }
            else:
                self.error_count += 1
                self.logger.error(f"❌ Pipeline failed: {result['error']}")
                return result
                
        except Exception as e:
            self.error_count += 1
            error_msg = f"Processing error: {e}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "message": error_msg
            }
    
    async def handle_mllp_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handle MLLP connection from Orchestra
        
        Args:
            reader: Stream reader for incoming data
            writer: Stream writer for responses
        """
        client_addr = writer.get_extra_info('peername')
        self.logger.info(f"New MLLP connection from {client_addr}")
        
        try:
            # Read MLLP message
            data = await reader.read(8192)  # Read up to 8KB
            
            if data:
                # Convert bytes to string and handle MLLP framing
                hl7_message = self._extract_hl7_from_mllp(data)
                
                if hl7_message:
                    self.logger.info(f"Received HL7 message from {client_addr}")
                    
                    # Process the message
                    result = await self.process_hl7_message(hl7_message)
                    
                    # Send ACK/NACK response
                    if result["success"]:
                        # Create response with bundle data
                        response = self._create_mllp_response_with_bundle(result)
                        self.logger.info(f"Sending response with bundle to {client_addr}")
                    else:
                        response = self._create_mllp_nack(result.get("error", "Unknown error"))
                        self.logger.error(f"Sending NACK to {client_addr}: {result.get('error')}")
                    
                    writer.write(response)
                    await writer.drain()
                else:
                    self.logger.warning(f"Invalid MLLP message from {client_addr}")
                    writer.write(self._create_mllp_nack("Invalid message format"))
                    await writer.drain()
            
        except Exception as e:
            self.logger.error(f"Error handling MLLP connection from {client_addr}: {e}")
            try:
                writer.write(self._create_mllp_nack(str(e)))
                await writer.drain()
            except:
                pass
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Connection from {client_addr} closed")
    
    def _extract_hl7_from_mllp(self, data: bytes) -> Optional[str]:
        """
        Extract HL7 message from MLLP framing
        
        Args:
            data: Raw MLLP data
            
        Returns:
            Extracted HL7 message or None if invalid
        """
        try:
            # MLLP framing: VT (0x0B) at start, FS (0x1C) at end
            message = data.decode('utf-8', errors='ignore')
            
            # Remove MLLP framing characters
            if message.startswith('\x0B') and message.endswith('\x1C'):
                hl7_message = message[1:-1]  # Remove VT and FS
                return hl7_message
            else:
                self.logger.warning("Invalid MLLP framing")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting HL7 from MLLP: {e}")
            return None
    
    def _create_mllp_ack(self) -> bytes:
        """Create MLLP ACK response"""
        # MLLP ACK: VT + ACK + FS
        ack_message = "MSA|AA|MSG00001"  # Simple ACK
        return b'\x0B' + ack_message.encode('utf-8') + b'\x1C'
    
    def _create_mllp_nack(self, error_message: str) -> bytes:
        """Create MLLP NACK response"""
        # MLLP NACK: VT + NACK + FS
        nack_message = f"MSA|AE|MSG00001|{error_message}"
        return b'\x0B' + nack_message.encode('utf-8') + b'\x1C'
    
    def _create_mllp_response_with_bundle(self, result: Dict[str, Any]) -> bytes:
        """Create MLLP response that includes the FHIR Bundle data"""
        try:
            import json
            
            # Create response message with bundle
            if "bundle" in result:
                bundle_data = result["bundle"]
                bundle_json = json.dumps(bundle_data, ensure_ascii=False)
                
                # Create response message: ACK + Bundle data
                response_message = f"MSA|AA|MSG00001|Bundle created successfully\r{json.dumps(bundle_data, indent=2)}"
                
                # MLLP framing: VT + Response + FS
                return b'\x0B' + response_message.encode('utf-8') + b'\x1C'
            else:
                # Fallback to simple ACK if no bundle
                return self._create_mllp_ack()
                
        except Exception as e:
            self.logger.error(f"Error creating bundle response: {e}")
            # Fallback to simple ACK
            return self._create_mllp_ack()
    
    async def start_server(self, host: str = "0.0.0.0", port: int = 2100):
        """
        Start the MLLP server
        
        Args:
            host: Host to bind to
            port: Port to listen on
        """
        try:
            self.logger.info(f"Starting MLLP server on {host}:{port}")
            
            # Create server
            self.server = await asyncio.start_server(
                self.handle_mllp_connection,
                host,
                port
            )
            
            self.running = True
            self.logger.info(f"MLLP server started successfully on {host}:{port}")
            self.logger.info("Press Ctrl+C to stop the server")
            
            # Keep server running until shutdown signal
            try:
                async with self.server:
                    await self.server.serve_forever()
            except asyncio.CancelledError:
                self.logger.info("Server task cancelled, shutting down...")
                raise
                
        except Exception as e:
            self.logger.error(f"Failed to start server: {e}")
            sys.exit(1)
    
    async def stop_server(self):
        """Stop the MLLP server gracefully"""
        if hasattr(self, 'server') and self.server:
            self.logger.info("Stopping MLLP server...")
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("MLLP server stopped")
        self.running = False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            "processed_messages": self.processed_count,
            "error_count": self.error_count,
            "success_rate": (self.processed_count / (self.processed_count + self.error_count)) * 100 if (self.processed_count + self.error_count) > 0 else 0,
            "uptime": "TODO: Implement uptime tracking",
            "last_message": "TODO: Implement last message tracking"
        }
    
    def print_statistics(self):
        """Print current server statistics"""
        stats = self.get_statistics()
        print("\n" + "=" * 50)
        print("Pipeline Server Statistics")
        print("=" * 50)
        print(f"Processed Messages: {stats['processed_messages']}")
        print(f"Error Count: {stats['error_count']}")
        print(f"Success Rate: {stats['success_rate']:.1f}%")
        print(f"Uptime: {stats['uptime']}")
        print(f"Last Message: {stats['last_message']}")
        print("=" * 50)


async def main():
    """Main function"""
    print("Starting v2-to-fhir Pipeline Server")
    print("=" * 50)
    
    server = None
    server_task = None
    
    try:
        # Load configuration
        config = get_config()
        
        # Create and start server
        server = PipelineServer(config)
        
        # Start server in background
        server_task = asyncio.create_task(
            server.start_server(port=2100)  # Standard MLLP port
        )
        
        # Store server_task reference for signal handler
        server.server_task = server_task
        
        # Print statistics periodically
        while server.running:
            try:
                await asyncio.sleep(5)  # Every 5 seconds for testing
                if server.running:  # Check if still running after sleep
                    server.print_statistics()
            except asyncio.CancelledError:
                break
        
        # Wait for server to finish
        await server_task
        
    except KeyboardInterrupt:
        print("\nShutdown requested by user (Ctrl+C)")
        if server:
            server.running = False
            if server_task:
                server_task.cancel()
            await server.stop_server()
    except asyncio.CancelledError:
        print("\nServer task cancelled")
        if server:
            await server.stop_server()
    except Exception as e:
        print(f"Server error: {e}")
        if server:
            await server.stop_server()
        sys.exit(1)
    finally:
        print("Pipeline Server stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
