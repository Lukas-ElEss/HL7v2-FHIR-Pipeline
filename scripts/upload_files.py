#!/usr/bin/env python3
"""
Simple script to upload all files to Matchbox server
"""

from matchbox_client import MatchboxClient

def main():
    """Upload all project files to Matchbox server"""
    print("Starting file upload to Matchbox server...")
    
    client = MatchboxClient()
    
    # Test connection first
    if client.test_connection()['success']:
        print("Connection successful, uploading files...")
        
        # Upload all files
        result = client.upload_all_files()
        
        # Debug: Show the actual result structure
        print(f"\nDEBUG - Result structure: {result}")
        print(f"DEBUG - Result keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        
        # Show detailed results
        print("\n" + "=" * 60)
        print("UPLOAD DETAILED RESULTS:")
        print("=" * 60)
        
        if result.get('success', False):
            print("✅ All files uploaded successfully!")
        else:
            print(f"❌ Upload failed")
        
        # Show individual file results if available
        if 'upload_results' in result:
            print(f"\nTotal files: {len(result['upload_results'])}")
            
            successful_count = sum(1 for r in result['upload_results'] if r['result']['success'])
            print(f"Successful: {successful_count}")
            print(f"Failed: {len(result['upload_results']) - successful_count}")
            
            print("\nIndividual file results:")
            for file_result in result['upload_results']:
                status = "✅" if file_result['result']['success'] else "❌"
                print(f"{status} {file_result['type']}: {file_result['file']}")
                
                if not file_result['result']['success']:
                    error = file_result['result'].get('error', 'Unknown error')
                    print(f"   Error: {error}")
        else:
            print("No upload results available")
        
        print("=" * 60)
        
    else:
        print("Cannot connect to Matchbox server")

if __name__ == "__main__":
    main()
