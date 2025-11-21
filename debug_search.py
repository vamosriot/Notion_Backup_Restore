#!/usr/bin/env python3
"""
Debug script to see what Notion API returns when searching for databases.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.utils.api_client import create_notion_client

def main():
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("❌ NOTION_TOKEN not found in .env")
        return
    
    print(f"✅ Using token: {token[:20]}...")
    print()
    
    # Create API client
    api_client = create_notion_client(auth=token, requests_per_second=2.5)
    
    # Test databases to search
    databases = ["Documentation", "Tasks", "Notes", "Sprints"]
    
    for db_name in databases:
        print(f"🔍 Searching for: {db_name}")
        print("-" * 60)
        
        try:
            # Search without filter (our current approach)
            results = api_client.search(query=db_name)
            
            print(f"   Results found: {len(results.get('results', []))}")
            
            for i, result in enumerate(results.get('results', []), 1):
                obj_type = result.get('object')
                result_id = result.get('id', 'N/A')
                
                # Get title
                if obj_type == 'database':
                    title_prop = result.get('title', [])
                    title = "".join([t.get('plain_text', '') for t in title_prop])
                elif obj_type == 'page':
                    # Try to get page title
                    props = result.get('properties', {})
                    title_prop = props.get('title', {})
                    if isinstance(title_prop, dict):
                        title_content = title_prop.get('title', [])
                        title = "".join([t.get('plain_text', '') for t in title_content])
                    else:
                        title = "N/A"
                else:
                    title = "N/A"
                
                print(f"   [{i}] Type: {obj_type}")
                print(f"       Title: {title}")
                print(f"       ID: {result_id}")
                
                # Check if it's a database
                if obj_type == 'database':
                    print(f"       ✅ This is a DATABASE!")
                    if title.strip().lower() == db_name.lower():
                        print(f"       ✅✅ EXACT MATCH!")
                
                print()
            
            if not results.get('results'):
                print("   ⚠️  No results found!")
                print()
                print("   💡 This means:")
                print("      1. The database doesn't exist, OR")
                print("      2. The database is NOT shared with your integration")
                print()
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        print()
    
    print("=" * 60)
    print("🔧 TROUBLESHOOTING STEPS:")
    print()
    print("If no databases were found:")
    print()
    print("1. Go to Notion and open each database:")
    print("   - Documentation")
    print("   - Tasks")
    print("   - Notes")
    print("   - Sprints")
    print()
    print("2. For EACH database, click '...' (top right)")
    print()
    print("3. Click 'Add connections' or 'Connections'")
    print()
    print("4. Find and select your integration")
    print()
    print("5. Grant access")
    print()
    print("6. Run this script again")
    print()
    print("=" * 60)

if __name__ == "__main__":
    main()

