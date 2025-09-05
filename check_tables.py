#!/usr/bin/env python
"""
Script to check existing tables in Oracle database
"""

import os
import sys
import django
from django.db import connection

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'budget_transfer.settings')
django.setup()

def check_existing_tables():
    """Check what tables exist in the database"""
    tables_to_check = [
        'XX_ADJD_ACCOUNT_LIMIT_GLOBAL',
        '_XX_ADJD_ACCOUNT_LIMIT_GLOBAL'
    ]
    
    try:
        with connection.cursor() as cursor:
            print("Checking for tables...")
            
            # Get all user tables
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            all_tables = [row[0] for row in cursor.fetchall()]
            
            print(f"\nFound {len(all_tables)} total tables in database")
            
            # Check for tables containing 'ADJD' or 'ACCOUNT_LIMIT'
            matching_tables = [t for t in all_tables if 'ADJD' in t or 'ACCOUNT_LIMIT' in t]
            
            if matching_tables:
                print("\nTables containing 'ADJD' or 'ACCOUNT_LIMIT':")
                for table in matching_tables:
                    print(f"  - {table}")
            else:
                print("\nNo tables found containing 'ADJD' or 'ACCOUNT_LIMIT'")
            
            # Check specific tables
            print("\nChecking specific table names:")
            for table_name in tables_to_check:
                exists = table_name in all_tables
                print(f"  {table_name}: {'EXISTS' if exists else 'NOT FOUND'}")
            
            # Check for tables with similar names (fuzzy matching)
            print("\nTables with similar names to XX_ADJD_ACCOUNT_LIMIT_GLOBAL:")
            similar_tables = [t for t in all_tables if any(part in t for part in ['ADJD', 'ACCOUNT', 'LIMIT', 'GLOBAL'])]
            for table in similar_tables:
                print(f"  - {table}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_existing_tables()
