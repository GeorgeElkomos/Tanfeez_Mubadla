#!/usr/bin/env python
"""
Script to safely handle the problematic table with quoted identifiers
"""

import os
import sys
import django
from django.db import connection, transaction

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'budget_transfer.settings')
django.setup()

def handle_problematic_table_safely():
    """Handle the _XX_ADJD_ACCOUNT_LIMIT_GLOBAL table with proper quoting"""
    table_name = '_XX_ADJD_ACCOUNT_LIMIT_GLOBAL'
    quoted_table = f'"{table_name}"'  # Use quoted identifier
    
    try:
        with connection.cursor() as cursor:
            print(f"Analyzing table: {table_name}")
            
            # 1. Check if table exists using quoted name
            cursor.execute("""
                SELECT COUNT(*) 
                FROM user_tables 
                WHERE table_name = %s
            """, [table_name])
            
            if cursor.fetchone()[0] == 0:
                print(f"Table {table_name} does not exist!")
                return
            
            print("Table exists. Attempting to get row count...")
            
            # 2. Get row count using quoted identifier
            try:
                cursor.execute(f'SELECT COUNT(*) FROM {quoted_table}')
                row_count = cursor.fetchone()[0]
                print(f"Row count: {row_count}")
            except Exception as e:
                print(f"Could not get row count: {e}")
                row_count = 0
            
            # 3. Try deletion strategies with quoted identifiers
            print("\nAttempting deletion strategies:")
            
            # Strategy 1: Simple DROP with CASCADE CONSTRAINTS
            print("\nStrategy 1: DROP with CASCADE CONSTRAINTS")
            try:
                with transaction.atomic():
                    cursor.execute(f'DROP TABLE {quoted_table} CASCADE CONSTRAINTS')
                    print(f"✓ Successfully dropped table {table_name}")
                    return
            except Exception as e:
                print(f"✗ Strategy 1 failed: {e}")
            
            # Strategy 2: TRUNCATE then DROP
            print("\nStrategy 2: TRUNCATE then DROP")
            try:
                with transaction.atomic():
                    if row_count > 0:
                        cursor.execute(f'TRUNCATE TABLE {quoted_table}')
                        print("Data truncated")
                    
                    cursor.execute(f'DROP TABLE {quoted_table} CASCADE CONSTRAINTS')
                    print(f"✓ Successfully dropped table {table_name}")
                    return
            except Exception as e:
                print(f"✗ Strategy 2 failed: {e}")
            
            # Strategy 3: DELETE then DROP
            print("\nStrategy 3: DELETE then DROP")
            try:
                with transaction.atomic():
                    if row_count > 0:
                        cursor.execute(f'DELETE FROM {quoted_table}')
                        print("Data deleted")
                    
                    cursor.execute(f'DROP TABLE {quoted_table} CASCADE CONSTRAINTS')
                    print(f"✓ Successfully dropped table {table_name}")
                    return
            except Exception as e:
                print(f"✗ Strategy 3 failed: {e}")
            
            # Strategy 4: Get detailed error information
            print("\nStrategy 4: Detailed error analysis")
            try:
                # Check for any dependencies
                cursor.execute("""
                    SELECT constraint_name, constraint_type, r_constraint_name
                    FROM user_constraints
                    WHERE table_name = %s
                    OR r_constraint_name IN (
                        SELECT constraint_name 
                        FROM user_constraints 
                        WHERE table_name = %s
                    )
                """, [table_name, table_name])
                
                dependencies = cursor.fetchall()
                if dependencies:
                    print("Found dependencies:")
                    for dep in dependencies:
                        print(f"  - {dep}")
                else:
                    print("No dependencies found")
                
                # Try to get the exact DDL to understand the table structure
                cursor.execute("""
                    SELECT column_name, data_type, nullable
                    FROM user_tab_columns
                    WHERE table_name = %s
                    ORDER BY column_id
                """, [table_name])
                
                columns = cursor.fetchall()
                print(f"Table has {len(columns)} columns:")
                for col_name, data_type, nullable in columns:
                    print(f"  - {col_name}: {data_type} ({'NULL' if nullable == 'Y' else 'NOT NULL'})")
                
            except Exception as e:
                print(f"Error in detailed analysis: {e}")
            
            # Strategy 5: Use PURGE option
            print("\nStrategy 5: DROP with PURGE")
            try:
                with transaction.atomic():
                    cursor.execute(f'DROP TABLE {quoted_table} CASCADE CONSTRAINTS PURGE')
                    print(f"✓ Successfully dropped table {table_name} with PURGE")
                    return
            except Exception as e:
                print(f"✗ Strategy 5 failed: {e}")
            
            print(f"\n❌ All strategies failed. Manual intervention may be required.")
            print(f"You can try connecting to the database directly and running:")
            print(f'   DROP TABLE "{table_name}" CASCADE CONSTRAINTS PURGE;')
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    handle_problematic_table_safely()
