#!/usr/bin/env python
"""
Script to specifically handle the _XX_ADJD_ACCOUNT_LIMIT_GLOBAL table
"""

import os
import sys
import django
from django.db import connection, transaction

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'budget_transfer.settings')
django.setup()

def handle_problematic_table():
    """Handle the _XX_ADJD_ACCOUNT_LIMIT_GLOBAL table specifically"""
    table_name = '_XX_ADJD_ACCOUNT_LIMIT_GLOBAL'
    
    try:
        with connection.cursor() as cursor:
            print(f"Analyzing table: {table_name}")
            
            # 1. Check if table exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM user_tables 
                WHERE table_name = :table_name
            """, {'table_name': table_name})
            
            if cursor.fetchone()[0] == 0:
                print(f"Table {table_name} does not exist!")
                return
            
            # 2. Get table info
            print("\n1. Table Information:")
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            print(f"   Row count: {row_count}")
            
            # 3. Check for foreign key constraints referencing this table
            print("\n2. Foreign Key Constraints pointing TO this table:")
            cursor.execute("""
                SELECT c.constraint_name, c.table_name, c.column_name, c.r_constraint_name
                FROM user_cons_columns c
                JOIN user_constraints r ON c.r_constraint_name = r.constraint_name
                WHERE r.table_name = :table_name
                AND c.constraint_name IN (
                    SELECT constraint_name FROM user_constraints WHERE constraint_type = 'R'
                )
            """, {'table_name': table_name})
            
            incoming_fks = cursor.fetchall()
            if incoming_fks:
                print("   Found foreign keys pointing to this table:")
                for fk_name, fk_table, fk_column, ref_constraint in incoming_fks:
                    print(f"   - {fk_table}.{fk_column} ({fk_name}) -> {ref_constraint}")
            else:
                print("   No foreign keys pointing to this table")
            
            # 4. Check for foreign key constraints FROM this table
            print("\n3. Foreign Key Constraints FROM this table:")
            cursor.execute("""
                SELECT constraint_name, column_name, r_constraint_name
                FROM user_cons_columns
                WHERE table_name = :table_name
                AND constraint_name IN (
                    SELECT constraint_name FROM user_constraints 
                    WHERE constraint_type = 'R' AND table_name = :table_name
                )
            """, {'table_name': table_name})
            
            outgoing_fks = cursor.fetchall()
            if outgoing_fks:
                print("   Found foreign keys from this table:")
                for fk_name, fk_column, ref_constraint in outgoing_fks:
                    print(f"   - {fk_column} ({fk_name}) -> {ref_constraint}")
            else:
                print("   No foreign keys from this table")
            
            # 5. Get all constraints on this table
            print("\n4. All Constraints on this table:")
            cursor.execute("""
                SELECT constraint_name, constraint_type, status
                FROM user_constraints
                WHERE table_name = :table_name
            """, {'table_name': table_name})
            
            constraints = cursor.fetchall()
            constraint_types = {'P': 'Primary Key', 'R': 'Foreign Key', 'U': 'Unique', 'C': 'Check'}
            
            for constraint_name, constraint_type, status in constraints:
                type_desc = constraint_types.get(constraint_type, constraint_type)
                print(f"   - {constraint_name}: {type_desc} ({status})")
            
            # 6. Try different deletion strategies
            print("\n5. Attempting deletion strategies:")
            
            # Strategy 1: Disable all constraints first
            print("\n   Strategy 1: Disable constraints and drop table")
            try:
                with transaction.atomic():
                    # Disable all constraints on this table
                    for constraint_name, constraint_type, status in constraints:
                        if status == 'ENABLED':
                            try:
                                cursor.execute(f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name}")
                                print(f"   Disabled constraint: {constraint_name}")
                            except Exception as e:
                                print(f"   Could not disable {constraint_name}: {e}")
                    
                    # Disable incoming foreign keys
                    for fk_name, fk_table, fk_column, ref_constraint in incoming_fks:
                        try:
                            cursor.execute(f"ALTER TABLE {fk_table} DISABLE CONSTRAINT {fk_name}")
                            print(f"   Disabled incoming FK: {fk_name} from {fk_table}")
                        except Exception as e:
                            print(f"   Could not disable incoming FK {fk_name}: {e}")
                    
                    # Now try to drop the table
                    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                    print(f"   ✓ Successfully dropped table {table_name}")
                    return
                    
            except Exception as e:
                print(f"   ✗ Strategy 1 failed: {e}")
            
            # Strategy 2: Just drop with CASCADE CONSTRAINTS
            print("\n   Strategy 2: Direct drop with CASCADE CONSTRAINTS")
            try:
                with transaction.atomic():
                    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                    print(f"   ✓ Successfully dropped table {table_name}")
                    return
            except Exception as e:
                print(f"   ✗ Strategy 2 failed: {e}")
            
            # Strategy 3: Delete data first, then drop
            print("\n   Strategy 3: Delete data first, then drop structure")
            try:
                with transaction.atomic():
                    if row_count > 0:
                        cursor.execute(f"DELETE FROM {table_name}")
                        print(f"   Deleted {row_count} rows")
                    
                    cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                    print(f"   ✓ Successfully dropped table {table_name}")
                    return
            except Exception as e:
                print(f"   ✗ Strategy 3 failed: {e}")
            
            # Strategy 4: Manual constraint removal
            print("\n   Strategy 4: Manual constraint removal")
            try:
                with transaction.atomic():
                    # Drop all constraints manually
                    for constraint_name, constraint_type, status in constraints:
                        try:
                            cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}")
                            print(f"   Dropped constraint: {constraint_name}")
                        except Exception as e:
                            print(f"   Could not drop constraint {constraint_name}: {e}")
                    
                    # Drop incoming foreign keys
                    for fk_name, fk_table, fk_column, ref_constraint in incoming_fks:
                        try:
                            cursor.execute(f"ALTER TABLE {fk_table} DROP CONSTRAINT {fk_name}")
                            print(f"   Dropped incoming FK: {fk_name} from {fk_table}")
                        except Exception as e:
                            print(f"   Could not drop incoming FK {fk_name}: {e}")
                    
                    # Now drop the table
                    cursor.execute(f"DROP TABLE {table_name}")
                    print(f"   ✓ Successfully dropped table {table_name}")
                    return
                    
            except Exception as e:
                print(f"   ✗ Strategy 4 failed: {e}")
            
            print(f"\n❌ All strategies failed. Table {table_name} could not be dropped.")
            print("   You may need to manually investigate and remove this table.")
            
    except Exception as e:
        print(f"Error analyzing table: {e}")

if __name__ == "__main__":
    handle_problematic_table()
