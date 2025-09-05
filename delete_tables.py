#!/usr/bin/env python
"""
Script to delete specified tables from Oracle database
This script will delete all specified tables and their data permanently.
USE WITH CAUTION - This action cannot be undone!
"""

import os
import sys
import django
from django.conf import settings
from django.db import connection, transaction
import logging

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'budget_transfer.settings')
django.setup()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('table_deletion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# List of tables to delete
TABLES_TO_DELETE = [
    'AUTH_GROUP',
    'AUTH_GROUP_PERMISSIONS',
    'AUTH_PERMISSION',
    'DJANGO_ADMIN_LOG',
    'DJANGO_CONTENT_TYPE',
    'DJANGO_MIGRATIONS',
    'DJANGO_SESSION',
    'MAIN_CURRENCY',
    'MAIN_ROUTES_NAME',
    'XX_ACCOUNT_ENTITY_LIMIT_XX',
    'XX_ACCOUNT_XX',
    'XX_BUDGET_TRANSFER_ATTACHM9ED3',
    'XX_BUDGET_TRANSFER_REJECT_1499',
    'XX_BUDGET_TRANSFER_XX',
    'XX_DASHBOARD_BUDGET_TRANSFD593',
    'XX_ENTITY_XX',
    'XX_NOTIFICATION_XX',
    'XX_PIVOTFUND_XX',
    'XX_TRANSACTION_AUDIT_XX',
    'XX_TRANSACTION_TRANSFER_XX',
    'XX_USER_ABILITY_XX',
    'XX_USER_LEVEL_XX',
    'XX_USER_XX',
    'XX_USER_XX_GROUPS',
    'XX_USER_XX_USER_PERMISSIONS',
    '_XX_ADJD_ACCOUNT_LIMIT_GLOBAL',
    'APPROVAL_ACTION',
    'APPROVAL_ASSIGNMENT',
    'APPROVAL_DELEGATION',
    'APPROVAL_WORKFLOW_INSTANCE',
    'APPROVAL_WORKFLOW_STAGE_IN52C9',
    'APPROVAL_WORKFLOW_STAGE_TE69F8',
    'APPROVAL_WORKFLOW_TEMPLATE',
]

def check_table_exists(cursor, table_name):
    """Check if a table exists in the database"""
    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_tables 
            WHERE table_name = :table_name
        """, {'table_name': table_name.upper()})
        
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {e}")
        return False

def get_table_row_count(cursor, table_name):
    """Get the number of rows in a table"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.warning(f"Could not get row count for {table_name}: {e}")
        return 0

def disable_foreign_key_constraints(cursor):
    """Disable all foreign key constraints to avoid constraint violations during deletion"""
    try:
        logger.info("Disabling foreign key constraints...")
        
        # Get all foreign key constraints
        cursor.execute("""
            SELECT constraint_name, table_name 
            FROM user_constraints 
            WHERE constraint_type = 'R' 
            AND status = 'ENABLED'
        """)
        
        constraints = cursor.fetchall()
        disabled_constraints = []
        
        for constraint_name, table_name in constraints:
            try:
                cursor.execute(f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name}")
                disabled_constraints.append((constraint_name, table_name))
                logger.info(f"Disabled constraint {constraint_name} on table {table_name}")
            except Exception as e:
                logger.warning(f"Could not disable constraint {constraint_name}: {e}")
        
        return disabled_constraints
        
    except Exception as e:
        logger.error(f"Error disabling foreign key constraints: {e}")
        return []

def enable_foreign_key_constraints(cursor, constraints):
    """Re-enable foreign key constraints"""
    try:
        logger.info("Re-enabling foreign key constraints...")
        
        for constraint_name, table_name in constraints:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ENABLE CONSTRAINT {constraint_name}")
                logger.info(f"Enabled constraint {constraint_name} on table {table_name}")
            except Exception as e:
                logger.warning(f"Could not enable constraint {constraint_name}: {e}")
                
    except Exception as e:
        logger.error(f"Error enabling foreign key constraints: {e}")

def delete_table_data(cursor, table_name):
    """Delete all data from a table"""
    try:
        # Get row count before deletion
        row_count = get_table_row_count(cursor, table_name)
        
        if row_count == 0:
            logger.info(f"Table {table_name} is already empty")
            return True
        
        logger.info(f"Deleting {row_count} rows from table {table_name}...")
        
        # Use TRUNCATE for better performance if possible, otherwise use DELETE
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            logger.info(f"Successfully truncated table {table_name}")
        except Exception as truncate_error:
            logger.warning(f"TRUNCATE failed for {table_name}, trying DELETE: {truncate_error}")
            cursor.execute(f"DELETE FROM {table_name}")
            logger.info(f"Successfully deleted all rows from table {table_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error deleting data from table {table_name}: {e}")
        return False

def drop_table(cursor, table_name):
    """Drop a table completely"""
    try:
        logger.info(f"Dropping table {table_name}...")
        cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
        logger.info(f"Successfully dropped table {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error dropping table {table_name}: {e}")
        return False

def main():
    """Main function to execute table deletion"""
    logger.info("Starting table deletion process...")
    logger.info(f"Total tables to process: {len(TABLES_TO_DELETE)}")
    
    # Confirmation prompt
    print("\n" + "="*60)
    print("WARNING: This script will permanently delete the following tables:")
    print("="*60)
    for i, table in enumerate(TABLES_TO_DELETE, 1):
        print(f"{i:2d}. {table}")
    
    print("\n" + "="*60)
    print("This action CANNOT be undone!")
    print("Make sure you have a backup before proceeding.")
    print("="*60)
    
    confirm = input("\nDo you want to continue? Type 'DELETE_TABLES' to confirm: ")
    
    if confirm != 'DELETE_TABLES':
        logger.info("Operation cancelled by user")
        print("Operation cancelled.")
        return
    
    # Choose deletion mode
    print("\nChoose deletion mode:")
    print("1. Delete data only (keep table structure)")
    print("2. Drop tables completely (delete structure and data)")
    
    while True:
        mode = input("Enter your choice (1 or 2): ").strip()
        if mode in ['1', '2']:
            break
        print("Invalid choice. Please enter 1 or 2.")
    
    delete_data_only = (mode == '1')
    
    try:
        with connection.cursor() as cursor:
            # Check which tables exist
            existing_tables = []
            missing_tables = []
            
            logger.info("Checking which tables exist...")
            for table_name in TABLES_TO_DELETE:
                if check_table_exists(cursor, table_name):
                    existing_tables.append(table_name)
                    row_count = get_table_row_count(cursor, table_name)
                    logger.info(f"Table {table_name} exists with {row_count} rows")
                else:
                    missing_tables.append(table_name)
                    logger.warning(f"Table {table_name} does not exist")
            
            if missing_tables:
                logger.info(f"Tables not found: {', '.join(missing_tables)}")
            
            if not existing_tables:
                logger.info("No tables found to delete")
                return
            
            # Disable foreign key constraints if dropping tables
            disabled_constraints = []
            if not delete_data_only:
                disabled_constraints = disable_foreign_key_constraints(cursor)
            
            # Process each existing table
            successful_operations = []
            failed_operations = []
            
            with transaction.atomic():
                for table_name in existing_tables:
                    logger.info(f"Processing table: {table_name}")
                    
                    if delete_data_only:
                        # Delete data only
                        if delete_table_data(cursor, table_name):
                            successful_operations.append(f"Data deleted from {table_name}")
                        else:
                            failed_operations.append(f"Failed to delete data from {table_name}")
                    else:
                        # Drop table completely
                        if drop_table(cursor, table_name):
                            successful_operations.append(f"Table {table_name} dropped")
                        else:
                            failed_operations.append(f"Failed to drop table {table_name}")
            
            # Re-enable foreign key constraints if they were disabled
            if disabled_constraints:
                enable_foreign_key_constraints(cursor, disabled_constraints)
    
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        print(f"Error: {e}")
        return
    
    # Print summary
    print("\n" + "="*60)
    print("OPERATION SUMMARY")
    print("="*60)
    
    if successful_operations:
        print(f"\nSuccessful operations ({len(successful_operations)}):")
        for operation in successful_operations:
            print(f"  ✓ {operation}")
    
    if failed_operations:
        print(f"\nFailed operations ({len(failed_operations)}):")
        for operation in failed_operations:
            print(f"  ✗ {operation}")
    
    if missing_tables:
        print(f"\nTables not found ({len(missing_tables)}):")
        for table in missing_tables:
            print(f"  - {table}")
    
    print(f"\nTotal processed: {len(successful_operations)}/{len(existing_tables)} existing tables")
    print("="*60)
    
    logger.info("Table deletion process completed")
    print(f"\nDetailed log saved to: table_deletion.log")

if __name__ == "__main__":
    main()
