"""
Management command to decrypt and backfill boolean/integer columns on XX_ACCOUNT_ENTITY_LIMIT_XX.

This command safely handles the migration from encrypted boolean/integer fields 
to plain boolean/integer fields by:
1. Reading each row through Django ORM (which handles decryption)
2. Converting the decrypted values to appropriate types
3. Writing them to new columns via raw SQL

Usage: python manage.py backfill_account_entity_limit
"""

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from account_and_entitys.models import XX_ACCOUNT_ENTITY_LIMIT


class Command(BaseCommand):
    help = "Decrypt and backfill boolean/integer columns on XX_ACCOUNT_ENTITY_LIMIT_XX"

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without making changes',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of rows to process in each batch (default: 1000)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be made"))
        
        # Check if we need to add boolean columns for transfer flags
        self._ensure_boolean_columns_exist(dry_run)

        total_rows = XX_ACCOUNT_ENTITY_LIMIT.objects.count()
        self.stdout.write(f"Processing {total_rows} rows in batches of {batch_size}")
        
        updated = 0
        errors = 0
        
        # Process in batches to avoid memory issues with large datasets
        for offset in range(0, total_rows, batch_size):
            batch_updated, batch_errors = self._process_batch(offset, batch_size, dry_run)
            updated += batch_updated
            errors += batch_errors
            
            self.stdout.write(
                f"Processed {min(offset + batch_size, total_rows)}/{total_rows} rows "
                f"(Updated: {updated}, Errors: {errors})"
            )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"DRY RUN COMPLETE: Would update {updated} rows, {errors} errors encountered"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Backfilled {updated} rows successfully, {errors} errors encountered"
                )
            )
            
        if errors > 0:
            self.stdout.write(
                self.style.WARNING(
                    f"Note: {errors} rows had conversion errors and were skipped. "
                    "Check the data manually for these rows."
                )
            )

    def _ensure_boolean_columns_exist(self, dry_run):
        """Ensure boolean columns exist for the transfer flags."""
        boolean_columns = [
            'IS_TRANSFER_ALLOWED_BOOL',
            'IS_TRANSFER_ALLOWED_FOR_SOURCE_BOOL', 
            'IS_TRANSFER_ALLOWED_FOR_TARGET_BOOL'
        ]
        
        with connection.cursor() as cursor:
            for col_name in boolean_columns:
                # Check if column exists
                cursor.execute("""
                    SELECT COUNT(*) FROM user_tab_cols 
                    WHERE table_name = 'XX_ACCOUNT_ENTITY_LIMIT_XX' 
                    AND column_name = %s
                """, [col_name])
                
                if cursor.fetchone()[0] == 0:
                    if not dry_run:
                        cursor.execute(f"""
                            ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX 
                            ADD ({col_name} NUMBER(1))
                        """)
                    self.stdout.write(f"Added column {col_name}")

    def _process_batch(self, offset, batch_size, dry_run):
        """Process a batch of rows."""
        updated = 0
        errors = 0
        
        try:
            with transaction.atomic():
                # Get batch of rows
                rows = XX_ACCOUNT_ENTITY_LIMIT.objects.all()[offset:offset + batch_size]
                
                for row in rows:
                    try:
                        # Convert encrypted/decrypted values to appropriate types
                        is_transfer_allowed = self._to_boolean(row.is_transer_allowed)
                        is_transfer_allowed_for_source = self._to_boolean(row.is_transer_allowed_for_source)
                        is_transfer_allowed_for_target = self._to_boolean(row.is_transer_allowed_for_target)
                        
                        # source_count and target_count should already be integers
                        source_count = self._to_integer(row.source_count)
                        target_count = self._to_integer(row.target_count)

                        if not dry_run:
                            # Use raw SQL to update the boolean columns
                            with connection.cursor() as cursor:
                                cursor.execute("""
                                    UPDATE XX_ACCOUNT_ENTITY_LIMIT_XX
                                       SET IS_TRANSFER_ALLOWED_BOOL = %s,
                                           IS_TRANSFER_ALLOWED_FOR_SOURCE_BOOL = %s,
                                           IS_TRANSFER_ALLOWED_FOR_TARGET_BOOL = %s
                                     WHERE id = %s
                                """, [
                                    is_transfer_allowed,
                                    is_transfer_allowed_for_source, 
                                    is_transfer_allowed_for_target,
                                    row.id
                                ])
                        
                        updated += 1
                        
                    except Exception as e:
                        errors += 1
                        self.stdout.write(
                            self.style.ERROR(
                                f"Error processing row {row.id}: {str(e)}"
                            )
                        )
                        
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Batch processing error: {str(e)}")
            )
            
        return updated, errors

    def _to_boolean(self, value):
        """
        Convert a value to boolean (0/1 for Oracle), handling various input formats.
        
        Args:
            value: The value to convert (may be encrypted, string, etc.)
            
        Returns:
            1 for True, 0 for False, None for NULL
        """
        if value in (None, ""):
            return None
            
        try:
            # Convert to string first and normalize
            str_value = str(value).strip().lower()
            
            # Handle common boolean representations
            if str_value in ('true', '1', 'yes', 'y', 't'):
                return 1
            elif str_value in ('false', '0', 'no', 'n', 'f'):
                return 0
            else:
                # If it's not a recognizable boolean, return None
                self.stdout.write(
                    self.style.WARNING(
                        f"Could not convert value to boolean: {repr(value)}"
                    )
                )
                return None
                
        except (ValueError, TypeError):
            self.stdout.write(
                self.style.WARNING(
                    f"Could not convert value to boolean: {repr(value)}"
                )
            )
            return None

    def _to_integer(self, value):
        """
        Convert a value to integer, handling various input formats.
        
        Args:
            value: The value to convert
            
        Returns:
            Integer value or None if conversion fails
        """
        if value in (None, ""):
            return None
            
        try:
            if isinstance(value, int):
                return value
            return int(str(value).strip())
            
        except (ValueError, TypeError):
            self.stdout.write(
                self.style.WARNING(
                    f"Could not convert value to integer: {repr(value)}"
                )
            )
            return None
