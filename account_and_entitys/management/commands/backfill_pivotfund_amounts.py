"""
Management command to decrypt and backfill numeric amount columns on XX_PivotFund_XX.

This command safely handles the migration from encrypted CharField amounts 
to DecimalField amounts by:
1. Reading each row through Django ORM (which handles decryption)
2. Converting the decrypted values to Decimal
3. Writing them to new numeric columns via raw SQL

Usage: python manage.py backfill_pivotfund_amounts
"""

from decimal import Decimal, InvalidOperation
from django.core.management.base import BaseCommand
from django.db import transaction, connection
from account_and_entitys.models import XX_PivotFund


class Command(BaseCommand):
    help = "Decrypt and backfill numeric amount columns on XX_PivotFund_XX"

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
        
        # Check if numeric columns exist
        if not self._check_numeric_columns_exist():
            self.stdout.write(
                self.style.ERROR(
                    "Numeric columns (ACTUAL_NUM, FUND_NUM, BUDGET_NUM, ENCUMBRANCE_NUM) "
                    "do not exist. Run migration 0007_oracle_safe_transform first."
                )
            )
            return

        total_rows = XX_PivotFund.objects.count()
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

    def _check_numeric_columns_exist(self):
        """Check if the numeric columns exist in the database."""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM user_tab_cols 
                WHERE table_name = 'XX_PIVOTFUND_XX' 
                AND column_name IN ('ACTUAL_NUM', 'FUND_NUM', 'BUDGET_NUM', 'ENCUMBRANCE_NUM')
            """)
            count = cursor.fetchone()[0]
            return count == 4

    def _process_batch(self, offset, batch_size, dry_run):
        """Process a batch of rows."""
        updated = 0
        errors = 0
        
        try:
            with transaction.atomic():
                # Get batch of rows
                rows = XX_PivotFund.objects.all()[offset:offset + batch_size]
                
                for row in rows:
                    try:
                        # Convert encrypted/decrypted values to Decimal
                        actual = self._to_decimal(row.actual)
                        fund = self._to_decimal(row.fund)
                        budget = self._to_decimal(row.budget)
                        encumbrance = self._to_decimal(row.encumbrance)

                        if not dry_run:
                            # Use raw SQL to update the *_NUM columns we created
                            with connection.cursor() as cursor:
                                cursor.execute("""
                                    UPDATE XX_PivotFund_XX
                                       SET ACTUAL_NUM = %s, 
                                           FUND_NUM = %s, 
                                           BUDGET_NUM = %s, 
                                           ENCUMBRANCE_NUM = %s
                                     WHERE id = %s
                                """, [actual, fund, budget, encumbrance, row.id])
                        
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

    def _to_decimal(self, value):
        """
        Convert a value to Decimal, handling various input formats.
        
        Args:
            value: The value to convert (may be encrypted, string, number, etc.)
            
        Returns:
            Decimal value or None if conversion fails
        """
        if value in (None, ""):
            return None
            
        try:
            # Convert to string first, then clean up common formatting
            str_value = str(value).replace(",", "").strip()
            
            # Handle empty strings after cleanup
            if not str_value:
                return None
                
            return Decimal(str_value)
            
        except (InvalidOperation, ValueError, TypeError):
            # Log the problematic value for debugging
            self.stdout.write(
                self.style.WARNING(
                    f"Could not convert value to decimal: {repr(value)}"
                )
            )
            return None
