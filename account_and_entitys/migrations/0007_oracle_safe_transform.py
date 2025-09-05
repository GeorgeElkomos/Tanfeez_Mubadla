from django.db import migrations, connection
from django.db.utils import DatabaseError

# Oracle error codes we can safely ignore for idempotent DDL
IGNORABLE = (
    "ORA-01430",  # column being added already exists
    "ORA-00904",  # invalid identifier (e.g., column already renamed/missing)
    "ORA-00955",  # name is already used by an existing object (constraint/index)
    "ORA-02261",  # such unique or primary key already exists
    "ORA-02443",  # drop constraint - nonexistent
)

def _exec(sql: str, extra_ok=()):
    """Execute a single SQL statement, ignoring expected Oracle errors."""
    with connection.cursor() as c:
        try:
            c.execute(sql)
        except DatabaseError as e:
            s = str(e)
            ok = IGNORABLE + tuple(extra_ok)
            if not any(code in s for code in ok):
                raise

def _exec_variants(sql_list, extra_ok=()):
    """
    Try a list of SQL variants (e.g., unquoted UPPERCASE then quoted CamelCase).
    If ORA-00942 occurs, try next variant. Otherwise, ignore ignorable codes or raise.
    """
    last_exc = None
    with connection.cursor() as c:
        for i, sql in enumerate(sql_list):
            try:
                c.execute(sql)
                return
            except DatabaseError as e:
                s = str(e)
                if "ORA-00942" in s and i < len(sql_list) - 1:
                    last_exc = e
                    continue  # try next variant
                ok = IGNORABLE + tuple(extra_ok)
                if any(code in s for code in ok):
                    return
                last_exc = e
                break
    if last_exc:
        raise last_exc

def forwards(apps, schema_editor):
    # ---------- XX_PivotFund ----------
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX ADD (ENTITY_NUM NUMBER)',
        'ALTER TABLE "XX_PivotFund_XX" ADD ("ENTITY_NUM" NUMBER)',
    ])
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX ADD (ACCOUNT_NUM NUMBER)',
        'ALTER TABLE "XX_PivotFund_XX" ADD ("ACCOUNT_NUM" NUMBER)',
    ])

    # backfill numerics from text (prefer UPPERCASE columns, fallback to quoted)
    _exec_variants([
        ("UPDATE XX_PIVOTFUND_XX "
         "SET ENTITY_NUM = CASE WHEN REGEXP_LIKE(TRIM(ENTITY), '^-?[0-9]+$') THEN TO_NUMBER(ENTITY) ELSE ENTITY_NUM END, "
         "    ACCOUNT_NUM = CASE WHEN REGEXP_LIKE(TRIM(ACCOUNT),'^-?[0-9]+$') THEN TO_NUMBER(ACCOUNT) ELSE ACCOUNT_NUM END "
         "WHERE (ENTITY_NUM IS NULL OR ACCOUNT_NUM IS NULL)"),
        ('UPDATE "XX_PivotFund_XX" '
         'SET "ENTITY_NUM" = CASE WHEN REGEXP_LIKE(TRIM("entity"),  \'^-?[0-9]+$\') THEN TO_NUMBER("entity")  ELSE "ENTITY_NUM"  END, '
         '    "ACCOUNT_NUM" = CASE WHEN REGEXP_LIKE(TRIM("account"), \'^-?[0-9]+$\') THEN TO_NUMBER("account") ELSE "ACCOUNT_NUM" END '
         'WHERE ("ENTITY_NUM" IS NULL OR "ACCOUNT_NUM" IS NULL)'),
    ])

    # rename columns (try UPPERCASE first, then quoted)
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX RENAME COLUMN ENTITY TO ENTITY_TXT',
        'ALTER TABLE "XX_PivotFund_XX" RENAME COLUMN "entity" TO "entity_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX RENAME COLUMN ACCOUNT TO ACCOUNT_TXT',
        'ALTER TABLE "XX_PivotFund_XX" RENAME COLUMN "account" TO "account_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX RENAME COLUMN ENTITY_NUM TO ENTITY',
        'ALTER TABLE "XX_PivotFund_XX" RENAME COLUMN "ENTITY_NUM" TO "entity"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX RENAME COLUMN ACCOUNT_NUM TO ACCOUNT',
        'ALTER TABLE "XX_PivotFund_XX" RENAME COLUMN "ACCOUNT_NUM" TO "account"',
    ], extra_ok=("ORA-01430",))

    # unique constraint
    _exec_variants([
        'ALTER TABLE XX_PIVOTFUND_XX ADD CONSTRAINT UX_XXPF_ENT_ACC_YEAR UNIQUE (ENTITY, ACCOUNT, YEAR)',
        'ALTER TABLE "XX_PivotFund_XX" ADD CONSTRAINT "UX_XXPF_ENT_ACC_YEAR" UNIQUE ("entity","account","year")',
    ])

    # amount numeric columns
    for col in ("ACTUAL_NUM", "FUND_NUM", "BUDGET_NUM", "ENCUMBRANCE_NUM"):
        _exec_variants([
            f'ALTER TABLE XX_PIVOTFUND_XX ADD ({col} NUMBER(15,2))',
            f'ALTER TABLE "XX_PivotFund_XX" ADD ("{col}" NUMBER(15,2))',
        ])

    # ---------- XX_Account ----------
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_XX ADD (ACCOUNT_NUM NUMBER)',
        'ALTER TABLE "XX_Account_XX" ADD ("ACCOUNT_NUM" NUMBER)',
    ])
    _exec_variants([
        ('UPDATE XX_ACCOUNT_XX '
         'SET ACCOUNT_NUM = CASE WHEN REGEXP_LIKE(TRIM(ACCOUNT), \'^-?[0-9]+$\') THEN TO_NUMBER(ACCOUNT) ELSE ACCOUNT_NUM END '
         'WHERE ACCOUNT_NUM IS NULL'),
        ('UPDATE "XX_Account_XX" '
         'SET "ACCOUNT_NUM" = CASE WHEN REGEXP_LIKE(TRIM("account"), \'^-?[0-9]+$\') THEN TO_NUMBER("account") ELSE "ACCOUNT_NUM" END '
         'WHERE "ACCOUNT_NUM" IS NULL'),
    ])
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_XX RENAME COLUMN ACCOUNT TO ACCOUNT_TXT',
        'ALTER TABLE "XX_Account_XX" RENAME COLUMN "account" TO "account_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_XX RENAME COLUMN ACCOUNT_NUM TO ACCOUNT',
        'ALTER TABLE "XX_Account_XX" RENAME COLUMN "ACCOUNT_NUM" TO "account"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_XX ADD CONSTRAINT UX_XXACC_ACCOUNT UNIQUE (ACCOUNT)',
        'ALTER TABLE "XX_Account_XX" ADD CONSTRAINT "UX_XXACC_ACCOUNT" UNIQUE ("account")',
    ])

    # ---------- XX_Entity ----------
    _exec_variants([
        'ALTER TABLE XX_ENTITY_XX ADD (ENTITY_NUM NUMBER)',
        'ALTER TABLE "XX_Entity_XX" ADD ("ENTITY_NUM" NUMBER)',
    ])
    _exec_variants([
        ('UPDATE XX_ENTITY_XX '
         'SET ENTITY_NUM = CASE WHEN REGEXP_LIKE(TRIM(ENTITY), \'^-?[0-9]+$\') THEN TO_NUMBER(ENTITY) ELSE ENTITY_NUM END '
         'WHERE ENTITY_NUM IS NULL'),
        ('UPDATE "XX_Entity_XX" '
         'SET "ENTITY_NUM" = CASE WHEN REGEXP_LIKE(TRIM("entity"), \'^-?[0-9]+$\') THEN TO_NUMBER("entity") ELSE "ENTITY_NUM" END '
         'WHERE "ENTITY_NUM" IS NULL'),
    ])
    _exec_variants([
        'ALTER TABLE XX_ENTITY_XX RENAME COLUMN ENTITY TO ENTITY_TXT',
        'ALTER TABLE "XX_Entity_XX" RENAME COLUMN "entity" TO "entity_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ENTITY_XX RENAME COLUMN ENTITY_NUM TO ENTITY',
        'ALTER TABLE "XX_Entity_XX" RENAME COLUMN "ENTITY_NUM" TO "entity"',
    ], extra_ok=("ORA-01430",))

    # ---------- XX_ACCOUNT_ENTITY_LIMIT ----------
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX ADD (ACCOUNT_ID_NUM NUMBER)',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" ADD ("ACCOUNT_ID_NUM" NUMBER)',
    ])
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX ADD (ENTITY_ID_NUM NUMBER)',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" ADD ("ENTITY_ID_NUM" NUMBER)',
    ])
    _exec_variants([
        ('UPDATE XX_ACCOUNT_ENTITY_LIMIT_XX '
         'SET ACCOUNT_ID_NUM = CASE WHEN REGEXP_LIKE(TRIM(ACCOUNT_ID), \'^-?[0-9]+$\') THEN TO_NUMBER(ACCOUNT_ID) ELSE ACCOUNT_ID_NUM END, '
         '    ENTITY_ID_NUM  = CASE WHEN REGEXP_LIKE(TRIM(ENTITY_ID),  \'^-?[0-9]+$\') THEN TO_NUMBER(ENTITY_ID)  ELSE ENTITY_ID_NUM  END '
         'WHERE (ACCOUNT_ID_NUM IS NULL OR ENTITY_ID_NUM IS NULL)'),
        ('UPDATE "XX_ACCOUNT_ENTITY_LIMIT_XX" '
         'SET "ACCOUNT_ID_NUM" = CASE WHEN REGEXP_LIKE(TRIM("account_id"), \'^-?[0-9]+$\') THEN TO_NUMBER("account_id") ELSE "ACCOUNT_ID_NUM" END, '
         '    "ENTITY_ID_NUM"  = CASE WHEN REGEXP_LIKE(TRIM("entity_id"),  \'^-?[0-9]+$\') THEN TO_NUMBER("entity_id")  ELSE "ENTITY_ID_NUM"  END '
         'WHERE ("ACCOUNT_ID_NUM" IS NULL OR "ENTITY_ID_NUM" IS NULL)'),
    ])
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX RENAME COLUMN ACCOUNT_ID TO ACCOUNT_ID_TXT',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" RENAME COLUMN "account_id" TO "account_id_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX RENAME COLUMN ENTITY_ID TO ENTITY_ID_TXT',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" RENAME COLUMN "entity_id" TO "entity_id_txt"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX RENAME COLUMN ACCOUNT_ID_NUM TO ACCOUNT_ID',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" RENAME COLUMN "ACCOUNT_ID_NUM" TO "account_id"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX RENAME COLUMN ENTITY_ID_NUM TO ENTITY_ID',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" RENAME COLUMN "ENTITY_ID_NUM" TO "entity_id"',
    ], extra_ok=("ORA-01430",))
    _exec_variants([
        'ALTER TABLE XX_ACCOUNT_ENTITY_LIMIT_XX ADD CONSTRAINT UX_XXAEL_ACC_ENT UNIQUE (ACCOUNT_ID, ENTITY_ID)',
        'ALTER TABLE "XX_ACCOUNT_ENTITY_LIMIT_XX" ADD CONSTRAINT "UX_XXAEL_ACC_ENT" UNIQUE ("account_id","entity_id")',
    ])

class Migration(migrations.Migration):
    dependencies = [
        ("account_and_entitys", "0006_alter_xx_account_entity_limit_is_transer_allowed_and_more"),
    ]
    atomic = False
    operations = [migrations.RunPython(forwards, migrations.RunPython.noop)]
