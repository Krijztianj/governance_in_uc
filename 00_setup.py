# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: Data + Groups + User mapping
# MAGIC
# MAGIC Run this once before the demo. Creates everything the demo depends on:
# MAGIC - **Catalog/schema:** `uc_demo.sample`
# MAGIC - **Tables:** `employees` (1000 rows), `customers` (1000 rows), `user_region_map`
# MAGIC - **Workspace groups (manual, pre-demo):** `admins`, `managers`
# MAGIC
# MAGIC Companion notebook `01_demo.py` covers RBAC + ABAC + consumption.
# MAGIC
# MAGIC ---
# MAGIC ## Part A — Synthetic Data
# MAGIC Idempotent — overwrites on re-run.

# COMMAND ----------

# MAGIC %pip install faker --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "uc_demo"
SCHEMA  = "sample"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

from faker import Faker
import random
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)

fake = Faker("da_DK")
Faker.seed(42)
random.seed(42)

def fake_cpr(birthdate):
    """Danish CPR format: DDMMYY-XXXX. Not a real validation, just shape."""
    dd = birthdate.strftime("%d")
    mm = birthdate.strftime("%m")
    yy = birthdate.strftime("%y")
    serial = random.randint(1000, 9999)
    return f"{dd}{mm}{yy}-{serial}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## employees (1000 rows)
# MAGIC Sensitive columns: `salary`, `cpr`, `email`. ABAC will mask these for non-HR.

# COMMAND ----------

DEPARTMENTS = ["Engineering", "Finance", "Sales", "HR", "Marketing"]
COUNTRIES   = ["DK", "SE", "NO", "DE", "US"]

emp_rows = []
for i in range(1, 1001):
    birth = fake.date_of_birth(minimum_age=22, maximum_age=65)
    emp_rows.append(Row(
        emp_id     = i,
        full_name  = fake.name(),
        email      = fake.company_email(),
        cpr        = fake_cpr(birth),
        salary     = round(random.uniform(45_000, 220_000), 2),
        country    = random.choice(COUNTRIES),
        department = random.choice(DEPARTMENTS),
        hire_date  = fake.date_between(start_date=date(2015, 1, 1), end_date=date(2025, 12, 31)),
    ))

emp_schema = StructType([
    StructField("emp_id",     IntegerType(), False),
    StructField("full_name",  StringType(),  False),
    StructField("email",      StringType(),  False),
    StructField("cpr",        StringType(),  False),
    StructField("salary",     DoubleType(),  False),
    StructField("country",    StringType(),  False),
    StructField("department", StringType(),  False),
    StructField("hire_date",  DateType(),    False),
])

(spark.createDataFrame(emp_rows, schema=emp_schema)
      .write.mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.employees"))

display(spark.table(f"{CATALOG}.{SCHEMA}.employees").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## customers (1000 rows)
# MAGIC Sensitive columns: `email`, `date_of_birth`. Region is the key for row-filter policies.

# COMMAND ----------

REGIONS = ["EMEA", "AMER", "APAC"]
TIERS   = ["bronze", "silver", "gold", "platinum"]

cust_rows = []
for i in range(1, 1001):
    cust_rows.append(Row(
        customer_id     = i,
        full_name       = fake.name(),
        email           = fake.email(),
        date_of_birth   = fake.date_of_birth(minimum_age=18, maximum_age=85),
        tier            = random.choices(TIERS, weights=[40, 30, 20, 10])[0],
        region          = random.choice(REGIONS),
        lifetime_value  = round(random.uniform(50, 50_000), 2),
    ))

cust_schema = StructType([
    StructField("customer_id",    IntegerType(), False),
    StructField("full_name",      StringType(),  False),
    StructField("email",          StringType(),  False),
    StructField("date_of_birth",  DateType(),    False),
    StructField("tier",           StringType(),  False),
    StructField("region",         StringType(),  False),
    StructField("lifetime_value", DoubleType(),  False),
])

(spark.createDataFrame(cust_rows, schema=cust_schema)
      .write.mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.customers"))

display(spark.table(f"{CATALOG}.{SCHEMA}.customers").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

print("employees:", spark.table(f"{CATALOG}.{SCHEMA}.employees").count())
print("customers:", spark.table(f"{CATALOG}.{SCHEMA}.customers").count())
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part B — Groups (manual pre-demo step)
# MAGIC
# MAGIC Databricks Free Edition has no account console / account API access, so
# MAGIC groups are created manually via the workspace admin UI before the demo.
# MAGIC
# MAGIC ### Pre-demo checklist
# MAGIC In **Settings → Identity and access → Groups**, create these two groups
# MAGIC and add the corresponding test users as members:
# MAGIC
# MAGIC | Group | Intent | Demo behavior |
# MAGIC | --- | --- | --- |
# MAGIC | `admins` | No restrictions | Sees raw rows + raw PII |
# MAGIC | `managers` | Cross-region but PII-masked | Sees all rows, PII columns masked |
# MAGIC
# MAGIC Regular users are recognised by
# MAGIC `current_user()` joined to the `user_region_map` table built in Part C.
# MAGIC Anyone in `user_region_map` but not in either group above is a "regular user."
# MAGIC
# MAGIC ### Demo identity assignment
# MAGIC | Persona | Email | Group / Map |
# MAGIC | --- | --- | --- |
# MAGIC | Admin | `kristian.johannesen@outlook.dk` | `admins` |
# MAGIC | Regular user | `krjo@kapacity.dk` | row in `user_region_map` (region=EMEA) |

# COMMAND ----------

# Re-declare constants for the demo notebook to import / re-use
CATALOG = "uc_demo"
SCHEMA  = "sample"

DEMO_GROUPS = ["admins", "managers"]

# Sanity check: confirm the groups exist and are visible to this user
groups_in_ws = {row["name"] for row in spark.sql("SHOW GROUPS").collect()}
missing = [g for g in DEMO_GROUPS if g not in groups_in_ws]
if missing:
    print(f"[warn] missing groups (create in UI before running 01_demo): {missing}")
else:
    print(f"[ok] all demo groups present: {DEMO_GROUPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part C — User → Region mapping table
# MAGIC
# MAGIC The ABAC row-filter policy joins this against `current_user()` to decide
# MAGIC which `region` rows a regular user is allowed to see.
# MAGIC
# MAGIC **Action required:** replace `<REPLACE_WITH_REAL_USER_EMAIL>` below with
# MAGIC the email of the real Databricks user you'll log in as during the
# MAGIC "regular user" part of the demo.

# COMMAND ----------


demo_user = 'krjo@kapacity.dk'
group = 'emea-sales'

# (email, region) pairs. Add as many rows as you have test users to differentiate.
USER_REGION_MAP = [
    (demo_user, "EMEA"),
    (group, "EMEA")
]

map_schema = StructType([
    StructField("user_email", StringType(), False),
    StructField("region",     StringType(), False),
])

(spark.createDataFrame(USER_REGION_MAP, schema=map_schema)
      .write.mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.user_region_map"))

display(spark.table(f"{CATALOG}.{SCHEMA}.user_region_map"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup
# MAGIC
# MAGIC Tears down the data created above. Groups stay (they were created manually
# MAGIC in the UI; remove them in the UI if you want them gone). The companion
# MAGIC `01_demo.py` has its own cleanup for policies/functions/tags.

# COMMAND ----------

# Drop catalog (and all child tables). Uncomment to run.
# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")