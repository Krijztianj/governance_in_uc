# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Demo: RBAC + ABAC + Consumption
# MAGIC
# MAGIC The notebook is the *post-talk artifact*. The live demo runs in the
# MAGIC Databricks UI and Power BI; this file mirrors every step in code so the
# MAGIC audience can reproduce it.
# MAGIC
# MAGIC **Prereq:** run `00_setup.py` first. You must also have manually created
# MAGIC the workspace groups `admins` and `managers`, and replaced the
# MAGIC placeholder email in `uc_demo.sample.user_region_map`.
# MAGIC
# MAGIC ## Sections
# MAGIC 1. **Grants** — coarse, group-based grants. Boring by design.
# MAGIC 2. **ABAC: Tags** — attribute the data
# MAGIC 3. **ABAC: Functions** — define how data is transformed
# MAGIC 4. **ABAC: Policies** — bind tags + functions + principals
# MAGIC 5. **Consumption in Databricks** — same query, different identity, different result
# MAGIC 6. **Consumption in Power BI** — connection reference
# MAGIC 7. **Cleanup**

# COMMAND ----------

CATALOG = "uc_demo"
SCHEMA  = "sample"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Group-based grants
# MAGIC
# MAGIC The "old way." Coarse on/off access at the table level. We grant `SELECT`
# MAGIC broadly so people can *query* the tables — ABAC will then layer fine-grained
# MAGIC restrictions on top.
# MAGIC
# MAGIC `account users` is the system group containing every user in the account.
# MAGIC On workspace-only setups it may be called `users` instead — adjust if needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG uc_demo            TO `account users`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  uc_demo.sample     TO `account users`;
# MAGIC GRANT SELECT      ON TABLE   uc_demo.sample.employees TO `account users`;
# MAGIC GRANT SELECT      ON TABLE   uc_demo.sample.customers TO `account users`;
# MAGIC GRANT SELECT      ON TABLE   uc_demo.sample.user_region_map TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The two named groups don't get extra privileges here.
# MAGIC -- Their elevated access comes through ABAC policy *exceptions*.
# MAGIC SHOW GRANTS ON TABLE uc_demo.sample.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why this is boring
# MAGIC Every consumer can now `SELECT` everything. To prevent the analyst from
# MAGIC seeing salaries you'd traditionally either:
# MAGIC - Build a view per persona (N views, drift over time), or
# MAGIC - Move data to a separate table (data duplication, lineage break).
# MAGIC
# MAGIC ABAC replaces both patterns with one declarative policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. ABAC — Governed Tags
# MAGIC
# MAGIC ABAC policies match against **governed tags** — tags declared at the
# MAGIC metastore level via `CREATE GOVERNED TAG`. Plain `ALTER TABLE … SET TAGS`
# MAGIC writes informational tags that `has_tag_value()` will not see.
# MAGIC
# MAGIC Two-step pattern:
# MAGIC 1. Declare the governed tag (key, optional allowed values)
# MAGIC 2. Apply it to columns/tables with `ALTER … SET TAGS`
# MAGIC
# MAGIC Tag scheme:
# MAGIC - `pii` (values: `string`, `numeric`) on `cpr`, `email`, `full_name` → triggers column mask
# MAGIC - `geo_region` (key-only) on `region` columns → triggers row filter
# MAGIC
# MAGIC Requires **CREATE** privilege on governed tags. Workspace admins have it by default.

# COMMAND ----------

# Step 1: declare the governed tags.
# `CREATE GOVERNED TAG` does not support IF NOT EXISTS, so we wrap in try/except
# to keep this cell idempotent on re-run.

GOVERNED_TAGS = [
    """CREATE GOVERNED TAG pii
         DESCRIPTION 'Marks columns containing personally identifiable information'
         VALUES ('string', 'numeric')""",
    """CREATE GOVERNED TAG geo_region
         DESCRIPTION 'Marks columns used for region-based row filtering'""",
]

for stmt in GOVERNED_TAGS:
    try:
        spark.sql(stmt)
        print(f"[create] {stmt.split()[2]}")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower() or "duplicate" in msg.lower():
            print(f"[skip]   {stmt.split()[2]} (already exists)")
        else:
            raise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: apply the governed tags to columns
# MAGIC -- (same SET TAGS syntax — but now they're governed because the tag exists at metastore level)
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN cpr       SET TAGS ('pii' = 'string');
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN email     SET TAGS ('pii' = 'string');
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN full_name SET TAGS ('pii' = 'string');
# MAGIC
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN email     SET TAGS ('pii' = 'string');
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN full_name SET TAGS ('pii' = 'string');
# MAGIC
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN region    SET TAGS ('geo_region');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify governed tags exist + are applied
# MAGIC SHOW GOVERNED TAGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify column-tag bindings
# MAGIC SELECT table_name, column_name, tag_name, tag_value
# MAGIC FROM   system.information_schema.column_tags
# MAGIC WHERE  catalog_name = 'uc_demo'
# MAGIC AND    schema_name  = 'sample'
# MAGIC ORDER  BY table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. ABAC — Functions (UDFs)
# MAGIC
# MAGIC Two SQL UDFs encode the actual access logic. They reference
# MAGIC `is_account_group_member()` and `current_user()` to evaluate per-query
# MAGIC who's asking and what they're allowed to see.
# MAGIC
# MAGIC ### `mask_pii_string`
# MAGIC Returns the original value for `admins`. Otherwise replaces every
# MAGIC alphanumeric character with `X` — preserves shape (e.g. `150789-1234`
# MAGIC becomes `XXXXXX-XXXX`) so the mask is recognisable.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION uc_demo.sample.mask_pii_string(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC CASE
# MAGIC     WHEN is_member('admins') THEN val
# MAGIC     WHEN val IS NULL                            THEN NULL
# MAGIC     ELSE regexp_replace(val, '[A-Za-z0-9]', 'X')
# MAGIC END
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### `region_filter`
# MAGIC Row filter — returns `TRUE` (row visible) when:
# MAGIC - User is in `admins` OR `managers` (no row restriction), OR
# MAGIC - The row's region matches the user's region in `user_region_map`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION uc_demo.sample.region_filter(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC      is_member('managers')
# MAGIC   OR region IN (
# MAGIC        SELECT m.region
# MAGIC        FROM   uc_demo.sample.user_region_map m
# MAGIC        WHERE  m.user_email = current_user()
# MAGIC      );

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. ABAC — Policies
# MAGIC
# MAGIC Policies bind: **(tag selector) → (UDF) → (principals with optional exceptions)**.
# MAGIC
# MAGIC Apply at SCHEMA level so any future table tagged the same way inherits
# MAGIC automatically.

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC -- Column mask: any column tagged pii=string is masked for everyone
# MAGIC -- (admins see raw values via logic inside the mask_pii_string UDF)
# MAGIC CREATE OR REPLACE POLICY mask_pii_policy
# MAGIC ON SCHEMA uc_demo.sample
# MAGIC COMMENT 'Mask PII string columns for all but admins'
# MAGIC COLUMN MASK uc_demo.sample.mask_pii_string
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'string') AS pii_col
# MAGIC ON COLUMN pii_col;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row filter: any table with a column tagged geo_region is filtered.
# MAGIC -- admins + managers bypass the filter inside the UDF itself.
# MAGIC CREATE OR REPLACE POLICY filter_region_policy
# MAGIC ON SCHEMA uc_demo.sample
# MAGIC COMMENT 'Filter rows by user region for non-privileged users'
# MAGIC ROW FILTER uc_demo.sample.region_filter
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag('geo_region') AS region_col
# MAGIC USING COLUMNS (region_col);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm both policies registered
# MAGIC SHOW POLICIES ON SCHEMA uc_demo.sample;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Consumption in Databricks
# MAGIC
# MAGIC The same SQL produces different results depending on the logged-in user.
# MAGIC On stage, switch browser profiles between identities and re-run.
# MAGIC
# MAGIC | Identity | `employees.cpr` | `customers` rows |
# MAGIC | --- | --- | --- |
# MAGIC | `admins` member | `150789-1234` (raw) | All ~1000 rows |
# MAGIC | `managers` member     | `XXXXXX-XXXX`       | All ~1000 rows |
# MAGIC | regular user (in `user_region_map`) | `XXXXXX-XXXX` | Only their region |
# MAGIC | regular user NOT in map | `XXXXXX-XXXX` | 0 rows |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm who is running this query right now
# MAGIC SELECT current_user() AS me, is_account_group_member('admins') AS is_admin,
# MAGIC                            is_account_group_member('managers')     AS is_manager;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PII visibility check (cpr, email)
# MAGIC SELECT emp_id, full_name, email, cpr, salary, country, department
# MAGIC FROM   uc_demo.sample.employees
# MAGIC LIMIT  10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-filter visibility check
# MAGIC SELECT region, COUNT(*) AS row_count
# MAGIC FROM   uc_demo.sample.customers
# MAGIC GROUP  BY region
# MAGIC ORDER  BY region;

# COMMAND ----------

# DBTITLE 1,Cell 24
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Consumption in Power BI
# MAGIC
# MAGIC The exact same UC ABAC policies fire when Power BI connects, **provided
# MAGIC PBI authenticates as a real user** (OAuth/SSO). PAT/service-principal
# MAGIC connections give every PBI session one fixed identity.
# MAGIC
# MAGIC ### Connection (PBI Desktop)
# MAGIC 1. **Get Data → Azure → Azure Databricks**
# MAGIC 2. **Server hostname:** copy from SQL Warehouse → Connection details
# MAGIC 3. **HTTP path:**     copy from same panel
# MAGIC 4. **Database:**      `uc_demo`
# MAGIC 5. **Auth:** *Microsoft account* (passes user identity to UC) — **not** PAT
# MAGIC 6. **Storage mode:** *DirectQuery*. Import bypasses live ABAC enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Cleanup
# MAGIC
# MAGIC Reverses everything created by this notebook. Order matters: drop policies
# MAGIC before functions, untag columns last.

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC -- Drop policies
# MAGIC DROP POLICY mask_pii_policy    ON SCHEMA uc_demo.sample;
# MAGIC DROP POLICY filter_region_policy ON SCHEMA uc_demo.sample;
# MAGIC
# MAGIC -- Drop UDFs
# MAGIC DROP FUNCTION IF EXISTS uc_demo.sample.mask_pii_string;
# MAGIC DROP FUNCTION IF EXISTS uc_demo.sample.region_filter;
# MAGIC
# MAGIC -- Remove governed tags from columns first
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN cpr       UNSET TAGS ('pii');
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN email     UNSET TAGS ('pii');
# MAGIC ALTER TABLE uc_demo.sample.employees ALTER COLUMN full_name UNSET TAGS ('pii');
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN email     UNSET TAGS ('pii');
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN full_name UNSET TAGS ('pii');
# MAGIC ALTER TABLE uc_demo.sample.customers ALTER COLUMN region    UNSET TAGS ('geo_region');
# MAGIC
# MAGIC -- Drop the governed tags themselves (metastore-level)
# MAGIC DROP GOVERNED TAG pii;
# MAGIC DROP GOVERNED TAG geo_region;
# MAGIC
# MAGIC -- Revoke grants (optional — only run if you want a fully clean slate)
# MAGIC REVOKE SELECT      ON TABLE   uc_demo.sample.employees       FROM `account users`;
# MAGIC REVOKE SELECT      ON TABLE   uc_demo.sample.customers       FROM `account users`;
# MAGIC REVOKE SELECT      ON TABLE   uc_demo.sample.user_region_map FROM `account users`;
# MAGIC REVOKE USE SCHEMA  ON SCHEMA  uc_demo.sample                 FROM `account users`;
# MAGIC REVOKE USE CATALOG ON CATALOG uc_demo                        FROM `account users`;