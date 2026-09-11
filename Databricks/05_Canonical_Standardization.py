# Databricks notebook source
# MAGIC %md
# MAGIC ### Healthcare_Master_Data_Management - Stage 3 : Canonical Standardization
# MAGIC
# MAGIC Applies cross-source canonicalization (code-list lookups,
# MAGIC country/specialty/status normalisation) documented in
# MAGIC `raw_to_std_canonical`, so values from different source systems converge
# MAGIC on one canonical vocabulary before Land_to_Stage.
# MAGIC
# MAGIC `main_canonical_pipeline()` takes no arguments - it reads its own
# MAGIC configuration (which entities/columns are canonicalized) from the
# MAGIC canonical mapping control table, so this notebook has no widgets beyond
# MAGIC informational logging.

# COMMAND ----------
from canonical.canonical import main_canonical_pipeline
from core.runtime_config import catalog, env, get_notebook_run_url

# COMMAND ----------
print(f"Environment : {env}")
print(f"Catalog     : {catalog}")
print(f"Job run URL : {get_notebook_run_url()}")

result = main_canonical_pipeline()
print(result)

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
