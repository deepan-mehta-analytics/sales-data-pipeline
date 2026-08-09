# =============================================================================
# dags/__init__.py
# Marks dags/ as a regular Python package so `from dags.path_utils import ...`
# resolves reliably under pytest's pythonpath = ["."] configuration, matching
# the same convention already used by src/ and orchestration/.
# =============================================================================
