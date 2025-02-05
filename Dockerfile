FROM apache/airflow:2.10.0

# Passer à l'utilisateur airflow avant d'installer duckdb
USER airflow

# Installer le package Python souhaité
RUN pip install --no-cache-dir duckdb

# Revenir à l'utilisateur root (si nécessaire)
USER root
