import requests, duckdb
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from requests.auth import HTTPBasicAuth
import json
import os
from datetime import datetime, timedelta

# Constantes pour les données OpenSky
colonne_open_sky = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude",
                    "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude",
                    "squawk", "spi", "position_source", "category"]

# URL de l'API OpenSky
url_all = "https://opensky-network.org/api/states/all?extended=true"

# Identifiants pour l'API OpenSky
credentials = {"username": '******', "password": '****'}

# Chemin du fichier JSON (absolu recommandé)
data_file_path = "/opt/airflow/dags/data/opensky_data.json"  # Chemin absolu

# Chemin du fichier de base de données DuckDB (absolu recommandé)
db_path = "/opt/airflow/dags/data/opensky_data.duckdb"  # Chemin absolu

output_file_path = "/opt/airflow/dags/data/data_quality_summary.json"  # Chemin du fichier de résumé


@task
def create_or_truncate_table(db_path):
    try:
        conn = duckdb.connect(db_path)
        conn.execute(f"""
            CREATE OR REPLACE TABLE openskynetwork_brute (
                icao24 VARCHAR,
                callsign VARCHAR,
                origin_country VARCHAR,
                time_position BIGINT,
                last_contact BIGINT,
                longitude DOUBLE,
                latitude DOUBLE,
                baro_altitude DOUBLE,
                on_ground BOOLEAN,
                velocity DOUBLE,
                true_track DOUBLE,
                vertical_rate DOUBLE,
                sensors BIGINT[],
                geo_altitude DOUBLE,
                squawk VARCHAR,
                spi BOOLEAN,
                position_source INT,
                category INT,
                flight_duration DOUBLE,  -- Ajout de la colonne flight_duration ici
                PRIMARY KEY (callsign, time_position, last_contact)
            )
        """)
        print("Table created or replaced successfully.")
    except Exception as e:
        print(f"Error creating/replacing table: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task
def get_data_from_opensky(col, url, cred, data_file):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(cred['username'], cred['password']))

        # Lève une exception pour les codes d'erreur HTTP
        response.raise_for_status()
        data = response.json()

        # Gérer le cas où 'states' est absent
        states_list = data.get('states', [])
        states_json = [dict(zip(col, state)) for state in states_list if state is not None]

        with open(data_file, 'w') as f:
            json.dump(states_json, f, indent=4)

            # Ajout d'indentation pour la lisibilité
        print(f"Data downloaded and saved to {data_file}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        raise
        # Important: remonter l'erreur pour que Airflow gère l'échec de la tâche

    except (KeyError, TypeError) as e:  #gestion des erreurs de données
        print(f"Erreur de format des données OpenSky: {e}")
        raise
    except Exception as e:
        print(f"Une erreur est survenue: {e}")
        raise


@task
def load_data_into_duckdb(data_file, db_path):
    try:
        conn = duckdb.connect(db_path)
        conn.execute("BEGIN TRANSACTION")

        # Récupérer les données transformées depuis la base de données
        transformed_data = conn.execute("SELECT * FROM openskynetwork_brute").fetchall()

        # Préparer les données pour l'insertion
        data_to_insert = [dict(zip(colonne_open_sky, row)) for row in transformed_data]

        for row in data_to_insert:
            validated_row = validate_row(row, colonne_open_sky)
            if validated_row is None:
                continue

            insert_data(conn, validated_row, colonne_open_sky)

        conn.commit()
        print("Données chargées dans DuckDB avec succès.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur lors du chargement des données dans DuckDB: {e}")
        raise

    finally:
        if conn:
            conn.close()


def validate_row(row, colonne_open_sky):
    """Valide une ligne de données avant l'insertion."""
    validated_row = {}
    for col in colonne_open_sky:
        value = row.get(col)
        if col == 'time_position' and value is None:  # Gestion spécifique de time_position
            print(f"Ligne ignorée (time_position manquant): {row}")
            return None  # Ligne invalide

        validated_row[col] = value  # Ajoute la valeur validée (ou None si manquante)
    return validated_row


def insert_data(conn, row_data, colonne_open_sky):
    """Insère une ligne de données dans la base de données."""

    values = [row_data.get(col) for col in colonne_open_sky]
    placeholders = ", ".join(["?"] * len(colonne_open_sky))
    conn.execute(f"INSERT INTO openskynetwork_brute VALUES ({placeholders})", tuple(values))


@task
def transform_data(db_path):
    try:
        conn = duckdb.connect(db_path)
        conn.execute("BEGIN TRANSACTION")

        conn.execute("""
            UPDATE openskynetwork_brute
            SET flight_duration = (last_contact - time_position) / 60.0
            WHERE last_contact IS NOT NULL AND time_position IS NOT NULL;
        """)

        # Vous pouvez ajouter d'autres transformations ici

        conn.commit()
        print("Données transformées avec succès.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur lors de la transformation des données: {e}")
        raise

    finally:
        if conn:
            conn.close()


@task
def check_row_count(db_path):
    """Vérifie le nombre de lignes dans la table."""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        row_count = conn.execute("SELECT COUNT(*) FROM openskynetwork_brute").fetchone()[0]
        assert row_count > 0, "La table est vide après le chargement !"
        print(f"Nombre de lignes vérifié : {row_count}")
        return {"row_count": row_count}
    except AssertionError as e:
        print(f"Erreur: {e}")
        raise
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task
def check_time_range(db_path):
    """Détermine le temps minimal et le temps maximal."""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        min_time = conn.execute("SELECT MIN(time_position) FROM openskynetwork_brute").fetchone()[0]
        max_time = conn.execute("SELECT MAX(time_position) FROM openskynetwork_brute").fetchone()[0]
        print(f"Temps min et max vérifiés : min={min_time}, max={max_time}")
        return {"min_time_position": min_time, "max_time_position": max_time}
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task
def check_duplicates(db_path):
    """Détecte et compte le nombre de doublons."""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        duplicate_count = conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT icao24, callsign, time_position, last_contact)
            FROM openskynetwork_brute
        """).fetchone()[0]
        print(f"Nombre de doublons vérifié : {duplicate_count}")
        return {"duplicate_count": duplicate_count}
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task
def collect_quality_results(row_count_result, time_range_result, duplicate_count_result, output_file_path):
    """Collecte les résultats des vérifications et les enregistre dans un fichier JSON."""
    summary = {
        "row_count": row_count_result["row_count"],
        "min_time_position": time_range_result["min_time_position"],
        "max_time_position": time_range_result["max_time_position"],
        "duplicate_count": duplicate_count_result["duplicate_count"],
    }

    with open(output_file_path, 'w') as f:
        json.dump(summary, f, indent=4)
    print(f"Résumé enregistré dans {output_file_path}")


@dag(schedule=None, catchup=False, start_date=datetime(2025, 2, 2),
     default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
     dagrun_timeout=timedelta(minutes=60))
def flights_pipeline():
    start = EmptyOperator(task_id="Start")
    get_data_task = get_data_from_opensky(colonne_open_sky, url_all, credentials, data_file_path)
    create_table_task = create_or_truncate_table(db_path)
    load_data_task = load_data_into_duckdb(data_file_path, db_path)
    transform_data_task = transform_data(db_path)

    # Les tâches suivantes s'exécutent en parallèle
    quality_checks = [
        check_time_range(db_path),
        #check_row_count(db_path)
        #check_duplicates(db_path),
    ]

    #quality_results_task = collect_quality_results(*quality_checks, "/opt/airflow/dags/data/data_quality_summary.json") # * pour déballer la liste

    end = EmptyOperator(task_id="End")

    start >> create_table_task >> get_data_task >> transform_data_task >> load_data_task >> quality_checks  >> end

flights_pipeline_dag = flights_pipeline()
