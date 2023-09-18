import subprocess
import snowflake.connector
import yaml
import os
import datetime
from snowflake.connector import DictCursor


profile_file = open("../profiles.yml", "r")
test_profile = yaml.safe_load(profile_file)['my-snowflake-db']['outputs']['TEST']
con = snowflake.connector.connect(
    user=test_profile['user'],
    password=test_profile['password'],
    account=test_profile['account'],
    warehouse=test_profile['warehouse']
)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
print(ROOT_DIR)

def init_db_and_dbt():
    con.cursor().execute("CREATE OR REPLACE DATABASE {}".format(test_profile['database']))
    con.cursor().execute("USE DATABASE {}".format(test_profile['database']))
    con.cursor().execute("CREATE OR REPLACE SCHEMA {}_STG".format(test_profile['schema']))
    con.cursor().execute("USE SCHEMA {}_STG".format(test_profile['schema']))
    os.chdir(os.path.join(ROOT_DIR, '..'))
    subprocess.run(["dbt", "deps"])

def test_ingest_2_X_30_random_clients():
    init_db_and_dbt()
    result = subprocess.run(["dbt", "run", "--select", "+dwh_ref", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "run", "--select", "+dwh_ref", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "test", "--select", "conso_client", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout 

def test_ingest_2_X_100_random_clients():
    init_db_and_dbt()
    print("'{\"nb_clients\": 100}'") 
    result = subprocess.run(["dbt", "run", "--select", "+dwh_ref", "--target", "TEST", "--vars", "{\"nb_clients\": 100}"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "run", "--select", "+dwh_ref", "--target", "TEST", "--vars", "{\"nb_clients\": 100}"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "test", "--select", "conso_client", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout 


def test_ingest_2_X_30_random_clients_2_sources():
    init_db_and_dbt()
    result = subprocess.run(["dbt", "run", "--select", "+dwh_multiple_streams", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "run", "--select", "+dwh_multiple_streams", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout
    result = subprocess.run(["dbt", "test", "--select", "conso_client_multiple_streams", "--target", "TEST"], capture_output=True, text=True)
    print(result.stdout)
    assert "Completed successfully" in result.stdout 