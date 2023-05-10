import snowflake.snowpark.functions as F
from faker import Faker
from datetime import datetime


def add_clients(session, nb, start):
    fake = Faker()
    clients = [[value] + [fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=18, maximum_age=90)] + [datetime.now()] for value in range(start, start + nb)]
    df = session.create_dataframe(clients, schema=["ID", "FIRST_NAME", "LAST_NAME", "BIRTHDATE", "LOADED_AT"])
    return df

def model(dbt, session):
    dbt.config(
        packages = ["Faker"]
    )
    dbt.config(materialized = "incremental")
    max_id = 0
    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_id_req = f"select max(id) from {dbt.this}"
        max_id= session.sql(max_id_req).collect()[0][0] 

    nb_clients = dbt.config.get("nb_clients")
    return add_clients(session, nb_clients, int(max_id) + 1)