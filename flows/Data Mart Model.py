from sqlalchemy import create_engine, text
from prefect import task, Flow
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="C:/Users/dell/PycharmProjects/Shipping-a-Data-Product/.env")


@task
def connect_engine():
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = "localhost"
    port = 5432
    database = os.getenv('POSTGRES_DB')
    engine=create_engine(f'postgresql+pg8000://{user}:{password}@{host}:{port}/{database}')
    return engine

@task
def create_data_mart_tables(engine):
    with engine.begin() as conn:
        # Create schema if not exists
        conn.execute(text("""CREATE SCHEMA IF NOT EXISTS datamart;"""))

        # Create dim_channels
        conn.execute(text("""CREATE TABLE IF NOT EXISTS datamart.dim_channels (
                channel_id SERIAL PRIMARY KEY,
                channel_name TEXT UNIQUE NOT NULL);
        """))

        # Create dim_dates
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS datamart.dim_dates (
                date_id SERIAL PRIMARY KEY,
                date_value DATE UNIQUE NOT NULL,
                year INT,
                month INT,
                day INT,
                weekday TEXT
            );
        """))

        # Create fct_messages
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS datamart.fct_messages (
                message_id BIGINT PRIMARY KEY,
                sender_id BIGINT,
                channel_id INT REFERENCES datamart.dim_channels(channel_id),
                date_id INT REFERENCES datamart.dim_dates(date_id),
                message_length BIGINT,
                has_image BOOLEAN
            );
        """))

@task
def populate_data_mart(engine):
    with engine.begin() as conn:
        # Populate dim_channels
        conn.execute(text("""
            INSERT INTO datamart.dim_channels (channel_name)
            SELECT DISTINCT channel
            FROM staging.stg_messages
            ON CONFLICT (channel_name) DO NOTHING;
        """))

        # Populate dim_dates
        conn.execute(text("""
            INSERT INTO datamart.dim_dates (date_value, year, month, day, weekday)
            SELECT DISTINCT
                DATE(message_timestamp) AS date_value,
                EXTRACT(YEAR FROM message_timestamp) AS year,
                EXTRACT(MONTH FROM message_timestamp) AS month,
                EXTRACT(DAY FROM message_timestamp) AS day,
                TO_CHAR(message_timestamp, 'Day') AS weekday
            FROM staging.stg_messages
            ON CONFLICT (date_value) DO NOTHING;
        """))

        # Populate fct_messages
        conn.execute(text("""
            INSERT INTO datamart.fct_messages (message_id, sender_id, channel_id, date_id, message_length, has_image)
            SELECT
                s.message_id,
                s.sender_id,
                c.channel_id,
                d.date_id,
                s.message_length,
                s.has_image
            FROM staging.stg_messages s
            JOIN datamart.dim_channels c ON s.channel = c.channel_name
            JOIN datamart.dim_dates d ON DATE(s.message_timestamp) = d.date_value
            ON CONFLICT (message_id) DO NOTHING;
        """))

with Flow("build-data-mart") as build_data_mart_flow:
    engine = connect_engine()
    create = create_data_mart_tables(engine)
    populate = populate_data_mart(engine)
    populate.set_upstream(create)

if __name__ == "__main__":
    build_data_mart_flow.run()