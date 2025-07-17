import pandas as pd
import pg8000.dbapi as pg
from sqlalchemy import create_engine
from sqlalchemy import text
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
def extract_raw_messages(engine):
    query = "SELECT * FROM raw.messages"
    df = pd.read_sql(query, engine)
    return df

@task
def transform_messages(df: pd.DataFrame) -> pd.DataFrame:
    df["message_timestamp"] = pd.to_datetime(df["date"])
    df["message_length"] = df["text"].astype(str).str.len()
    df["has_image"] = df["media_file"].notnull()
    df = df.fillna({'text': '', 'media_file': ''})
    df = df.rename(columns={"text": "message_text"})
    return df


@task
def load_to_staging(df, engine):
    with engine.begin() as conn:
        # Ensure schema exists
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        for i, row in df.iterrows():
            try:
                pd.DataFrame([row]).to_sql(
                    "stg_messages", conn, schema="staging", if_exists="append", index=False
                )
            except Exception as e:
                print(f"❌ Failed at row {i}: {e}")
                raise


with Flow("staging-telegram-messages") as flow:
    engine = connect_engine()
    raw_df = extract_raw_messages(engine)
    transformed_df = transform_messages(raw_df)
    load_to_staging(transformed_df, engine)

if __name__ == "__main__":
    flow.run()
