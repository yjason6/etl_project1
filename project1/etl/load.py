import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from sqlalchemy.dialects import postgresql

# LOAD FUNCTION

def load(df_currency_selected:pd.DataFrame, engine)->bool:
    """
    Loading dataframe to Postgres Database
    """


    meta = MetaData()
    aud_conversion_2022_table = Table(
        "aud_conversion_2022", meta,
        Column("date_format", String, primary_key=True),
        Column("base_currency", String),
        Column("exchange_rates", String),
    )
    meta.create_all(engine)
    # UPSERT STATEMENT
    insert_statement = postgresql.insert(aud_conversion_2022_table).values(df_currency_selected.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
    index_elements=['date_format'],
    set_={c.key: c for c in insert_statement.excluded if c.key not in ['date_format']})
    engine.execute(upsert_statement)
    return True