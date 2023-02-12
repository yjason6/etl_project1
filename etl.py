import requests
import pandas as pd

# EXTRACT FUNCTION
def extract(api_key:str,
        start_date:str,
        end_date:str,
        symbols:str,
        base:str)->pd.DataFrame:
    """
    Extracting daily currency data from foreign exchange API.
    - api_key: api key
    - start_date: beginning day of extract
    - end_date: final day of extraction
    - base: Base currency
    - symbols: what currencies you wish to convert 'base' against, three digit currency code seperate by comma
    """
    url = f"https://api.apilayer.com/exchangerates_data/timeseries?start_date={start_date}&end_date={end_date}&symbols={symbols}&base={base}"
    payload = {}
    headers= {
    "apikey": "TyfX4zH87A9CmeZI7CUWHpCayinLIXKF"
    }

    response = requests.request("GET", url, headers=headers, data = payload)

    status_code = response.status_code
    result = response.json()
    df_currency = pd.DataFrame.from_dict(result)
    return df_currency

# TRANSFORM FUNCTION
def transform(
        df_currency:pd.DataFrame,  
    )->pd.DataFrame:
    """
    This function is to transform the raw dataframe, transformation includes.
    - Changing of default date format from YYYY-MM-DD to DD-MM-YYYY
    - Adjusting of index column so that the date column becomes a reular column
    - Renaming of column
    - Selecting selected columns only
    """
    # changing date format to DD-MM-YYYY
    df_currency['date'] = df_currency.index
    df_currency['date_string'] = pd.to_datetime(df_currency['date'], errors='coerce')
    df_currency["date_format"] = df_currency["date_string"].dt.strftime('%d-%m-%Y')
    df_currency
    # renaming columns
    df_currency_rename = df_currency.rename(columns={
    "base": "base_currency",
    "rates": "exchange_rates"
    })
    df_currency_rename
    # selecting the useful columns only
    df_currency_selected = df_currency_rename[["date_format", "base_currency", "exchange_rates"]]
    df_currency_selected
    return df_currency_selected

# LOAD FUNCTION

def load(df_currency_selected:pd.DataFrame)->bool:
    """
    Loading dataframe to Postgres Database
    """
    
    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
    from sqlalchemy.engine import URL
    from sqlalchemy.dialects import postgresql
    from secret_configs import db_user, db_password, db_server_name, db_database_name
    from sqlalchemy.schema import CreateTable

    connection_url = URL.create(
    drivername = "postgresql+pg8000",
    username = db_user,
    password = db_password,
    host = db_server_name,
    port = 5432,
    database = db_database_name,
    )

    engine = create_engine(connection_url)

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

def pipeline()->bool:
    """
    Pipeline performs the ETL from the currency exchange API and loads the data in Postgres in Upsert manner
    """

    # SETTING ENVIRONMENT VARIABLES
    import os
    api_key = os.environ.get("api_key")
    db_user = os.environ.get("db_user")
    db_password = os.environ.get("db_passwor")
    db_server_name = os.environ.get("db_server_name")
    db_database_name = os.environ.get("db_database_name")

    # EXTRACT(Parsing Arguments)
    df_currency = extract(
    api_key=api_key,
    start_date="2022-01-01",
    end_date="2022-12-31",
    symbols="USD,SGD,JPY,EUR,CNY,INR",
    base="AUD"      
    )
    
    # TRANSFORM (Parsing Argument)
    df_currency_selected = transform(df_currency)

    # IMPORT SQLALCHEMY LIBRARIES

    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
    from sqlalchemy.engine import URL
    from sqlalchemy.dialects import postgresql
    from secret_configs import db_user, db_password, db_server_name, db_database_name
    from sqlalchemy.schema import CreateTable

    # LOADING TO DATABASE

    load(df_currency_selected)

    return True

if __name__ == "__main__":
    # run etl pipeline
    if pipeline():
        print("success")