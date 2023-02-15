from etl import extract as Extract
from etl import transform as Transform
from etl import load as Load
import logging
import yaml
from utility.metadata_logging import MetadataLogging
import datetime as dt
from io import StringIO


def pipeline()->bool:
    log_stream = StringIO()
    """
    Pipeline performs the ETL from the currency exchange API and loads the data in Postgres in Upsert manner
    """
    logging.basicConfig(stream=log_stream, level=logging.INFO, format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")
    logging.info("Commencing Extract")
    metadata_logger = MetadataLogging()
    metadata_logger_table = "my_log_table"
    metadata_logger_run_id = metadata_logger.get_latest_run_id(db_table=metadata_logger_table)
    with open("config.yaml") as yaml_file:
        config = yaml.safe_load(yaml_file)
    # SETTING ENVIRONMENT VARIABLES
    import os
    api_key = os.environ.get("api_key")
    db_user = os.environ.get("db_user")
    db_password = os.environ.get("db_password")
    db_server_name = os.environ.get("db_server_name")
    db_database_name = os.environ.get("db_database_name")

    try:
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="started",
            run_id=metadata_logger_run_id, 
            run_config=config,
            db_table=metadata_logger_table
        )

        logging.info("Extract Complete")
        logging.info("Commencing Transform")

        # EXTRACT(Parsing Arguments)
        df_currency = Extract.extract(
        api_key=api_key,
        start_date=config["extract"]["start_date"],
        end_date=config["extract"]["end_date"],
        symbols=config["extract"]["symbols"],
        base=config["extract"]["base"]      
        )
            
        # TRANSFORM (Parsing Argument)
        df_currency_selected = Transform.transform(df_currency)

        logging.info("Transform Complete")

        # IMPORT SQLALCHEMY LIBRARIES

        #from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
        #from sqlalchemy.engine import URL
        #from sqlalchemy.dialects import postgresql
        #from secret_configs import db_user, db_password, db_server_name, db_database_name
        #from sqlalchemy.schema import CreateTable

        # LOADING TO DATABASE

        logging.info("Commencing Load to File")

        #IMPORT SQLALCHEMY LIBRARIES
        from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
        from sqlalchemy.engine import URL
        from sqlalchemy.schema import CreateTable

        import os
        api_key = os.environ.get("api_key")
        db_user = os.environ.get("db_user")
        db_password = os.environ.get("db_password")
        db_server_name = os.environ.get("db_server_name")
        db_database_name = os.environ.get("db_database_name")

        connection_url = URL.create(
        drivername = "postgresql+pg8000",
        username = db_user,
        password = db_password,
        host = db_server_name,
        port = 5432,
        database = db_database_name,
        )

        engine = create_engine(connection_url)


        Load.load(df_currency_selected, engine)

        logging.info("Load Completed")

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_logger_run_id, 
            run_config=config,
            run_log=log_stream.getvalue(),
            db_table=metadata_logger_table
        )

    except BaseException as e:
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="error",
            run_id=metadata_logger_run_id, 
            run_config=config,
            run_log=log_stream.getvalue(),
            db_table=metadata_logger_table
        )

    return True

if __name__ == "__main__":
    # run etl pipeline
    if pipeline():
        print("success")