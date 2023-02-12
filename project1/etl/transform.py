import pandas as pd

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