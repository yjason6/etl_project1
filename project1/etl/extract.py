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