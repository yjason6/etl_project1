from etl import extract as Extract
import pandas as pd
import os


def test_extract():
    # ASSEMBLE
    api_key = os.environ.get("api_key")
    start_date="2022-01-01"
    end_date="2022-01-01"
    symbols="USD"
    base="AUD" 

    # EXPECTED DATAFRAME

    extract_mock_variables = {
        'success':[True],
        'timeseries':[True],
        'start_date':["2022-01-01"],
        'end_date':["2022-01-01"],
        'base':["AUD"],
        'rates':[{'USD': 0.72685}]
    }
    
    index = ['2022-01-01']  
    df_expected = pd.DataFrame(extract_mock_variables, index=index)
    # print(df_expected)
    # ACT
    df_output = Extract.extract(api_key=api_key, start_date=start_date, end_date=end_date, symbols=symbols, base=base)
    # print(df_output)
    # ASSERT

    pd.testing.assert_frame_equal(left=df_output, right=df_expected, check_exact=True)

