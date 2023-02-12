from project1.etl import transform as Transform
import pandas as pd

def test_transform():
    # Assemble - Putting in mock data

    # Setting the scene

    mock_variables = {
        'success':["True", "True", "True"],
        'timeseries':["True", "True", "True"],
        'start_date':["2022-01-01", "2022-01-01", "2022-01-01"],
        'end_date':["2022-12-31", "2022-12-31", "2022-12-31"],
        'base':["AUD", "AUD", "AUD"],
        'rates':["123456", "123456", "123456"]
    }
    
    # Creating Input Dataframe

    index = ['2022-01-01', '2022-01-02', '2022-01-03']  
    df_input = pd.DataFrame(mock_variables, index=index)
    
    # Creating my expected Dataframe

    expected_variables = {
        'date_format':["01-01-2022", "02-01-2022", "03-01-2022"],
        'base_currency':["AUD", "AUD", "AUD"],
        'exchange_rates':["123456", "123456", "123456"],
    }

    df_expected = pd.DataFrame(expected_variables, index=index)

    # ACT

    df_output = Transform.transform(df_currency=df_input)

    # ASSERT

    pd.testing.assert_frame_equal(left=df_output, right=df_expected, check_exact=True)