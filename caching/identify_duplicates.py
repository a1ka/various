import pandas as pd
import requests
import json
import re
from config import SNOWFLAKE_PWD, SNOWFLAKE_USER, SECRET_API_TOKEN, SECRET_PROD_URL
from snowflake_database import create_snowflake_connection, fetch_dataframe_text_query

headers = {'content-type': 'application/json', 'x-auth-token': SECRET_API_TOKEN}

# data pulled in snowflake UI due to sqlalchemy limitations and imported as csv 
# snowflake_engine = create_snowflake_connection(
#     user=SNOWFLAKE_USER,
#     password=SNOWFLAKE_PWD,
#     database="DATABASE",
#     schema="SCHEMA",
# )


def get_requests_data():
    result = pd.read_csv('result.csv', index_col=None)
    result = result[['DATA_SOURCE', 'ALL_FIELDS']]

    return result


def load_caching_tool_data():
    with open('caching_tool_export.json') as f:
        data = json.load(f)

    caching_tool_df = pd.DataFrame(data, columns=['cache_duration', 'refresh_interval', 'source_request'])
    caching_tool_df = pd.concat(
        [caching_tool_df.drop(['source_request'], axis=1), caching_tool_df['source_request'].apply(pd.Series)], axis=1)

    for field in caching_tool_df['fields']:
        field.sort()
    caching_tool_df['combined_col'] = caching_tool_df.apply(lambda r: r['fields'] + [r['data_source']], axis=1)
    caching_tool_df.sort_values(by='combined_col', ascending=True)

    caching_tool_df['previous_row_combined'] = caching_tool_df['combined_col'].shift(1).fillna('0')
    for item in caching_tool_df['combined_col']:
        listToStr = ' '.join([str(elem) for elem in item])
    caching_tool_df['combined_col'] = caching_tool_df['combined_col'].astype(str)

    return caching_tool_df


def add_id_to_caching_tool_model():
    caching_tool_data = load_caching_tool_data()

    i = 1
    for index in range(len(caching_tool_data)):
        for key in caching_tool_data[index]:
            if key == "source_request":
                my_dict = caching_tool_data[index][key]
                my_dict = my_dict.update(_id=i)
                i = i+1

    with open("caching_tool_cache_input.txt", "w") as output:
        output.write(str(caching_tool_data))


def find_duplicates_in_caching_tool():
    caching_tool_data = load_caching_tool_data()
    duplicate_rows_df = caching_tool_data[caching_tool_data.duplicated(['combined_col'])]
    duplicate_ids = duplicate_rows_df[['_id']]

    return duplicate_ids

if __name__ == '__main__':
    find_duplicates_in_caching_tool()
