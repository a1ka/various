import pandas as pd
import json
import re
from config import SNOWFLAKE_PWD, SNOWFLAKE_USER, SECRET_API_TOKEN, SECRET_PROD_URL
from caching_tool.taxonomy import CallError, validate_models

headers = {'content-type': 'application/json', 'x-auth-token': SECRET_API_TOKEN}


def get_requests_data():
    result = pd.read_csv('result.csv', index_col=None)
    result = result[['DATA_SOURCE', 'ALL_FIELDS']]

    return result


def aggregate():
    platform_requests = get_requests_data()
    platform_requests.columns = ['data_source', 'fields']

    # remove quotes and square brackets
    request_fields = []
    sources = []
    for row in platform_requests['fields']:
        if row is not None:
            row = re.sub('"', '', row).replace('[', '').replace(']', '').split(',')
            row = [i.strip() for i in row]
            row.sort()
            request_fields.append(row)
        else:
            request_fields.append("null")
    for row in platform_requests['data_source']:
        if row is not None:
            row = re.sub('"', '', row).replace('[', '').replace(']', '').split(',')
            row = [i.strip() for i in row]
            row.sort()
            sources.append(row)
        else:
            sources.append("null")
    platform_requests['fields'] = request_fields
    platform_requests['sources'] = sources

    flatten2list(platform_requests['fields'])
    platform_requests['sources'] = platform_requests['sources'].astype(str)

    # remove empty field
    for row in platform_requests['fields']:
        if '' in row:
            row = row.remove('')
    platform_requests = platform_requests[['sources', 'fields']]
    platform_requests.columns = ['data_source', 'fields']

    # remove square brackets
    for row in platform_requests['data_source']:
        row = ''.join([str(elem) for elem in row])
    platform_requests['data_source'] = platform_requests['data_source'].str.replace(']', '').str.replace('[', '')
    platform_requests['data_source'] = platform_requests['data_source'].str.replace("'", '')

    # replace custom tags with corresponding raw field
    for row in platform_requests['fields']:
        l = [row.index(i) for i in row if 'adgroup_tags:' in i]
        if len(l) > 0:
            del row[0:len(l)]
            row.append('adgroup_id')
    for row in platform_requests['fields']:
        l = [row.index(i) for i in row if 'ad_tags:' in i]
        if len(l) > 0:
            del row[0:len(l)]
            row.append('ad_id')
    for row in platform_requests['fields']:
        l = [row.index(i) for i in row if 'campaign_tags:' in i]
        if len(l) > 0:
            del row[0:len(l)]
            row.append('campaign_id')

    new_col = []
    for row in platform_requests['fields']:
        row.append('date')
        row.append('account_id')
        row = list(dict.fromkeys(row))
        new_col.append(row)
        row.sort()
    platform_requests['fields'] = new_col

    platform_requests.sort_values(by=['data_source'], inplace=True)

    # compare to caching_tool data and eliminate already cached models
    ss_data = load_caching_tool_data()
    platform_requests['combined_col'] = platform_requests.apply(lambda r: r['fields'] + [r['data_source']], axis=1)
    platform_requests.sort_values(by='combined_col', ascending=True)
    platform_requests['combined_col'] = platform_requests['combined_col'].astype(str)

    new_models = platform_requests.loc[~platform_requests['combined_col'].isin(ss_data['combined_col'])]
    new_models = new_models[['data_source', 'fields']]

    # validate against husky model
    # for index, row in new_models.iterrows():
    for row in new_models.itertuples(index=True, name='Pandas'):
        try:
            response = validate_models(row, "data_source"), getattr(row, "fields")
    #     response = validate_models(row['data_source'], row['fields'])
            print(response) # xxxxxxx
        except:
            pass

    # create 'source request' level
    caching_config = []
    for config_dict in new_models.to_dict(orient='records'):
        caching_config.append(
            {'source_request': config_dict}
        )

    with open("file.txt", "w") as output:
        output.write(str(caching_config))

    return platform_requests #TODO new_models


def analyze_requests():
    requests = aggregate()
    requests['dimensions'] = ''
    requests['metrics'] = ''
    field_type = taxonomy()
    field_type_dim = field_type[field_type['field_type'] == 'dimension']['slug'].tolist()
    field_type_met = field_type[field_type['field_type'] == 'metric']['slug'].tolist()

    # add dimensions and metrics columns to divide fields into dimensions/metrics
    new_dim = []
    new_met = []
    for row in requests['fields']:
        new_dim_sub = []
        new_met_sub = []
        for i in row:
            if i in field_type_dim:
                new_dim_sub.append(i)
            elif i in field_type_met:
                new_met_sub.append(i)
            else:
                pass
        new_dim.append(new_dim_sub)
        new_met.append(new_met_sub)
    requests['dimensions'] = new_dim
    requests['metrics'] = new_met

    # define unique dimensions per data_source
    unique_dimensionets = requests[['data_source', 'dimensions']]

    sources = list(unique_dimensionets['data_source'].drop_duplicates())

    # validate models and remove invalid ones
    requests_dict = requests[['data_source', 'fields']].to_dict(orient='records')


    remove_flag = []
    for row in requests_dict:
        data_source = []
        data_source.append(row['data_source'])
        fields = row['fields']
        validate_models(data_source, fields)
        if 1==1:
            remove_flag.append('1')
        else:
            remove_flag.append('0')
    requests['duplicate_flag'] = remove_flag


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


def compare_fields(row):
    diff = set(row['fields']) - set(row['previous_row_fields'])
    return diff


def flatten2list(object):
    gather = []
    for item in object:
        if isinstance(item, (list, tuple, set)):
            gather.extend(flatten2list(item))
        else:
            gather.append(item)
    return gather


def check_if_exists_in_caching_tool():
    platform_df = aggregate()
    caching_tool_df = load_caching_tool_data()
    print(caching_tool_df)
    print(platform_df)
    ss_models = platform_df.loc[platform_df['combined_col'].isin(caching_tool_df['combined_col'])]
    print(ss_models)


if __name__ == '__main__':
    aggregate()
