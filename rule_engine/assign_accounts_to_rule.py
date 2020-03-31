import pandas as pd
import requests
import logging
import sqlalchemy

from companydatacol.logging.configure import configure_logging

from config.dotenv import load_env
load_env()

from config.app import AppSettings
app_config = AppSettings()
configure_logging(env=app_config.environment)

from config.setup import configure_bugsnag
configure_bugsnag()

from utils.config import SECRET_API_TOKEN, SECRET_PROD_URL, SNOWFLAKE_PWD, SNOWFLAKE_USER
from utils.snowflake_database import create_snowflake_connection, fetch_dataframe_text_query


def load_rule_account_ids():
    headers = {'content-type': 'application/json', 'x-auth-token': SECRET_API_TOKEN}
    # load existing rule ids as json
    resp = requests.get(f'{SECRET_PROD_URL}/api/data', headers=headers)
    resp.raise_for_status()
    data = resp.json()['data']
    # flatten json and convert to dataframe
    rule_df = pd.DataFrame()
    for rule in data:
        rule_keys = ['platform', 'id', 'scope']
        df = pd.DataFrame(rule['attachments'])[['account_id']]
        for key in rule_keys:
            df[key] = [rule[key] for x in range(len(df))]
        df = df.append(df).drop_duplicates()
        rule_df = rule_df.append(df)

    return rule_df


def load_account_ids():
    # get unique company ids from rule engine
    company_ids = tuple(load_rule_account_ids()['scope'].drop_duplicates())
    # run query limited to these company ids
    snowflake_engine = create_snowflake_connection(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PWD,
    )
    query_string = sqlalchemy.text(f'''
                            SELECT
                                COMPANY_ID as company_id,
                                ACCOUNT_ID as account_id,
                                case when PLATFORM = 'facebook_old' then 'facebook' else PLATFORM end as data_source
                            FROM "DATABASE"."SCHEMA"."TABLE"
                            WHERE company_id IN (:company_ids)''')
    result = fetch_dataframe_text_query(query=query_string, args={'company_ids': company_ids},
                                        connection=snowflake_engine)
    snowflake_engine.dispose()
    result['company_id'] = result['company_id'].astype(str)

    return result


def create_links(rule_url, payload):
    # load parameters to API url
    url = f'{rule_url}/links'
    # send request to API
    resp = requests.post(url, headers={'x-auth-token': SECRET_API_TOKEN}, json=payload)
    resp.raise_for_status()


def run_rule(rule_id):
    # send request to API
    resp = requests.post(f'{SECRET_PROD_URL}/api/{rule_id}/run',
                         headers={'x-auth-token': SECRET_API_TOKEN})
    resp.raise_for_status()


def add_account_ids_to_rule_engine():
    rule_df = load_rule_account_ids()
    accounts_df = load_account_ids()
    distinct_rules = rule_df[['platform', 'id', 'scope']].drop_duplicates()
    distinct_rules = distinct_rules.sort_values(by='platform', ascending=False)
    missing_accounts = accounts_df.loc[~accounts_df['account_id'].isin(rule_df['account_id'])]
    missing_accounts = missing_accounts.loc[missing_accounts['data_source'].isin(rule_df['platform'])]
    if len(missing_accounts) == 0:
        logging.info(f'No action needed. No accounts to add.')
    else:
        final_accounts = pd.merge(missing_accounts, distinct_rules, left_on=['data_source', 'company_id'],
                                  right_on=['platform', 'scope'], how='inner')
        final_accounts_dict = final_accounts[['account_id', 'data_source', 'id']].to_dict(orient='records')
        if (len(final_accounts_dict)) == 0:
            logging.info(f'No action needed. All accounts are assigned.')
        for acc in final_accounts_dict:
            # create API url
            rule_url = f'{SECRET_PROD_URL}/api/{acc["id"]}'
            # create request payload
            payload = {"account_id": acc['account_id'], "state": "A"}
            # send request to API
            create_links(rule_url, payload)
            # show me what you're doing
            logging.info(f'''Assigning following account_id: {payload}
                {rule_url}''')
            # execute the rule
            run_rule(acc["id"])


if __name__ == '__main__':
    add_account_ids_to_rule_engine()
