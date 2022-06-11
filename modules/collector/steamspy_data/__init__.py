import pandas as pd
import json
import requests
import datetime
import time
from tqdm import tqdm
from utilities.exceptions import ServiceUnavailableException, TooManyRequestsException, NotFoundException

STEAMSPY_ENDPOINT = 'https://steamspy.com/api.php?request=appdetails&appid={}'


def get_steamspy_df(appids, basket_timelimit=60, basket_countlimit=60):
    function_desc = 'collecting SteamSpy data'
    results = []

    basket_start = datetime.datetime.now()
    total_count = 0
    for appid in tqdm(appids, desc=function_desc.upper()):
        try:
            basket_duration = (datetime.datetime.now() - basket_start).seconds
            total_count += 1

            if basket_duration >= basket_timelimit:
                basket_start = datetime.datetime.now()

            if total_count / basket_countlimit == total_count // basket_countlimit:
                time.sleep(basket_timelimit - basket_duration + 1)

            response = requests.get(STEAMSPY_ENDPOINT.format(appid))

            if response.status_code == 503:
                raise ServiceUnavailableException
            if response.status_code == 429:
                raise TooManyRequestsException
            if response.status_code == 404:
                raise NotFoundException

            response_formatted = pd.json_normalize(json.loads(response.content))
            response_formatted.columns = [col.replace(f'{appid}.', '') for col in response_formatted.columns]
            results.append(response_formatted)

        except TooManyRequestsException:
            print(f'Too many requests')
            print(
                f'Basket duration: {basket_duration} | Basket count: {total_count - total_count // basket_countlimit * basket_countlimit}')
            return pd.concat(results)
        except ServiceUnavailableException:
            print(f'Service unavailable for app {appid} ')
        except NotFoundException:
            print(f'App {appid} not found ')
        except:
            raise

    return pd.concat(results)


def get_tags_df(df):
    columns = ['appid']
    for x in df.columns:
        if x.split('.')[0] == 'tags':
            columns.append(x)

    result = df[columns]
    result.columns = ["_".join(x.replace('tags.', '').lower().replace('-', '_').split()) for x in result.columns]
    return result.reset_index(drop=True)


# IMPORTANT QUESTION: is it better to divide by the max or by the sum of a row?
# for now, I will keep the sum
def normalize_tags_df(df):
    df = df.drop('tags', axis=1, errors='ignore')
    df = df.set_index('appid')
    df = df.dropna(how='all')
    df = df.fillna(0)
    df = df.divide(df.max(axis=1), axis=0)

    return df.reset_index()


def clean_steam_spy_df(df):
    df = df[['appid', 'positive', 'negative', 'average_forever', 'median_forever']].rename(columns={
        'average_forever': 'playtime_mean', 'median_forever': 'playtime_median'})

    return df.reset_index(drop=True)


def get_playtime_df(df):
    playtime_df = df[['appid', 'playtime_mean', 'playtime_median']]
    return playtime_df
