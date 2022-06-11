import pandas as pd
import numpy as np
import requests
import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm
from utilities.exceptions import NotFoundException
pd.options.mode.chained_assignment = None  # default='warn'

PLAYERS_ENDPOINT = 'https://steamplayercount.com/app/{}'


def get_player_stats(appid):
    try:
        response = requests.get(PLAYERS_ENDPOINT.format(appid))

        if response.status_code == 404:
            raise NotFoundException

        soup = BeautifulSoup(response.text, 'html.parser')

        tables = [
            [
                [td.get_text(strip=True) for td in tr.find_all(lambda tag: tag.name == 'td' or tag.name == 'th')]
                for tr in table.find_all('tr')
            ]
            for table in soup.find_all(class_='breakdown-table')
        ]

        result = pd.DataFrame(data=tables[0][1:], columns=tables[0][0])

        return result
    except NotFoundException:
        return np.nan


def clean_players_df(df, release_date):
    df = df[['Month', 'Peak', 'Min Daily Peak', 'Avg Daily Peak']]
    df['Month'] = pd.to_datetime(df['Month'])
    df.columns = ['month', 'peak', 'min_peak', 'mean_peak']

    for col in ['peak', 'min_peak', 'mean_peak']:
        df[col] = [int(x.replace(',', '')) for x in df[col]]

    release_month = datetime.datetime(release_date.year, release_date.month, 1)
    df = df[df['month'] >= release_month]

    return df.sort_values('month').reset_index(drop=True)


def get_all_player_stats(appids, release_dates):
    function_desc = 'collecting player stats'
    results = {}

    for appid, release_date in tqdm(zip(appids, release_dates), total=len(appids), desc=function_desc.upper()):
        temp_players_df = get_player_stats(appid)
        results[appid] = np.nan
        if type(temp_players_df) != float:
            results[appid] = clean_players_df(temp_players_df, release_date)

    return results


def get_player_info_df(player_stats):
    appids = [appid for appid in player_stats]

    peak_launch = [player_stats[appid].iloc[0]['peak'] if type(
        player_stats[appid]) != float else np.nan for appid in player_stats]

    peak_year_mean = [round(player_stats[appid].iloc[1:12]['mean_peak'].mean(), 2) if type(
        player_stats[appid]) != float else np.nan for appid in player_stats]

    result = pd.DataFrame({'appid': appids, 'peak_launch': peak_launch, 'peak_year_mean': peak_year_mean})
    return result
