import pandas as pd
import numpy as np
import requests
import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm
from utilities.exceptions import NotFoundException

PRICES_ENDPOINT = 'https://steampricehistory.com/app/{}'

MAX_DATE = datetime.datetime.now()
MAX_DATE = datetime.datetime(MAX_DATE.year, MAX_DATE.month, 1)


def get_price_stats(appid):
    try:
        response = requests.get(PRICES_ENDPOINT.format(appid))

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


def get_price_periods(df):
    periods = []
    for i in range(len(df['date']) - 1):
        periods.append((df.iloc[i]['date'] - df.iloc[i + 1]['date']).days)
    periods.insert(0, (MAX_DATE - df.iloc[0]['date']).days)
    return periods


def arange_months(df):
    result = []

    min_month = df['date'].min()
    max_month = MAX_DATE

    min_year = min_month.year
    max_year = max_month.year

    years = np.arange(min_year, max_year + 1)
    months = np.arange(12) + 1

    for year in years:
        for month in months:
            date = datetime.datetime(year, month, 1)
            if min_month <= date <= max_month:
                result.append(date)

    return pd.DataFrame(result[::-1]).rename(columns={0: 'month'})


def get_weighted_average_price(df):
    df = df[df['period'] != 0]
    result = df.groupby('month').agg({'price': lambda x: list(x), 'period': lambda x: list(x)}).reset_index()
    result['average_price'] = [np.average(price, weights=period).round(2) for price, period in zip(
        result['price'], result['period'])]

    return result[['month', 'average_price']]


# not optimized: repeating df['month']=... rows
def clean_price_df(df):
    df = df.copy()
    df['Price'] = [float(x.strip('$')) for x in df['Price']]
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.drop(['Gain', 'Discount'], axis=1)
    df.columns = [x.lower() for x in df.columns]

    df['month'] = [datetime.datetime(x.year, x.month, 1) for x in df['date']]
    df = df.merge(arange_months(df), how='outer')
    df = pd.DataFrame(df['month'].rename('date')).merge(df, how='outer').drop_duplicates('date')
    df = df.dropna(subset=['date'])
    df['month'] = [datetime.datetime(x.year, x.month, 1) for x in df['date']]
    df = df.sort_values(['month', 'date'], ascending=False)

    df['price'] = df['price'].fillna(method='bfill')
    df = df.dropna(subset=['price'])

    df['period'] = get_price_periods(df)
    df = df.merge(get_weighted_average_price(df))

    return df.reset_index(drop=True)


def get_all_prices(appids):
    function_desc = 'collecting prices'
    results = {}

    for appid in tqdm(appids, desc=function_desc.upper()):
        temp_price_df = get_price_stats(appid)
        results[appid] = np.nan
        if type(temp_price_df) != float:
            results[appid] = clean_price_df(temp_price_df)

    return results


def get_price_info_df(prices):
    mean_prices = {x: round(prices[x]['average_price'].mean(), 2) if type(
        prices[x]) != float else np.nan for x in prices}
    result = pd.DataFrame.from_dict(mean_prices, orient='index').reset_index()
    result.columns = ['appid', 'mean_price']

    return result
