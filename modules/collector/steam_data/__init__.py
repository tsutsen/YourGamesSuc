import pandas as pd
import numpy as np
import re
import json
import requests
import datetime
import time
from tqdm import tqdm
from utilities.exceptions import ServiceUnavailableException, TooManyRequestsException
from utilities import misc

# STEAM API ENDPOINTS
ALL_APPS_ENDPOINT = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
BASIC_INFO_ENDPOINT = "https://store.steampowered.com/api/appdetails?appids={}&cc=us&l=en"
RATING_ENDPOINT = "https://store.steampowered.com/appreviews/{}?json=1&language=all&purchase_type=all&num_per_page=1"

TAG_PATTERN = re.compile('<.*?>')


def get_all_apps():
    response = requests.get(ALL_APPS_ENDPOINT)
    if response.status_code != 200:
        raise Exception(f'Error {response.status_code}')
    response_formatted = pd.json_normalize(json.loads(response.content)['applist']['apps'])

    return response_formatted


# Steam API is rate limited to 200 requests per 5 minutes
def get_basic_info(appids, basket_timelimit=300, basket_countlimit=200):
    function_desc = 'collecting app details from Steam'
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

            response = requests.get(BASIC_INFO_ENDPOINT.format(appid))

            if response.status_code == 503:
                raise ServiceUnavailableException
            if response.status_code == 429:
                raise TooManyRequestsException

            response_formatted = pd.json_normalize(json.loads(response.content))
            response_formatted.columns = [col.replace(f'{appid}.', '') for col in response_formatted.columns]
            results.append(response_formatted)

        except TooManyRequestsException:
            print(f'Too many requests')
            print(
                f'Basket duration: {basket_duration} | Basket count: {total_count - total_count // basket_countlimit * basket_countlimit}')
            return pd.concat(results)
        except ServiceUnavailableException:
            print(f'App {appid} unavailable')
        except:
            raise

    return pd.concat(results)


def remove_tags(string):
    string_clean = re.sub(TAG_PATTERN, ' ', string)
    string_clean = string_clean.replace('&quot;', "'").replace('&gt;', '')
    string_clean = " ".join(string_clean.split())
    return string_clean


def clean_languages(language_series):
    results = []
    for e in language_series:
        if type(e) == float:
            results.append(np.nan)
        else:
            results.append([x.strip() for x in remove_tags(e).replace(
                'languages with full audio support', '').replace('*', '').replace(' -', '').split(',')])

    return results


def extract_from_dict_series(series, dict_key):
    result = []
    for this in [x for x in series]:
        result_this = []
        if type(this) != float:
            for i in range(len(this)):
                result_this.append(this[i][dict_key])
        result.append(result_this)

    return result


def is_iterable(x):
    try:
        iter(x)
        return True
    except:
        return False


def get_now_string(sep=''):
    current_time = datetime.datetime.now()
    return f'{current_time.hour:02d}{sep}{current_time.minute:02d}{sep}{current_time.second:02d}{sep}{current_time.day:02d}{sep}{current_time.month:02d}{sep}{current_time.year - 2000}'


def clean_basic_info_df(df):
    df.columns = [col.replace(
        'data.', '').replace(
        'overview.', '').replace(
        'steam_appid', 'appid').replace(
        '.', '_') for col in df.columns]

    df = df.query('success == True and type == "game"')
    df = df.drop(['price_currency', 'price_initial_formatted',
                  'price_final_formatted', 'success', 'type',
                  'ext_user_account_notice', 'legal_notice',
                  'price_discount_percent', 'price_final',
                  'fullgame_name', 'background_raw', 'reviews',
                  'fullgame_appid', 'package_groups', 'achievements_highlighted',
                  'support_info_url', 'support_info_email',
                  'pc_requirements', 'mac_requirements', 'linux_requirements',
                  'mac_requirements_recommended', 'linux_requirements_recommended',
                  'mac_requirements_minimum', 'linux_requirements_minimum',
                  'required_age', 'is_free', 'recommendations_total',
                  'price_recurring_sub', 'price_recurring_sub_desc'], errors='ignore', axis=1)
    # required_age is incorrect most of the time
    # even with the notes about murder, blood, nudity, etc., the age is still 0

    df = df.rename(columns=({'release_date_coming_soon': 'coming_soon', 'release_date_date': 'release_date',
                             'platforms_windows': 'windows', 'platforms_mac': 'mac', 'platforms_linux': 'linux',
                             'supported_languages': 'languages'}))

    # if there are no such columns for the given appids, do nothing with them
    try:
        df['controller_support'] = df['controller_support'].replace('full', True).fillna(False)
    except KeyError:
        pass

    params_in_json = {'categories':'description',
                      'genres':'description',
                      'demos':'appid',
                      'screenshots':'path_thumbnail'}

    for param, key in params_in_json.items():
        try:
            df[param] = extract_from_dict_series(df[param], key)
        except KeyError:
            pass

    try:
        df['movies'] = [x[0]['480'] if len(x) > 0 else x for x in extract_from_dict_series(df['movies'], 'mp4')]
    except KeyError:
        pass

    df['about_the_game'] = [remove_tags(x) for x in df['about_the_game']]
    df['short_description'] = [remove_tags(x) for x in df['short_description']]
    df['detailed_description'] = [remove_tags(x) for x in df['detailed_description']]

    df['languages'] = clean_languages(df['languages'])

    df['appid'] = df['appid'].astype(int)
    df['release_date'] = [pd.to_datetime(x, errors='coerce') for x in df['release_date']]
    df['coming_soon'] = df['coming_soon'].astype(bool)
    df['windows'] = df['windows'].astype(bool)
    df['mac'] = df['mac'].astype(bool)
    df['linux'] = df['linux'].astype(bool)

    df['release_year'] = [x.year for x in df['release_date']]
    df['dlcs_total'] = [len(x) for x in df['dlc']]
    df['packages_total'] = [len(x) for x in df['packages']]
    df['languages_total'] = [len(x) for x in df['languages']]
    df['screenshots_total'] = [len(x) for x in df['screenshots']]
    df['developers'] = [x[0] if type(x) != float else np.nan for x in df['developers']]
    df['publishers'] = [x[0] if type(x) != float else np.nan for x in df['publishers']]

    for col in df.columns:
        df[col] = [misc.empty_to_nan(x) for x in df[col]]

    return df.reset_index(drop=True)


def get_rating_df(appids, basket_timelimit=300, basket_countlimit=200):
    function_desc = 'collecting rating data'
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

            response = requests.get(RATING_ENDPOINT.format(appid))

            if response.status_code == 503:
                raise ServiceUnavailableException
            if response.status_code == 429:
                raise TooManyRequestsException

            response_formatted = pd.json_normalize(json.loads(response.content)['query_summary'])
            response_formatted.columns = [col.replace(f'{appid}.', '') for col in response_formatted.columns]
            results.append(response_formatted)

        except TooManyRequestsException:
            print(f'Too many requests')
            print(
                f'Basket duration: {basket_duration} | Basket count: {total_count - total_count // basket_countlimit * basket_countlimit}')
            return pd.concat(results)
        except ServiceUnavailableException:
            print(f'App {appid} unavailable')
        except:
            raise

    results = pd.concat(results)
    results['appid'] = appids
    results = results.drop(['review_score_desc'], axis=1)
    results = results.rename(columns={'total_reviews': 'reviews_total'})
    results = results.reindex(list(results.columns[-1:]) + list(results.columns[:-1]), axis='columns')
    return results


def get_images_df(df):
    images_df = df[['appid', 'screenshots_total', 'header_image', 'background', 'screenshots', 'movies']]
    return images_df


def get_dlc_df(df):
    dlc_df = df[['appid', 'dlc', 'dlcs_total']]
    return dlc_df


def get_packages_df(df):
    dlc_df = df[['appid', 'packages', 'packages_total']]
    return dlc_df


def get_content_descriptors_df(df):
    content_descriptors_df = df[['appid', 'content_descriptors_ids', 'content_descriptors_notes']]
    return content_descriptors_df


def get_languages_df(df):
    languages_df = misc.get_dummy_df(df, 'languages')
    return languages_df


def get_categories_df(df):
    categories_df = misc.get_dummy_df(df, 'categories')
    return categories_df


def get_genres_df(df):
    steam_genres_df = misc.get_dummy_df(df, 'genres')
    return steam_genres_df


def get_requirements_df(df, param):
    results = []
    requirements_df = pd.DataFrame()

    try:
        for requirements, appid in zip(df[param], df['appid']):
            if type(requirements) != float:
                result = {'appid': appid}
                for x in requirements.split('<li>')[1:]:
                    temp = [x.strip() for x in remove_tags(x).split(':')]
                    try:
                        result[temp[0]] = temp[1]
                    except:
                        result['Other'] = temp[0]
                results.append(result)

        requirements_df = pd.DataFrame(results)
        requirements_df.columns = ['_'.join(x.lower().split()) for x in requirements_df.columns]

        for x in requirements_df:
            requirements_df[x] = requirements_df[x].replace('n/a', np.nan)

        return requirements_df
    except:
        return requirements_df


def get_requirements_minimum_df(df):
    minimum_requirements_df = get_requirements_df(df, 'pc_requirements_minimum')
    return minimum_requirements_df


def get_requirements_recommended_df(df):
    minimum_requirements_df = get_requirements_df(df, 'pc_requirements_recommended')
    return minimum_requirements_df


def get_descriptions_df(df):
    descriptions_df = df[['detailed_description', 'about_the_game', 'short_description']]
    return descriptions_df


def estimate_owners(reviews, year):
    if year < 2014:
        return reviews * 60
    elif year < 2017:
        return reviews * 50
    elif year < 2018:
        return reviews * 40
    elif year < 2020:
        return reviews * 35
    else:
        return reviews * 30


def estimate_revenue(owners,
                     average_price,
                     platform_cut=0.7,
                     regional_price=0.8,
                     vat=0.93,
                     returns=0.92):
    return round(owners * average_price * platform_cut * regional_price * vat * returns)


def get_owners(df):
    owners = [estimate_owners(reviews, year) for reviews, year in zip(
        df['reviews_total'], df['release_year'])]
    return owners


def get_revenue(df):
    revenue = [estimate_revenue(owners, average_price) for owners, average_price in zip(
        df['owners'], df['mean_price'])]
    return revenue
