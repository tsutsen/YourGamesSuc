import pandas as pd
import numpy as np
import requests
from tqdm import tqdm
from utilities import misc


# AUTHORIZATION
def get_access_token(client_id, client_secret):
    auth_response = requests.post(AUTH_REQUEST.format(client_id, client_secret))
    token = auth_response.json()['access_token']
    return token


with open('secrets.txt') as f:
    secrets = f.readlines()

client_id = secrets[0]
client_secret = secrets[1]
AUTH_REQUEST = f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials'
access_token = get_access_token(client_id, client_secret)
AUTH = {"Client-ID": f"{client_id}", "Authorization": f"Bearer {access_token}"}

# IGDB API ENDPOINTS
BRIDGE_ENDPOINT = 'https://api.igdb.com/v4/external_games'
IGDB_INFO_ENDPOINT = 'https://api.igdb.com/v4/games'


def send_igdb_request(endpoint, request_data, ids_to_get, function_desc='collecting IGDB data'):
    function_desc = function_desc
    limit = 500
    end = 0
    responses = []

    pbar = tqdm(total=len(ids_to_get) // limit + 1, desc=function_desc.upper())
    count = 0

    while end < len(ids_to_get):
        start, end = end, end + limit
        ids = ','.join(str(e) for e in ids_to_get[start:end])

        response = requests.post(endpoint, data=request_data.format(ids, limit), headers=AUTH)
        response_formatted = pd.json_normalize(response.json())

        responses.append(response_formatted)
        count += 1
        pbar.update(count)

    pbar.close()

    result = pd.concat(responses)
    return result


def get_igdb_ids(appids):
    raw_data = '''
        fields game, uid, name; 
        where category=1 & uid=({});
        limit {};'''

    result = send_igdb_request(BRIDGE_ENDPOINT, raw_data, appids, function_desc='collection IGDB ids')

    result = result.drop('id', axis=1)
    result = result.rename(columns={'uid': 'appid', 'game': 'igdbid'})
    result = result.reindex(['appid', 'igdbid', 'name'], axis="columns")
    result['appid'] = result['appid'].astype(int)
    return result


def get_age_rating(rating_ids):
    result = []
    allowed_ratings = list(np.arange(12) + 1)

    if type(rating_ids) != float:
        for rating_id in rating_ids:
            if rating_id in allowed_ratings:
                result.append(rating_id)
    else:
        return np.nan

    if len(result) > 0:
        if result[0] == 1 or result[0] == 7:
            return 3
        if result[0] == 2 or result[0] == 8 or result[0] == 9:
            return 7
        if result[0] == 3 or result[0] == 10:
            return 12
        if result[0] == 4 or result[0] == 11:
            return 16
        if result[0] == 5 or result[0] == 12:
            return 18
    else:
        return np.nan


def get_igdb_info(igdbids):
    raw_data = '''
        fields 
            name, 
            age_ratings.rating, 
            collection, 
            franchise, 
            game_modes.name, 
            game_engines.name, 
            genres.name, 
            keywords.name, 
            multiplayer_modes, 
            platforms.name, 
            player_perspectives, 
            themes.name,
            aggregated_rating,
            aggregated_rating_count;
        where 
            id = ({});
        limit
            {};'''

    result = send_igdb_request(IGDB_INFO_ENDPOINT, raw_data, igdbids)

    return result


game_engine_variants = {
    'BigWorld': ['BigWorld Technology'],
    'Cocos2d': ['Cocos2d-x'],
    'Construct': ['Construct 2', 'Construct 3'],
    'CryEngine': ['CryEngine 3', 'CryEngine 5', 'CryEngine V'],
    'C++': ['Custom C++ Backend'],
    'C': ['Custom C Backend'],
    'EGO Engine': ['EGO Engine 4.0'],
    'Essence Engine': ['Essence Engine 5'],
    'Evolution Engine': ['Evolution'],
    'Frostbite': ['Frostbite 3'],
    'GameMaker': ['Game Maker', 'Game Maker Studio', 'Game Maker Studio 2', 'GameMaker Studio', 'GameMaker Studio 2',
                  'GameMaker: Studio'],
    'Glacier': ['Glacier 2'],
    'Godot Engine': ['Godot'],
    'HPL Engine': ['HPL'],
    'Havok Physics': ['Havok'],
    'Havok Vision Engine': ['Vision', 'Vision Engine', 'Havok Vision Game Engine', 'Trinity Vision Engine'],
    'Visionaire': ['Visionaire Studio'],
    'In-house engine': ['In house engine'],
    'XNA': ['Microsoft XNA', 'XNA Game Studio'],
    'Nvidia PhysX': ['PhysX'],
    'Phoenix Engine': ['Pheonix 3D engine', 'Phoenix Engine (Relic)', 'Phoenix Engine (Wolfire)', 'Phoenix VR'],
    'RPG Maker': ['RPG Maker 2003', 'RPG Maker MV', 'RPG Maker MZ', 'RPG Maker VX', 'RPG Maker VX Ace', 'RPG Maker XP',
                  'Rpgmaker'],
    'Serious Engine': ['Serious Engine 4', 'Serious Engine 4.0'],
    'Siglus': ['SiglusEngine'],
    'Source': ['Source 2'],
    'TW Engine': ['TW Engine 2', 'TW Engine 3'],
    'Torque': ['Torque 2D', 'Torque 3D', 'Torque Game Engine'],
    'Unreal Engine': ['UE4', 'unreal 4', 'UE4 - duplicate', 'Unreal', 'Unreal Engine 2', 'Unreal Engine 2.5',
                      'Unreal Engine 3', 'Unreal Engine 4', 'Unreal Engine 5'],
    'Unity': ['unity engine', 'Unity 2017', 'Unity 2018', 'Unity 2019', 'Unity 2020', 'Unity 2021', 'Unity 3d',
              'Unity 4', 'Unity 5', 'Unity3D'],
    'Adobe Flash Player': ['flash'],
    'id Tech': ['id Tech 1', 'id Tech 2', 'id Tech 3', 'id Tech 4', 'id Tech 5', 'id Tech 6', 'id Tech 7'],
    'libGDX': ['libgdx'],
    "Ren'Py": ["Ren'Py Visual Novel Engine", 'renpy']
}

game_engines_to_ban = [
    'Havok Physics', 'Custom built engine',
    'Nvidia PhysX', 'ADV Player HD', 'Multimedia Fusion',
    'SpeedTree', 'PathEngine', 'Aseprite', '(CN) GameEngine 5',
    'KEX', 'Chowdren', 'AGI', 'AZ (Arika Engine)', 'iMUSE', 'OWI Core',
    'AEGIS', 'Corona SDK', 'FNA', 'Angular', 'Spiller', 'Infernal Engine',
    'Symbian', 'Adventure Creator', 'steam', 'NW.js', 'react', 'Southpaw',
    "Internal Piranha Byte's Engine", 'Beam Next Generation',
    'Euphoria Engine', 'ElectronJS', 'VueJS', 'Twilight', 'Adobe Flash Player'
]


def clean_igdb_info(df, id_conversion_table):
    df = df.copy()
    columns_to_format = ['age_ratings', 'game_engines', 'game_modes',
                         'genres', 'keywords', 'platforms', 'themes']

    for col in columns_to_format:
        if col == 'age_ratings':
            key = 'rating'
        else:
            key = 'name'
        df[col] = misc.extract_from_dict_series(df[col], key)
        df[col] = [misc.empty_to_nan(x) for x in df[col]]

    df['id'] = [id_conversion_table[id_conversion_table['igdbid'] == x]['appid'].iloc[0] for x in df['id']]
    df = df.rename(columns={'id': 'appid',
                            'aggregated_rating': 'critic_score',
                            'aggregated_rating_count': 'critic_reviews_total'})

    df['age_ratings'] = [get_age_rating(x) for x in df['age_ratings']]

    for i in range(len(df['game_engines'])):
        if type(df['game_engines'][i]) != float:
            for j in range(len(df['game_engines'][i])):
                if df['game_engines'][i][j] in game_engines_to_ban:
                    df['game_engines'][i].remove(df['game_engines'][i][j])
                else:
                    df['game_engines'][i][j] = misc.get_dict_key_by_value(df['game_engines'][i][j],
                                                                          game_engine_variants)

    df['game_engines'] = [x[0] if type(x) != float else np.nan for x in df['game_engines']]

    df = df.rename(columns={'game_engines': 'game_engine', 'age_ratings': 'age_rating'})

    if 'collection' in df.columns:
        df['is_collection'] = df['collection'].notna()
    else:
        df['is_collection'] = [False] * len(df)

    if 'franchise' in df.columns:
        df['is_franchise'] = df['franchise'].notna()
    else:
        df['is_franchise'] = [False] * len(df)

    return df


def get_platforms_df(df, steam_df):
    platform_dummies = pd.get_dummies(df['platforms'].apply(pd.Series).stack()).groupby(level=0).sum()
    result = df[['appid']].join(platform_dummies)

    result = result.rename(columns={'PC (Microsoft Windows)': 'windows'})
    result.columns = ["_".join(x.lower().split()) for x in result.columns]

    result = result.fillna(0).astype(int)
    result[['windows', 'linux', 'mac']] = steam_df[['windows', 'linux', 'mac']].astype(int)

    return result


def get_player_perspectives_df(df):
    player_perspective_ids = {1: 'first_person',
                              2: 'third_person',
                              3: 'bird_view',
                              4: 'side_view',
                              5: 'text',
                              6: 'auditory',
                              7: 'vr'}

    result = misc.get_dummy_df(df, 'player_perspectives')
    result = result.rename(columns=player_perspective_ids)
    return result


def get_genres_df(df):
    igdb_genres_df = misc.get_dummy_df(df, 'genres')
    return igdb_genres_df


def get_themes_df(df):
    themes_df = misc.get_dummy_df(df, 'themes')
    return themes_df


def get_game_modes_df(df):
    game_modes_df = misc.get_dummy_df(df, 'game_modes')
    return game_modes_df


def get_keywords_df(df):
    keywords_df = misc.get_dummy_df(df, 'keywords')
    return keywords_df
