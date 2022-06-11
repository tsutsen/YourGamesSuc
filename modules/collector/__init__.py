import os
import numpy as np
from varname.helpers import Wrapper
from modules.collector import steam_data, igdb_data, steamspy_data, prices_data, players_data
from utilities import savior

DATA_PATH = 'data/'


def update_data():

    # STEAM DATA
    print('fetching appids')

    old_appids = []
    if os.path.exists(DATA_PATH+'summary_df.pkl'):
        old_appids = list(savior.load(DATA_PATH+'all_appids.pkl'))

    # use only new appids for Steam data and IGDB data (except critic scores)
    # use all appids for prices, players, SteamSpy

    # I do not use these appids during testing not to waste extra time
    all_apps = steam_data.get_all_apps()
    all_appids = list(all_apps['appid'])

    new_appids = []
    for x in all_appids:
        if not (x in old_appids):
            new_appids.append(x)

    appids_test_1 = [620, 730, 424141343, 216938]
    appids_test_2 = [620, 630, 14850]
    appids_test_3 = []
    appids_test_4 = [620]

    steam_df_raw = steam_data.get_basic_info(new_appids)
    steam_df = steam_data.clean_basic_info_df(steam_df_raw)
    images_df = steam_data.get_images_df(steam_df)
    languages_df = steam_data.get_languages_df(steam_df)
    categories_df = steam_data.get_categories_df(steam_df)
    steam_genres_df = steam_data.get_genres_df(steam_df)
    dlc_df = steam_data.get_dlc_df(steam_df)
    packages_df = steam_data.get_packages_df(steam_df)
    content_descriptors_df = steam_data.get_content_descriptors_df(steam_df)
    requirements_minimum_df = steam_data.get_requirements_minimum_df(steam_df)
    requirements_recommended_df = steam_data.get_requirements_recommended_df(steam_df)
    descriptions_df = steam_data.get_descriptions_df(steam_df)

    appids = list(steam_df.query('coming_soon == False')['appid'])

    # SteamSpy API is faster than rating endpoint of Steam API, and it provides similar data
    # But it lacks user score and score rank – they are always blank for some reason
    # I can reverse engineer Steam user score formula or calculate SteamDB score instead
    # (I think Steam score formula is the number of positive reviews divided by negative, but I'm not sure)
    # It will make data collector faster but more vulnerable for errors on the SteamSpy side
    # But for now, I keep this
    # TODO: reverse-engineer Steam score formula
    rating_df = steam_data.get_rating_df(appids)
    summary_df = steam_df.merge(rating_df[['appid', 'reviews_total']])
    summary_df = summary_df.drop(['header_image', 'background', 'screenshots', 'movies', 'dlc', 'categories', 'genres',
                                  'languages', 'packages', 'content_descriptors_ids', 'content_descriptors_notes',
                                  'pc_requirements_minimum', 'pc_requirements_recommended', 'metacritic_score',
                                  'metacritic_url', 'detailed_description', 'about_the_game', 'short_description'],
                                 errors='ignore', axis=1)

    appids_filter_1 = list(summary_df.query('reviews_total >= 10')['appid'])

    # STEAMSPY
    steam_spy_df_raw = steamspy_data.get_steamspy_df(all_appids)
    steam_spy_df = steamspy_data.clean_steam_spy_df(steam_spy_df_raw)

    playtime_df = steamspy_data.get_playtime_df(steam_spy_df)
    tags_df_raw = steamspy_data.get_tags_df(steam_spy_df_raw)
    tags_df = steamspy_data.normalize_tags_df(tags_df_raw)

    # CONCURRENT PLAYERS
    release_dates = [summary_df[summary_df['appid'] == appid]['release_date'].iloc[0] if appid in list(
        summary_df['appid']) else np.nan for appid in appids_filter_1]
    player_stats_dict = players_data.get_all_player_stats(appids_filter_1, release_dates)
    player_info_df = players_data.get_player_info_df(player_stats_dict)

    # PRICES
    price_stats_dict = prices_data.get_all_prices(all_appids)
    price_info_df = prices_data.get_price_info_df(price_stats_dict)
    summary_df = summary_df.merge(price_info_df, on='appid')

    # OWNERS AND REVENUE
    summary_df['owners'] = steam_data.get_owners(summary_df)
    summary_df['revenue'] = steam_data.get_revenue(summary_df)

    # IGDB
    appid_to_igdbid = igdb_data.get_igdb_ids(new_appids)
    igdb_info_df_raw = igdb_data.get_igdb_info(appid_to_igdbid['igdbid'])
    igdb_info_df = igdb_data.clean_igdb_info(igdb_info_df_raw, appid_to_igdbid)

    # TODO: add condition, check if there are collection and franchise columns in new data, and merge after that
    # TODO: or forcibly add collection and franchise columns if there are none
    summary_df = summary_df.merge(igdb_info_df[['appid', 'age_rating', 'game_engine', 'collection', 'is_collection']])

    platforms_df = igdb_data.get_platforms_df(igdb_info_df, summary_df)
    igdb_genres_df = igdb_data.get_genres_df(igdb_info_df)
    themes_df = igdb_data.get_themes_df(igdb_info_df)
    game_modes_df = igdb_data.get_game_modes_df(igdb_info_df)
    keywords_df = igdb_data.get_keywords_df(igdb_info_df)
    player_perspectives_df = igdb_data.get_player_perspectives_df(igdb_info_df)

    rating_df = rating_df.merge(igdb_info_df[['appid', 'critic_score', 'critic_reviews_total']])

    # all_appids = old_appids + new_appids

    # ugly part needed to save variables under their names
    # wrapping the variables in a for loop does not work – both with 'for x in lst' and 'for i in range(len(lst))'
    # TODO: think again how to de-hardcode this part
    all_appids = Wrapper(all_appids)
    summary_df = Wrapper(summary_df)
    rating_df = Wrapper(rating_df)
    images_df = Wrapper(images_df)
    languages_df = Wrapper(languages_df)
    categories_df = Wrapper(categories_df)
    dlc_df = Wrapper(dlc_df)
    packages_df = Wrapper(packages_df)
    content_descriptors_df = Wrapper(content_descriptors_df)
    requirements_minimum_df = Wrapper(requirements_minimum_df)
    requirements_recommended_df = Wrapper(requirements_recommended_df)
    descriptions_df = Wrapper(descriptions_df)
    playtime_df = Wrapper(playtime_df)
    tags_df = Wrapper(tags_df)
    player_stats_dict = Wrapper(player_stats_dict)
    price_stats_dict = Wrapper(price_stats_dict)
    player_info_df = Wrapper(player_info_df)
    price_info_df = Wrapper(price_info_df)
    appid_to_igdbid = Wrapper(appid_to_igdbid)
    platforms_df = Wrapper(platforms_df)
    steam_genres_df = Wrapper(steam_genres_df)
    igdb_genres_df = Wrapper(igdb_genres_df)
    themes_df = Wrapper(themes_df)
    game_modes_df = Wrapper(game_modes_df)
    keywords_df = Wrapper(keywords_df)
    player_perspectives_df = Wrapper(player_perspectives_df)

    to_save = [all_appids, summary_df, rating_df, images_df, languages_df, categories_df, dlc_df, packages_df,
               content_descriptors_df, requirements_minimum_df, requirements_recommended_df, descriptions_df,
               playtime_df, tags_df, player_stats_dict, price_stats_dict, player_info_df, price_info_df,
               appid_to_igdbid, platforms_df, steam_genres_df, igdb_genres_df, themes_df, game_modes_df, keywords_df,
               player_perspectives_df]

    for x in to_save:
        savior.save(x, DATA_PATH + x.name)
