"""
Copyright (C) 2020  Rusne Sileryte
Modified based on the original code under the same license available at https://github.com/rusne/geoFluxus
rusne.sileryte@gmail.com

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""

"""
This module:
1) Match company with KvK data to assign NACE codes
2) Validate NACE with EWC code
"""

import logging
import pandas as pd
import geopandas as gpd
from shapely import wkt
import variables as var
from fuzzywuzzy import fuzz

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


def prepare_KvK_data(KvK_actors):
    """
    Extract relevant actors from the LMA dataset and connect them with
    the KvK companies within their search space
    """

    KvK_actors['activenq'] = KvK_actors['activenq'].astype(str).str.zfill(4).str[:4]

    # rename columns
    KvK_actors = KvK_actors[['zaaknaam', 'orig_zaaknaam', 'adres', 'postcode',
                             'activenq', 'AG', 'key', 'wkt']]
    KvK_actors.columns = ['KvK_name', 'KvK_origname', 'KvK_address',
                          'KvK_postcode', 'KvK_sbi', 'KvK_ag', 'KvK_key',
                          'KvK_loc']

    KvK_actors['KvK_loc'] = KvK_actors['KvK_loc'].apply(wkt.loads)
    KvK_actors_geo = gpd.GeoDataFrame(KvK_actors,
                                      geometry="KvK_loc", crs={"init": "epsg:28992"})

    return KvK_actors_geo


def prepare_LMA_data(LMA_dataframe):
    # extract ontdoeners from LMA dataset to connect NACE
    connect_nace = var.connect_nace
    logging.info(f"Extract {connect_nace}s...")
    ontdoener_columns = [col for col in LMA_dataframe.columns if connect_nace in col]
    ontdoener_columns.append('EuralCode')
    # ontdoener_columns.append('RouteInzameling')
    ontdoeners = LMA_dataframe[ontdoener_columns]
    ontdoeners.columns = [col.split("_")[-1] for col in ontdoener_columns]
    ontdoeners = ontdoeners.rename(columns={connect_nace: "Name"})

    # # taking out route actors first
    # route = ontdoeners[ontdoeners["RouteInzameling"] == 'J']
    # if len(route.index):
    #     logging.info(f"{len(route.index)} {connect_nace}s removed due to en route collection...")
    # ontdoeners = ontdoeners[ontdoeners['RouteInzameling'] == 'N']
    # ontdoeners.drop(columns=["RouteInzameling"])

    # remove any ontdoeners with no location
    missing_locations = ontdoeners[ontdoeners["Location"].isnull()]
    if len(missing_locations.index):
        logging.warning(f"{len(missing_locations.index)} {connect_nace}s with missing locations removed...")
        ontdoeners.dropna(subset=["Location"], inplace=True)

    # further matching only happens for the actors inside the boundary
    # load casestudy boundary
    logging.info("Import casestudy boundary...")
    try:
        boundary = gpd.read_file("Spatial_data/Metropoolregio_RDnew.shp")
    except Exception as error:
        logging.critical(error)
        raise

    # convert wkt to geometry
    locations = ontdoeners["Location"].apply(wkt.loads)
    LMAgdf = gpd.GeoDataFrame(locations, geometry="Location", crs={"init": "epsg:28992"})
    joined = gpd.sjoin(LMAgdf, boundary, how="left", op="within")

    # join with study area
    in_boundary = joined[joined["OBJECTID"].isna() == False].index
    out_boundary = joined[joined["OBJECTID"].isna()].index
    ontdoeners.loc[in_boundary, "area_in"] = True
    ontdoeners.loc[out_boundary, "area_out"] = False
    LMA_inbound = ontdoeners[ontdoeners["area_in"] == True]
    # LMA_inbound = ontdoeners

    # prepare key to match with KvK dataset (clean name + postcode)
    LMA_inbound["Key"] = LMA_inbound["Name"].str.cat(LMA_inbound[["Postcode"]], sep=" ")
    logging.info(f"Ontdoener in boundary: {LMA_inbound['Key'].nunique()}")

    # concatenate EWC codes into a list
    LMA_inbound = LMA_inbound.astype(str).groupby('Key').agg(lambda x: ','.join(x.unique()))

    # rename columns
    LMA_inbound.reset_index(inplace=True)
    LMA_inbound = LMA_inbound[['Key', 'Name', 'Origname', 'Adres', 'Location',
                               'EuralCode']]
    LMA_inbound.columns = ['LMA_key', 'LMA_name', 'LMA_origname', 'LMA_address',
                           'LMA_loc', 'LMA_eural']

    return LMA_inbound


def resolve_duplicates(dataframe):
    """
    in case multiple matches are identified, this function resolves them
    using a 3-step approach
    all duplicates that cannot be resolved using those steps are returned as unmatched
    """
    matches = dataframe.copy()

    # making sure that removing duplicates always leaves the most similar ones
    matches.sort_values(by=['ratio', 'dist', 'LMA_key'], ascending=[False, True, True], inplace=True)

    matches['nace_count'] = matches.groupby('LMA_key')['KvK_sbi'].transform("nunique")
    # if all duplicates refer to the same NACE/SBI code
    # then any can be chosen
    resolved_4dig = matches[matches['nace_count'] == 1]
    resolved_4dig.drop(columns=['nace_count'], inplace=True)
    resolved_4dig.drop_duplicates(subset=['LMA_key'], inplace=True)
    resolved_4dig['match'] = resolved_4dig['match'] + 'a'

    matches = matches[(matches['LMA_key'].isin(resolved_4dig['LMA_key']) == False)]

    # # if all duplicates refer to the same first 2 digits of the NACE/SBI code
    # # then a two digit + 00 code is assigned
    # matches['KvK_sbi2'] = matches['KvK_sbi'].astype(str).str[:2] + '00'
    # matches['nace_count'] = matches.groupby('LMA_key')['KvK_sbi2'].transform("nunique")
    # resolved_2dig = matches[matches['nace_count'] == 1]
    # resolved_2dig['KvK_sbi'] = resolved_2dig['KvK_sbi2']
    # resolved_2dig.drop(columns=['KvK_sbi2', 'nace_count'], inplace=True)
    # resolved_2dig.drop_duplicates(subset=['LMA_key'], inplace=True)
    # resolved_2dig['match'] = resolved_2dig['match'] + 'b'
    #
    # matches = matches[(matches['LMA_key'].isin(resolved_2dig['LMA_key']) == False)]

    # if there are still duplicates remaining
    # then priority is given to the combined closest one
    # if "dist" not in matches.columns:
    #     matches = compute_distances(matches)

    # !!! find closest match by name and closest match by distance and check if it is the same
    # if matches['match'].str[0].any() == '2':
    #     resolved_combined = pd.DataFrame()
    # else:
    closest_name = matches.loc[matches.groupby(["LMA_key"])["ratio"].idxmax()]
    closest_point = matches.loc[matches.groupby(["LMA_key"])["dist"].idxmin()]
    resolved_combined = closest_name[closest_name.index.isin(closest_point.index)]
    resolved_combined['match'] = resolved_combined['match'] + 'b'

    # resolved = pd.concat([resolved_4dig, resolved_2dig, resolved_combined])
    resolved = pd.concat([resolved_4dig, resolved_combined])

    # check how many could not be resolved
    if resolved['LMA_key'].nunique() != dataframe['LMA_key'].nunique():
        e = dataframe['LMA_key'].nunique() - resolved['LMA_key'].nunique()
        # logging.warning(f'{e} duplicates could not be resolved')

    return resolved


def compute_distances(search_space):
    # geo distance
    if (search_space["LMA_loc"].dtype == object):
        search_space["LMA_loc"] = search_space["LMA_loc"].apply(wkt.loads)
    if (search_space["KvK_loc"].dtype == object):
        search_space["KvK_loc"] = search_space["KvK_loc"].apply(wkt.loads)
    search_space["dist"] = search_space.apply(lambda x: x["LMA_loc"].distance(x["KvK_loc"]), axis=1, result_type="reduce")

    # name similarity
    search_space["ratio"] = search_space.apply(lambda x: fuzz.ratio(x.LMA_name, x.KvK_name), axis=1, result_type="reduce")
    # search_space["ratio"] = search_space.apply(lambda x: fuzz.partial_ratio(x.LMA_name, x.KvK_name), axis=1)
    # search_space["ratio"] = search_space.apply(lambda x: fuzz.token_set_ratio(x.LMA_name, x.KvK_name), axis=1)

    return search_space


def match_name_postcode(LMA_data, KvK_data):
    # ______________________________________________________________________________
    # MATCH EXACT KEYS AND NAMES FIRST
    # ______________________________________________________________________________

    by_name_and_address = pd.merge(LMA_data, KvK_data, left_on="LMA_key", right_on="KvK_key")
    by_name_and_address['match'] = '1'
    by_name_and_address = compute_distances(by_name_and_address)
    by_name_and_address = resolve_duplicates(by_name_and_address)

    logging.warning(f"{len(by_name_and_address.index)} actors matched by exact name and postcode")

    # take out those ontdoeners that had not been matched
    remaining = LMA_data[(LMA_data["LMA_key"].isin(by_name_and_address["LMA_key"]) == False)]

    by_name = pd.merge(remaining, KvK_data, left_on="LMA_name", right_on="KvK_name")
    by_name['match'] = '2'
    by_name = compute_distances(by_name)
    by_name = resolve_duplicates(by_name)

    logging.warning(f"{len(by_name.index)} actors matched by exact name")

    # take out those ontdoeners that had not been matched
    remaining = remaining[(remaining["LMA_key"].isin(by_name["LMA_key"]) == False)]
    logging.info(f'{len(remaining)} remaining')

    matched = pd.concat([by_name_and_address, by_name])

    return matched, remaining


def make_search_space(LMA_inbound, KvK_actors_geo):
    # ______________________________________________________________________________
    # REDUCE SEARCH SPACE
    #    only match with the companies within a certain geographical radius
    # ______________________________________________________________________________
    # turn wkt location into geopandas geometry
    LMA_inbound["LMA_loc"] = LMA_inbound["LMA_loc"].apply(wkt.loads)
    LMA_inbound = gpd.GeoDataFrame(LMA_inbound, geometry="LMA_loc", crs={"init": "epsg:28992"})

    LMA_inbound["buffer"] = LMA_inbound["LMA_loc"].buffer(var.buffer_dist)
    buffers = gpd.GeoDataFrame(LMA_inbound, geometry="buffer", crs={"init": "epsg:28992"})

    contains = gpd.sjoin(buffers, KvK_actors_geo, how="inner", op="intersects")
    # retain KvK geometry
    search_space = pd.merge(contains, KvK_actors_geo[['KvK_key', 'KvK_loc']], how='left', on='KvK_key')

    # search_space.to_excel('Testing_data/4_search_space.xlsx')
    return(search_space)


def match(dataframe, l_thresh, g_thresh, step):
    """
    finds matches, makes a new file, removes from the pool
    """

    matched = dataframe[(dataframe['dist'] <= g_thresh) & (dataframe['ratio'] >= l_thresh)]
    matched["match"] = step

    matched = resolve_duplicates(matched)

    # logging.warning(f"{len(matched.index)} actors matched with l-ratio > {l_thresh}, g-dist < {g_thresh}")

    # take out those ontdoeners that had not been matched
    remaining = dataframe[(dataframe['LMA_key'].isin(matched['LMA_key']) == False)]
    # logging.warning(f"{remaining['LMA_key'].nunique()} remaining")

    # check if nothing got lost
    if dataframe['LMA_key'].nunique() != matched['LMA_key'].nunique() + remaining['LMA_key'].nunique():
        e = dataframe['LMA_key'].nunique() - matched['LMA_key'].nunique() - remaining['LMA_key'].nunique()
        logging.warning(f'{e} entries got lost on the way')
        print(dataframe['LMA_key'].nunique(), matched['LMA_key'].nunique(), remaining['LMA_key'].nunique())

    return matched, remaining


def match_criteria(search_space):
    """
    Connect roles with NACE codes from the KvK dataset
    :param search_space:
    :return: dataframe with NACE codes for each role
    """
    search_space = compute_distances(search_space)

    # ______________________________________________________________________________
    # 3. BY NAME SIMILARITY AND GEO PROXIMITY
    #    L-dist >= 85, g-dist =< 5
    # ______________________________________________________________________________

    matched, remaining = match(search_space, var.sim_ratio, 5, '3')
    if len(matched.index):
        control = matched.copy()

    # ______________________________________________________________________________
    # 4. BY NAME SIMILARITY
    #    L-dist >= 85, g-dist <= buffer_dist
    # ______________________________________________________________________________

    matched, remaining = match(remaining, var.sim_ratio, var.buffer_dist, '4')
    if len(matched.index):
        control = control.append(matched.copy())

    # ______________________________________________________________________________
    # 5. BY GEO PROXIMITY
    #    g-dist <= 5
    # ______________________________________________________________________________

    matched, remaining = match(remaining, 0, 5, '5')
    if len(matched.index):
        control = control.append(matched.copy())

    # ______________________________________________________________________________
    # 6. BY COMBINED SIMILARITY
    #    max(L-dist), min(g-dist)
    # ______________________________________________________________________________

    # gets sent immediatelly to resolving duplicates
    matched, remaining = match(remaining, 0, var.buffer_dist, '6')
    if len(matched.index):
        control = control.append(matched.copy())

    c = remaining['LMA_key'].drop_duplicates()

    return control, c


def run(dataframe):
    """
    Connect roles with NACE codes from the KvK dataset
    :param dataframe:
    :return: dataframe with NACE codes for each role
    """
    # import KvK dataset with NACE codes
    # NOTE: this dataset is the result of prepare_kvk.py
    logging.info("Import KvK dataset with geolocation...")
    try:
        global KvK_actors
        KvK_actors = pd.read_csv("Private_data/KvK/2018_KvK.csv", low_memory=False)
    except Exception as error:
        logging.critical(error)
        raise

    # prepare KvK dataset
    logging.info("Prepare KvK data...")
    KvK_actors = prepare_KvK_data(KvK_actors)

    # prepare LMA dataset
    logging.info("Prepare LMA data...")
    LMA_actors = prepare_LMA_data(dataframe)

    # match on name & postcode
    logging.info("Match name & postcode...")
    matched_1, remaining = match_name_postcode(LMA_actors, KvK_actors)

    # match on criteria
    logging.info("Match on criteria...")
    interval = 1000
    steps = int(len(remaining.index) / interval) + 1
    matched_2, unmatched = [], []
    for i in range(steps):
        start, end = i * interval, (i+1) * interval
        search_space = make_search_space(remaining[start:end], KvK_actors)
        df_matched, df_unmatched = match_criteria(search_space)
        matched_2.append(df_matched)
        unmatched.append(df_unmatched)
    matched_2 = pd.concat(matched_2)
    unmatched = pd.concat(unmatched)
    logging.warning(f'{len(unmatched.index)} remain unmatched')

    # collect all matches
    all_nace = pd.concat([matched_1, matched_2])
    all_nace.drop_duplicates(subset=["LMA_key"], inplace=True)


    # add to original dataframe
    role = var.connect_nace
    columns = list(dataframe.columns)
    dataframe["LMA_key"] = dataframe[f"{role}"].str.cat(dataframe[[f"{role}_Postcode"]], sep=" ")
    dataframe = pd.merge(dataframe, all_nace, how="left", on="LMA_key")
    dataframe.rename(columns={'KvK_sbi': f'{role}_activenq', 'KvK_ag': f'{role}_AG'}, inplace=True)
    columns.extend([f'{role}_activenq', f'{role}_AG', 'match'])
    unknown = dataframe[dataframe[f'{role}_AG'].isnull()]["LMA_key"].nunique()
    logging.warning(f"Unknown activity: {unknown}")
    dataframe = dataframe[columns]

    # mark unknown activity
    dataframe.loc[dataframe[f"{role}_AG"].isna(), f"{role}_activenq"] = "0000"
    dataframe.loc[dataframe[f"{role}_AG"].isna(), f"{role}_AG"] = "W"
    dataframe[f"{role}_NACE"] = dataframe[f"{role}_AG"].str.cat(dataframe[f"{role}_activenq"].astype(str).str[:4], sep="-")

    return dataframe
