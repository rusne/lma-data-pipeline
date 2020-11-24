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

KvK_actors = None
nace_ewc = None
control_output = None
control_columns = ["Key", "Origname", "Adres", "orig_zaaknaam", "adres", "activenq", "AG"]
output_columns = ["Key", "Origname", "AG", "activenq"]


def match_by_name_and_address(LMA_inbound):
    """
    Match ontdoeners with KvK dataset by name & address
    :param LMA_inbound:
    :return:
    """
    LMA_inbound1 = LMA_inbound[["Key", "Origname", "Adres", "EuralCode"]].copy()
    LMA_inbound1.drop_duplicates(subset=["Key"], inplace=True)

    by_name_and_address = pd.merge(LMA_inbound1, KvK_actors, left_on="Key", right_on="key")

    # matching control output
    global control_output
    control_output = by_name_and_address[control_columns]
    control_output["match"] = 1

    # OUTPUT BY NAME AND ADDRESS
    output_by_name_address = by_name_and_address[output_columns].copy()
    output_by_name_address["how"] = "by name and address"

    return output_by_name_address


def match_by_name(remaining):
    """
    Match ontdoeners with KvK dataset only by name
    :param remaining:
    :return:
    """
    LMA_inbound2 = remaining[["Key", "Name", "Origname", "Adres", "Location", 'EuralCode']].copy()
    LMA_inbound2.drop_duplicates(subset=["Key"], inplace=True)

    by_name = pd.merge(LMA_inbound2, KvK_actors, left_on="Name", right_on="zaaknaam")
    by_name["wkt"] = by_name["wkt"].apply(wkt.loads)
    by_name["dist"] = by_name.apply(lambda x: x["wkt"].distance(x["Location"]), axis=1, result_type="reduce")

    by_name = pd.merge(by_name, nace_ewc, on=['activenq', 'EuralCode'])
    closest = by_name.loc[by_name.groupby(["Key"])["dist"].idxmin()]

    # matching control output
    control_output_2 = closest[control_columns]
    control_output_2["match"] = 2
    global control_output
    control_output = control_output.append(control_output_2)

    # OUTPUT BY NAME
    output_by_name = closest[output_columns].copy()
    output_by_name["how"] = "by name"

    return output_by_name


def match_by_address(remaining):
    """
    Match ontdoeners with KvK dataset only by address
    :param remaining:
    :return:
    """
    LMA_inbound3 = remaining[["Key", "Origname", "Adres", "Postcode", 'EuralCode']].copy()
    LMA_inbound3.drop_duplicates(subset=["Key"], inplace=True)

    KvK_actors["adres"] = KvK_actors["adres"].str.cat(KvK_actors[["postcode"]], sep=" ") # (TO BE REMOVED IN THE FINAL VERSION)
    by_address = pd.merge(LMA_inbound3, KvK_actors, left_on=["Adres"], right_on=["adres"])

    # find those that got matched to only one NACE group
    by_address["count"] = by_address.groupby(["Key"])["AG"].transform("count")

    # only one activity
    matched_by_address = by_address[by_address["count"] == 1]

    # multiple activities - we need to choose based on NACE-EWC
    ambiguous = by_address[by_address["count"] > 1]
    ambiguous = pd.merge(ambiguous, nace_ewc, on=['activenq', 'EuralCode'])
    ambiguous["count"] = ambiguous.groupby(["Key"])["AG"].transform("count")
    matched_ambiguous = ambiguous[ambiguous["count"] == 1]

    by_address = pd.concat([matched_by_address, matched_ambiguous])

    # matching control output
    control_output_3 = by_address[control_columns]
    control_output_3["match"] = 3
    global control_output
    control_output = control_output.append(control_output_3)

    by_address = by_address[output_columns]
    by_address.drop_duplicates(subset=["Key"], inplace=True)

    # OUTPUT BY ADDRESS
    output_by_address = by_address[output_columns]
    output_by_address["how"] = "by address"

    return output_by_address


def match_by_text_proximity(remaining):
    """
    Match ontdoeners with KvK dataset by name similarity & closest distance
    :param remaining:
    :return:
    """
    KvK_actors["wkt"] = KvK_actors["wkt"].apply(wkt.loads)
    KvK_actors_geo = gpd.GeoDataFrame(KvK_actors[["key", "orig_zaaknaam", "adres", "activenq", "AG", "wkt"]],
                                      geometry="wkt", crs={"init": "epsg:28992"})

    LMA_inbound4 = remaining[["Key", "Origname", "Adres", "Location", 'EuralCode']]
    LMA_inbound4.drop_duplicates(subset=["Key"], inplace=True)
    LMA_inbound4["buffer"] = LMA_inbound4["Location"].buffer(var.buffer_dist)
    buffers = gpd.GeoDataFrame(LMA_inbound4[["Key", "buffer"]], geometry="buffer", crs={"init": "epsg:28992"})

    contains = gpd.sjoin(buffers, KvK_actors_geo, how="inner", op="intersects")

    distances = pd.merge(contains, KvK_actors_geo[["wkt"]], left_on="index_right", right_index=True)
    distances = pd.merge(distances, LMA_inbound4[["Origname", "Adres", "Location", 'EuralCode']], left_index=True, right_index=True)
    distances = pd.merge(distances, nace_ewc, on=['activenq', 'EuralCode'])

    # make sure that geodataframe has the right column set as geometry
    distances = distances.set_geometry("wkt")
    destinations = distances.set_geometry("Location")

    distances["dist"] = distances["wkt"].distance(destinations["Location"])

    fuzz_ratio = [fuzz.ratio(x, y) for x, y in zip(distances["orig_zaaknaam"], distances["Origname"])]
    distances["text_dist"] = pd.Series(fuzz_ratio, index=distances.index)
    fuzz_ratio.clear()

    # distances.reset_index(inplace=True)
    text_distances = distances[distances["text_dist"] >= 50]
    matched_text = text_distances.loc[text_distances.groupby(["Key"])["text_dist"].idxmax()]

    # # matching control output
    control_output_4 = matched_text[control_columns]
    control_output_4["match"] = 4
    global control_output
    control_output = control_output.append(control_output_4)

    matched_by_text_proximity = matched_text[output_columns].drop_duplicates(subset=["Key"])

    # OUTPUT BY TEXT PROXIMITY
    output_by_text_proximity = matched_by_text_proximity.copy()
    output_by_text_proximity["how"] = "by text proximity"

    return output_by_text_proximity, distances


def match_by_geo_proximity(remaining, distances):
    """
    Match ontdoeners with KvK dataset only by closest distance
    :param remaining:
    :param distances:
    :return:
    """
    remaining.drop_duplicates(subset=["Key"], inplace=True)

    distances = distances[distances["Key"].isin(remaining["Key"])]

    closest = distances.groupby(["Key"])["dist"].idxmin()
    matched_by_geo_proximity = pd.merge(remaining[["Key"]], distances.loc[closest], on="Key")

    # matching control output
    control_output_5 = matched_by_geo_proximity[control_columns]
    control_output_5["match"] = 5
    global control_output
    control_output = control_output.append(control_output_5)

    matched_by_geo_proximity = matched_by_geo_proximity[output_columns].drop_duplicates(subset=["Key"])

    # OUTPUT BY PROXIMITY
    output_by_geo_proximity = matched_by_geo_proximity.copy()
    output_by_geo_proximity["how"] = "by geo proximity"

    return output_by_geo_proximity


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
        KvK_actors = pd.read_csv("Private_data/all_KvK.csv", low_memory=False)
        KvK_actors['activenq'] = KvK_actors['activenq'].astype(str).str.zfill(4).str[:4]
    except Exception as error:
        logging.critical(error)
        raise

    # import NACE-EWC validation
    # pairs of valid activity-waste codes
    logging.info("Import NACE-EWC validation...")
    try:
        global nace_ewc
        nace_ewc = pd.read_csv("Private_data/NACE-EWC.csv", low_memory=False)
        nace_ewc['activenq'] = nace_ewc['activenq'].astype(str).str.zfill(4)
    except Exception as error:
        logging.critical(error)

    # extract from KvK dataset only those actors
    # whose activity code is included in NACE-EWC validation
    nace = nace_ewc['activenq'].drop_duplicates()
    KvK_actors = pd.merge(KvK_actors, nace, on='activenq')

    # extract ontdoeners from LMA dataset to connect NACE
    # all other roles have predefined NACE codes
    connect_nace = var.connect_nace
    logging.info(f"Extract {connect_nace}s...")
    ontdoener_columns = [col for col in dataframe.columns if connect_nace in col]
    ontdoener_columns.append('EuralCode')
    ontdoeners = dataframe[ontdoener_columns]
    ontdoeners.columns = [col.split("_")[-1] for col in ontdoener_columns]
    ontdoeners = ontdoeners.rename(columns={connect_nace: "Name"})
    # logging.info(f"Original ontdoeners: {len(ontdoeners.index)}")

    # prepare key to match with KvK dataset (clean name + postcode)
    ontdoeners["Key"] = ontdoeners["Name"].str.cat(ontdoeners[["Postcode"]], sep=" ")

    # first ontdoeners need to be matched with their own locations
    # taking out route actors first
    ontdoeners.loc[ontdoeners["Key"].str.contains("route"), "route"] = "J"
    ontdoeners.loc[ontdoeners["route"] == "J", "Key"] = ontdoeners["Key"].apply(lambda x: str(x).strip(" route"))

    # remove any ontdoeners with no location
    missing_locations = ontdoeners[ontdoeners["Location"].isnull()]
    if len(missing_locations.index):
        logging.warning(f"{len(missing_locations.index)} {connect_nace}s with missing locations removed...")
        ontdoeners.dropna(subset=["Location"], inplace=True)

    # after filtering missing locations, add route info again
    ontdoeners.loc[ontdoeners["route"] == "J", "Key"] = ontdoeners["Key"] + " route"
    total_inbound = ontdoeners['Key'].nunique()
    logging.info(f"{total_inbound} unique {connect_nace}s to connect NACE...")

    # further matching only happens for the actors inside the boundary
    in_boundary = ontdoeners[ontdoeners["AMA"] == True]
    out_boundary = ontdoeners[ontdoeners["AMA"] == False]
    LMA_inbound = in_boundary[ontdoeners.columns]
    LMA_inbound["Location"] = LMA_inbound["Location"].apply(wkt.loads)
    LMA_inbound = gpd.GeoDataFrame(LMA_inbound, geometry="Location", crs={"init": "epsg:28992"})

    # route inzameling gets a separate nace code as well
    route = LMA_inbound[LMA_inbound["route"] == "J"]
    # if len(route.index):
    #     logging.warning(f"Remove {route['Key'].nunique()} ontdoeners belonging to route inzameling")

    LMA_inbound = LMA_inbound[LMA_inbound["route"] != "J"]
    LMA_inbound.drop(columns=["route"])

    # total_inbound = LMA_inbound["Key"].nunique()
    logging.info(f"Ontdoeners within AMA: {LMA_inbound['Key'].nunique()}")

    # ______________________________________________________________________________
    # 1. BY NAME AND ADDRESS
    #    both name and address are the same
    # ______________________________________________________________________________

    output_by_name_address = match_by_name_and_address(LMA_inbound)
    perc = round(len(output_by_name_address.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_name_address.index)} {connect_nace}s matched by name & postcode ({perc}%)")

    # take out those ontdoeners that had not been matched
    remaining = LMA_inbound[(LMA_inbound["Key"].isin(output_by_name_address["Key"]) == False)]

    # ______________________________________________________________________________
    # 2. BY NAME ONLY
    #    geographically closer one gets a priority
    # ______________________________________________________________________________

    output_by_name = match_by_name(remaining)
    perc = round(len(output_by_name.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_name.index)} {connect_nace}s matched only by name ({perc}%)")

    # take out those Ontdoeners that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_name["Key"]) == False)]

    # ______________________________________________________________________________
    # 3. BY ADDRESS ONLY
    # ______________________________________________________________________________

    output_by_address = match_by_address(remaining)
    perc = round(len(output_by_address.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_address.index)} {connect_nace}s matched only by address ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_address["Key"]) == False)]

    # ______________________________________________________________________________
    # 4. BY  GEO AND TEXT PROXIMITY
    #    closest name within a certain radius
    # ______________________________________________________________________________

    output_by_text_proximity, distances = match_by_text_proximity(remaining)
    perc = round(len(output_by_text_proximity.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_text_proximity.index)} {connect_nace} matched with the closest name match "
                    f"in <{var.buffer_dist}m ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_text_proximity["Key"]) == False)]

    # ______________________________________________________________________________
    # 5. BY GEO PROXIMITY
    # ______________________________________________________________________________

    output_by_geo_proximity = match_by_geo_proximity(remaining, distances)
    perc = round(len(output_by_geo_proximity.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_geo_proximity.index)} {connect_nace}s matched by proximity ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_geo_proximity["Key"]) == False)]

    # ______________________________________________________________________________
    # 5. BY KEYWORD
    # ______________________________________________________________________________

    # governmental activities
    keywords = ['GEMEENTE', 'POLITIE', 'RIJKSWATERSTAAT', 'HOGESCHOOL', 'STADSDEEL',
                'BELASTINGDIENST', 'PROVINCIE', 'OPENBARE', 'UNIE', 'UNIVERSITEIT',
                'RAADHUIS', 'WATERSCHAP']

    # if any of they keywords appear in the company name, then these companies need to get
    # NACE W 0003
    governmental = remaining[remaining['Name'].str.contains('|'.join(keywords))]
    governmental["AG"], governmental["activenq"] = ["W", "0003"]
    remaining = remaining[(remaining["Key"].isin(governmental["Key"]) == False)]

    # ______________________________________________________________________________
    # 6. UNMATCHED
    #    dummy NACE code for:
    #    A) unmatched points
    #    B) points outside the AMA boundary also get a dummy code
    #    C) route points
    # ______________________________________________________________________________

    remaining["AG"], remaining["activenq"] = ["W", "0000"]
    out_boundary["AG"], out_boundary["activenq"] = ["W", "0001"]
    route["AG"], route["activenq"] = ["W", "0002"]

    output_unmatched = pd.concat([remaining[output_columns], out_boundary[output_columns], route[output_columns]])
    output_unmatched.drop_duplicates(subset=["Key"], inplace=True)
    output_unmatched["how"] = "unmatched"

    perc = round(len(output_unmatched.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_unmatched.index)} {connect_nace}s unmatched ({perc}%)")

    all_nace = pd.concat([output_by_name_address, output_by_name, output_by_address,
                          output_by_text_proximity, output_by_geo_proximity, governmental, output_unmatched])
    all_nace.drop_duplicates(subset=["Key"], inplace=True)

    original_index = ontdoeners.index
    ontdoeners = pd.merge(ontdoeners, all_nace, how="left", on="Key")
    ontdoeners.index = original_index
    dataframe[f"{connect_nace}_AG"] = ontdoeners["AG"]
    dataframe[f"{connect_nace}_activenq"] = ontdoeners["activenq"]
    dataframe[f"{connect_nace}_NACE"] = ontdoeners["AG"].str.cat(ontdoeners["activenq"].astype(str).str[:4], sep="-")

    # ______________________________________________________________________________
    # ______________________________________________________________________________
    # G I V I N G   N A C E   T O   A L L   O T H E R   R O L E S
    # ______________________________________________________________________________
    # ______________________________________________________________________________

    map_roles = var.roles.copy()
    dummy_nace = var.dummy_nace.copy()

    for role in map_roles:
        if f"{role}" in dummy_nace.keys():
            logging.info(f"Assign NACE to {role}s...")
            dataframe[f"{role}_activenq"] = dummy_nace[f"{role}"]

    return dataframe
