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
control_output = None
control_columns = ["Key", "Origname", "Adres", "orig_zaaknaam", "adres", "activenq", "AG"]
output_columns = ["Key", "Origname", "AG", "activenq"]


def match_by_name_and_address(LMA_inbound):
    """
    Match ontdoeners with KvK dataset by name & address
    :param LMA_inbound:
    :return:
    """
    LMA_inbound1 = LMA_inbound[["Key", "Origname", "Adres"]].copy()
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
    LMA_inbound2 = remaining[["Key", "Name", "Origname", "Adres", "Location"]].copy()
    LMA_inbound2.drop_duplicates(subset=["Key"], inplace=True)

    by_name = pd.merge(LMA_inbound2, KvK_actors, left_on="Name", right_on="zaaknaam")
    by_name["wkt"] = by_name["wkt"].apply(wkt.loads)
    by_name["dist"] = by_name.apply(lambda x: x["wkt"].distance(x["Location"]), axis=1, result_type="reduce")
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
    LMA_inbound3 = remaining[["Key", "Origname", "Adres", "Postcode"]].copy()
    LMA_inbound3.drop_duplicates(subset=["Key"], inplace=True)

    by_address = pd.merge(LMA_inbound3, KvK_actors, left_on=["Adres", "Postcode"], right_on=["adres", "postcode"])

    # find those that got matched to only one NACE group
    by_address["count"] = by_address.groupby(["Key"])["AG"].transform("count")
    by_address = by_address[by_address["count"] == 1]

    # matched_by_address = by_address[by_address["count"] == 1]
    #
    # perc = round(len(matched_by_address.index) / float(total_inbound) * 100, 2)
    # logging.info(f"{len(matched_by_address.index)} actors matched only by address ({perc}%)")

    # ambiguous = by_address[by_address["count"] > 1]

    # # give priority by year if possible, otherwise discard the matching
    # temp = pd.DataFrame(columns=ambiguous.columns)
    # for year in var.map_years:
    #     col = "in{0}".format(year)
    #     m = ambiguous[(ambiguous["Jaar"] == year) & (ambiguous[col].astype(str) == "JA")]
    #     temp.append(m)
    #
    # ambiguous["count"] = ambiguous.groupby(["Key"])["AGcode"].transform("count")
    # matched_ambiguous = ambiguous[ambiguous["count"] == 1]
    #
    # print(matched_ambiguous["Key"].nunique(), "additional actors have been matched by address and year")
    #
    # discard = ambiguous[ambiguous["count"] > 1]
    # print(discard["Key"].nunique(), "matches have been discarded due to multiple NACE codes")
    #
    # by_address = pd.concat([matched_by_address, matched_ambiguous])

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

    LMA_inbound4 = remaining[["Key", "Origname", "Adres", "Location"]]
    LMA_inbound4.drop_duplicates(subset=["Key"], inplace=True)
    LMA_inbound4["buffer"] = LMA_inbound4["Location"].buffer(var.buffer_dist)
    buffers = gpd.GeoDataFrame(LMA_inbound4[["Key", "buffer"]], geometry="buffer", crs={"init": "epsg:28992"})

    contains = gpd.sjoin(buffers, KvK_actors_geo, how="inner", op="intersects")

    distances = pd.merge(contains, KvK_actors_geo[["wkt"]], left_on="index_right", right_index=True)
    distances = pd.merge(distances, LMA_inbound4[["Origname", "Adres", "Location"]], left_index=True, right_index=True)

    distances["dist"] = distances["wkt"].distance(distances["Location"])

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

    distances = distances[distances.index.isin(remaining.index)]
    original_index = distances.drop_duplicates(subset=["Key"]).index
    distances.reset_index(inplace=True)

    closest = distances.groupby(["Key"])["dist"].idxmin()
    matched_by_geo_proximity = pd.merge(remaining[["Key"]], distances.loc[closest], on="Key")
    matched_by_geo_proximity.index = original_index

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
        # KvK_actors = pd.read_excel("Private_data/KvK_data/raw_data/all_LISA_part2.xlsx")
        KvK_actors = pd.read_csv("Private_data/KvK_data/raw_data/all_LISA_part2.csv", low_memory=False)
    except Exception as error:
        logging.critical(error)
        raise

    # load casestudy boundary
    # connect with KvK dataset only ontdoeners within the casestudy boundary
    logging.info("Import casestudy boundary...")
    try:
        MRA_boundary = gpd.read_file("Spatial_data/Metropoolregio_RDnew.shp")
    except Exception as error:
        logging.critical(error)
        raise

    # extract ontdoeners from LMA dataset to connect nace
    # all other roles have predefined NACE codes
    logging.info("Extract ontdoeners...")
    ontdoener_columns = [col for col in dataframe.columns if "Ontdoener" in col]
    ontdoeners = dataframe[ontdoener_columns]
    ontdoeners.columns = [col.split("_")[-1] for col in ontdoener_columns]
    ontdoeners = ontdoeners.rename(columns={"Ontdoener": "Name"})
    logging.info(f"Original ontdoeners: {len(ontdoeners.index)}")

    # prepare key to match with KvK dataset (clean name + postcode)
    ontdoeners["Key"] = ontdoeners["Name"].str.cat(ontdoeners[["Postcode"]], sep=" ")

    # first ontdoeners need to be matched with their own locations
    # taking out route actors first
    ontdoeners.loc[ontdoeners["Key"].str.contains("route"), "route"] = "J"
    ontdoeners.loc[ontdoeners["route"] == "J", "Key"] = ontdoeners["Key"].apply(lambda x: str(x).strip(" route"))

    # remove any ontdoeners with no location
    missing_locations = ontdoeners[ontdoeners["Location"].isnull()]
    if len(missing_locations.index):
        logging.warning(f"Remove {len(missing_locations.index)} ontdoeners with missing locations...")
        ontdoeners.dropna(subset=["Location"], inplace=True)

    # after filtering missing locations, add route info again
    ontdoeners.loc[ontdoeners["route"] == "J", "Key"] = ontdoeners["Key"] + " route"
    # logging.info(f"{ontdoeners["key"].nunique()} ontdoeners to connect NACE...")

    # convert WKT to geometry
    ontdoeners["Location"] = ontdoeners["Location"].apply(wkt.loads)
    LMAgdf = gpd.GeoDataFrame(ontdoeners, geometry="Location", crs={"init": "epsg:28992"})

    # check which ontdoeners are within the casestudy area
    joined = gpd.sjoin(LMAgdf, MRA_boundary, how="left", op="within")
    in_boundary = joined[joined["OBJECTID"].isna() == False]
    out_boundary = joined[joined["OBJECTID"].isna()]

    # logging.info(f"{in_boundary["key"].nunique()} ontdoeners are inside the casestudy area")
    if len(out_boundary.index):
        logging.warning(f"Remove {out_boundary['Key'].nunique()} ontdoeners outside the casestudy area")

    # further matching only happens for the actors inside the boundary
    LMA_inbound = in_boundary[ontdoeners.columns]

    # route inzameling gets a separate nace code as well
    route = LMA_inbound[LMA_inbound["route"] == "J"]
    if len(route.index):
        logging.warning(f"Remove {route['Key'].nunique()} ontdoeners belonging to route inzameling")

    LMA_inbound = LMA_inbound[LMA_inbound["route"] != "J"]
    LMA_inbound.drop(columns=["route"])

    total_inbound = LMA_inbound["Key"].nunique()
    logging.info(f"Unique ontdoeners for matching: {total_inbound}")

    # ______________________________________________________________________________
    # 1. BY NAME AND ADDRESS
    #    both name and address are the same
    # ______________________________________________________________________________

    output_by_name_address = match_by_name_and_address(LMA_inbound)
    perc = round(len(output_by_name_address.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_name_address.index)} ontdoeners matched by name & postcode ({perc}%)")

    # take out those ontdoeners that had not been matched
    remaining = LMA_inbound[(LMA_inbound["Key"].isin(output_by_name_address["Key"]) == False)]

    # ______________________________________________________________________________
    # 2. BY NAME ONLY
    #    geographically closer one gets a priority
    # ______________________________________________________________________________

    output_by_name = match_by_name(remaining)
    perc = round(len(output_by_name.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_name.index)} ontdoeners matched only by name ({perc}%)")

    # take out those Ontdoeners that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_name["Key"]) == False)]

    # ______________________________________________________________________________
    # 3. BY ADDRESS ONLY
    # ______________________________________________________________________________

    output_by_address = match_by_address(remaining)
    perc = round(len(output_by_address.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_address.index)} ontdoeners matched only by address ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_address["Key"]) == False)]

    # ______________________________________________________________________________
    # 4. BY  GEO AND TEXT PROXIMITY
    #    closest name within a certain radius
    # ______________________________________________________________________________

    output_by_text_proximity, distances = match_by_text_proximity(remaining)
    perc = round(len(output_by_text_proximity.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_text_proximity.index)} ontdoeners matched with the closest name match "
                    f"in <{var.buffer_dist}m ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_text_proximity["Key"]) == False)]

    # ______________________________________________________________________________
    # 5. BY GEO PROXIMITY
    # ______________________________________________________________________________

    output_by_geo_proximity = match_by_geo_proximity(remaining, distances)
    perc = round(len(output_by_geo_proximity.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_by_geo_proximity.index)} ontdoeners matched by proximity ({perc}%)")

    # take out those actors that had not been matched
    remaining = remaining[(remaining["Key"].isin(output_by_geo_proximity["Key"]) == False)]

    # ______________________________________________________________________________
    # 5. UNMATCHED
    #    dummy NACE code for:
    #    A) unmatched points
    #    B) points outside the LISA boundary also get a dummy code
    #    C) route points
    # ______________________________________________________________________________

    remaining[["AG", "activenq"]] = ["W", "0000"]
    out_boundary[["AG", "activenq"]] = ["W", "0001"]
    route[["AG", "activenq"]] = ["W", "0002"]

    output_unmatched = pd.concat([remaining[output_columns], out_boundary[output_columns], route[output_columns]])
    output_unmatched.drop_duplicates(subset=["Key"], inplace=True)
    output_unmatched["how"] = "unmatched"

    perc = round(len(output_unmatched.index) / float(total_inbound) * 100, 2)
    logging.warning(f"{len(output_unmatched.index)} ontdoeners unmatched ({perc}%)")

    all_nace = pd.concat([output_by_name_address, output_by_name, output_by_address,
                         output_by_text_proximity, output_by_geo_proximity, output_unmatched])
    all_nace.drop_duplicates(subset=["Key"], inplace=True)

    original_index = ontdoeners.index
    ontdoeners = pd.merge(ontdoeners, all_nace, how="left", on="Key")
    ontdoeners.index = original_index
    dataframe["Ontdoener_NACE"] = ontdoeners["AG"].str.cat(ontdoeners["activenq"].astype(str).str[:4], sep="-")

    # ______________________________________________________________________________
    # ______________________________________________________________________________
    # G I V I N G   N A C E   T O   A L L   O T H E R   R O L E S
    # ______________________________________________________________________________
    # ______________________________________________________________________________

    map_roles = var.roles.copy()
    map_roles.remove("Ontdoener")
    dummy_nace = var.dummy_nace.copy()

    for role in map_roles:
        if f"{role}" in dummy_nace.keys():
            dataframe[f"{role}_activenq"] = dummy_nace[f"{role}"]

    return dataframe
