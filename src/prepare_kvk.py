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

# Reads the original KvK file, filters the relevant columns,
# unifies NACE codes to 4 or 5 digits,
# filters out companies without a NACE code,
# prepares unlocated entries for the geolocation

import pandas as pd
import geopandas as gpd
import logging
from src.clean import (clean_company_name,
                       clean_nace,
                       clean_address,
                       clean_postcode,
                       clean_huisnr)
from src import geolocate
import numpy as np

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


def run():
    priv_folder = "Private_data/"
    pub_folder = "Public_data/"

    # load NACE codes on economic activities
    logging.info("Load NACE codes...")
    try:
        NACEtable = pd.read_excel(priv_folder + "NACE_table.xlsx", sheet_name="NACE_nl")
    except Exception as error:
        logging.critical(error)
        raise

    # load KvK dataset
    logging.info("Load KvK dataset...")
    try:
        orig_KvK_dataset = priv_folder + "parlijst_utrecht.csv"
        KvK = pd.read_csv(orig_KvK_dataset, dtype={"SBI": object}, low_memory=False)
    except Exception as error:
        logging.critical(error)
        raise

    # original KvK entries
    pre = len(KvK.index)
    logging.info(f"Original KvK dataset: {pre} lines")

    # filter incorrect SBI codes
    KvK["SBI"] = KvK["SBI"].astype(str)
    KvK["SBI"] = KvK["SBI"].apply(clean_nace)
    KvK = KvK[KvK["SBI"].str.len() < 6]
    KvK = KvK[KvK["SBI"].str.len() > 3]
    KvK = KvK[KvK["SBI"].astype(int) != 100]
    KvK["SBI"] = KvK["SBI"].apply(lambda x: str(x)[:4])

    e = pre - len(KvK.index)
    if e:
        logging.warning(f"{e} lines have been filtered due to an invalid SBI")
        pre = len(KvK.index)

    columns = [
        'NAAM',
        'STRAATLANG',
        'HUISNR',
        'POSTCODE',
        'PLAATSLANG',
        'SBI'
    ]
    KvK = KvK[columns]
    KvK.columns = ["zaaknaam", "straat", "huisnr", "postcode", "plaats", "activenq"]

    # cast columns as strings
    KvK["zaaknaam"] = KvK["zaaknaam"].astype(str)
    KvK["straat"] = KvK["straat"].astype(str)
    KvK["huisnr"] = KvK["huisnr"].astype(str)
    KvK["plaats"] = KvK["plaats"].astype(str)
    KvK["postcode"] = KvK["postcode"].astype(str)

    # filter invalid company names
    KvK["orig_zaaknaam"] = KvK["zaaknaam"].copy()  # copy of the orig name
    KvK["zaaknaam"] = KvK["zaaknaam"].apply(clean_company_name)
    KvK = KvK[KvK["zaaknaam"].str.len() > 1]

    # clean addresses
    KvK["straat"] = KvK["straat"].apply(clean_address)
    KvK["huisnr"] = KvK["huisnr"].apply(clean_huisnr)
    KvK["postcode"] = KvK["postcode"].apply(clean_postcode)
    KvK["plaats"] = KvK["plaats"].apply(clean_address)
    KvK["land"] = 'NEDERLAND'

    # filter invalid postcodes
    KvK = KvK[KvK["postcode"].str.len() == 6]

    # join address into a single column for easier geolocation
    KvK["adres"] = KvK["straat"].str.cat(KvK[["huisnr", "postcode"]], sep=" ")

    # create a key for each separate actor (name + postcode)
    KvK["key"] = KvK["zaaknaam"].str.cat(KvK[["postcode"]], sep=" ")

    KvK["Adres"] = KvK["straat"].str.cat(KvK[["huisnr", "postcode", "plaats", "land"]], sep=" ")

    # load mapbox addresses
    # load geolocations (TO BE REMOVED IN THE FINAL VERSION)
    geo = pd.read_csv("Private_data/mapbox_locations.csv", low_memory=False, sep='\t')

    geo["straat"] = geo["straat"].astype("str")
    geo["straat"] = geo["straat"].apply(clean_address)

    geo["huisnr"] = geo["huisnr"].astype("str")
    geo["huisnr"] = geo["huisnr"].apply(clean_huisnr)

    geo["postcode"] = geo["postcode"].astype("str")
    geo["postcode"] = geo["postcode"].apply(clean_postcode)

    geo["plaats"] = geo["plaats"].astype("str")
    geo["plaats"] = geo["plaats"].apply(clean_address)

    geo["land"] = geo["land"].astype("str")
    geo["land"] = geo["land"].apply(clean_address)

    geo["adres"] = geo["straat"].str.cat(geo[["huisnr", "postcode", "plaats", "land"]], sep=" ")
    geo.drop_duplicates(subset=['adres'], inplace=True)

    geo.loc[geo["lon"] == "None", "lon"] = np.nan
    geo.loc[geo["lat"] == "None", "lat"] = np.nan

    addresses = pd.merge(KvK["Adres"], geo, how='left', left_on="Adres", right_on="adres")
    addresses.index = KvK.index  # keep original index
    locations = gpd.GeoDataFrame(addresses, geometry=gpd.points_from_xy(addresses.lon, addresses.lat), crs={"init":"epsg:4326"})
    locations = locations.to_crs("epsg:28992")
    KvK["wkt"] = geolocate.add_wkt(locations)

    # match with the list of NACE activities, skip if not present
    NACEtable["Digits"] = NACEtable["Digits"].astype(str)
    NACEtable["Digits"] = NACEtable["Digits"].str.zfill(4)
    KvK = pd.merge(KvK, NACEtable[["Digits", "AGcode"]], left_on="activenq", right_on="Digits", validate="m:1")

    e = pre - len(KvK.index)
    if e:
        logging.warning(f"{e} lines have been filtered due to an invalid NACE")
        pre = len(KvK.index)

    KvK.rename(columns={"AGcode": "AG"}, inplace=True)
    KvK = KvK[[
        "zaaknaam",
        "orig_zaaknaam",
        "straat",
        "huisnr",
        "postcode",
        "plaats",
        "adres",
        "activenq",
        "AG",
        "key",
        "wkt"
    ]]
    KvK.to_csv(priv_folder + 'utrecht_kvk.csv', index=False)

    # # all company name versions in KvK dataset
    # name_versions = ["HN_1X45", "HN_1X2X30", "HN_1X30"]
    # # all company address versions in KvK dataset
    # loc_versions = ["_1", "_CA"]
    #
    # # process all company name & address versions in KvK dataset
    # all_versions = []
    # for name_col in name_versions:
    #     for loc_col in loc_versions:
    #         # selection of the columns we want to include in our analysis
    #         KvK_columns = [name_col, "STRAATNAAM" + loc_col, "HUISNR" + loc_col,
    #                        "POSTCODE" + loc_col, "WOONPLAATS" + loc_col, "SBI"]
    #
    #         # filter & rename requested columns
    #         KvK_ver = KvK[KvK_columns]
    #         KvK_ver.columns = ["zaaknaam", "straat", "huisnr", "postcode", "plaats", "activenq"]
    #
    #         # cast columns as strings
    #         KvK_ver["zaaknaam"] = KvK_ver["zaaknaam"].astype(str)
    #         KvK_ver["straat"] = KvK_ver["straat"].astype(str)
    #         KvK_ver["huisnr"] = KvK_ver["huisnr"].astype(str)
    #         KvK_ver["plaats"] = KvK_ver["plaats"].astype(str)
    #         KvK_ver["postcode"] = KvK_ver["postcode"].astype(str)
    #
    #         # filter invalid company names
    #         KvK_ver["orig_zaaknaam"] = KvK_ver["zaaknaam"].copy()  # copy of the orig name
    #         KvK_ver["zaaknaam"] = KvK_ver["zaaknaam"].apply(clean_company_name)
    #         KvK_ver = KvK_ver[KvK_ver["zaaknaam"].str.len() > 1]
    #
    #         # clean addresses
    #         KvK_ver["straat"] = KvK_ver["straat"].apply(clean_address)
    #         KvK_ver["huisnr"] = KvK_ver["huisnr"].apply(clean_huisnr)
    #         KvK_ver["postcode"] = KvK_ver["postcode"].apply(clean_postcode)
    #         KvK_ver["plaats"] = KvK_ver["plaats"].apply(clean_address)
    #
    #         # filter invalid postcodes
    #         KvK_ver = KvK_ver[KvK_ver["postcode"].str.len() == 6]
    #
    #         # join address into a single column for easier geolocation
    #         KvK_ver["adres"] = KvK_ver["straat"].str.cat(KvK_ver[["huisnr", "postcode"]], sep=" ")
    #
    #         # create a key for each separate actor (name + postcode)
    #         KvK_ver["key"] = KvK_ver["zaaknaam"].str.cat(KvK_ver[["postcode"]], sep=" ")
    #
    #         # append dataframe to concatenate later
    #         all_versions.append(KvK_ver.copy())
    #
    # # concatenate & remove duplicates
    # all_KvK = pd.concat(all_versions)
    # all_KvK.drop_duplicates(subset=["key", "activenq"], inplace=True)
    #
    # # geolocate
    # logging.info("Geolocate KvK dataset...")
    # addresses = all_KvK[["adres", "postcode"]]
    # all_KvK["location"] = geolocate.run(addresses)
    #
    # logging.info(f"Extract {len(all_KvK.index)} from KvK...")
    # return all_KvK
