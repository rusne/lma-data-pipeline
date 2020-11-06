# Reads the original KvK file, filters the relevant columns,
# unifies NACE codes to 4 or 5 digits,
# filters out companies without a NACE code,
# prepares unlocated entries for the geolocation

# TODO filter out all actors that do not belong to the SBI codes that are supposed to produce waste

import pandas as pd
import logging
from clean import (clean_company_name,
                   clean_nace,
                   clean_address,
                   clean_postcode,
                   clean_huisnr)
import geolocate

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


def run(dataframe):
    priv_folder = "Private_data/"
    pub_folder = "Public_data/"

    # load NACE codes on economic activities
    logging.info("Load NACE codes...")
    try:
        NACEtable = pd.read_excel(pub_folder + "NACE_table.xlsx", sheet_name="NACE_nl")
    except Exception as error:
        logging.critical(error)
        raise

    # load KvK dataset
    logging.info("Load KvK dataset...")
    try:
        # orig_KvK_dataset = priv_folder + "KvK_data/raw_data/KvK AMA 31-10-2018 _ all.xlsx"
        # KvK = pd.read_excel(orig_KvK_dataset, dtype={"SBI": object})
        orig_KvK_dataset = priv_folder + "KvK_data/raw_data/KvK AMA 31-10-2018 _ all.csv"
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

    # match with the list of NACE activities, skip if not present
    NACEtable["Digits"] = NACEtable["Digits"].astype(str)
    NACEtable["Digits"] = NACEtable["Digits"].str.zfill(4)
    KvK = pd.merge(KvK, NACEtable[["Digits"]], left_on="SBI", right_on="Digits", validate="m:1")

    e = pre - len(KvK.index)
    if e:
        logging.warning(f"{e} lines have been filtered due to an invalid NACE")
        pre = len(KvK.index)

    # all company name versions in KvK dataset
    name_versions = ["HN_1X45", "HN_1X2X30", "HN_1X30"]
    # all company address versions in KvK dataset
    loc_versions = ["_1", "_CA"]

    # process all company name & address versions in KvK dataset
    all_versions = []
    for name_col in name_versions:
        for loc_col in loc_versions:
            # selection of the columns we want to include in our analysis
            KvK_columns = [name_col, "STRAATNAAM" + loc_col, "HUISNR" + loc_col,
                           "POSTCODE" + loc_col, "WOONPLAATS" + loc_col, "SBI"]

            # filter & rename requested columns
            KvK_ver = KvK[KvK_columns]
            KvK_ver.columns = ["zaaknaam", "straat", "huisnr", "postcode", "plaats", "activenq"]

            # cast columns as strings
            KvK_ver["zaaknaam"] = KvK_ver["zaaknaam"].astype(str)
            KvK_ver["straat"] = KvK_ver["straat"].astype(str)
            KvK_ver["huisnr"] = KvK_ver["huisnr"].astype(str)
            KvK_ver["plaats"] = KvK_ver["plaats"].astype(str)
            KvK_ver["postcode"] = KvK_ver["postcode"].astype(str)

            # filter invalid company names
            KvK_ver["orig_zaaknaam"] = KvK_ver["zaaknaam"].copy()  # copy of the orig name
            KvK_ver["zaaknaam"] = KvK_ver["zaaknaam"].apply(clean_company_name)
            KvK_ver = KvK_ver[KvK_ver["zaaknaam"].str.len() > 1]

            # clean addresses
            KvK_ver["straat"] = KvK_ver["straat"].apply(clean_address)
            KvK_ver["huisnr"] = KvK_ver["huisnr"].apply(clean_huisnr)
            KvK_ver["postcode"] = KvK_ver["postcode"].apply(clean_postcode)
            KvK_ver["plaats"] = KvK_ver["plaats"].apply(clean_address)

            # filter invalid postcodes
            KvK_ver = KvK_ver[KvK_ver["postcode"].str.len() == 6]

            # join address into a single column for easier geolocation
            KvK_ver["adres"] = KvK_ver["straat"].str.cat(KvK_ver[["huisnr", "postcode", "plaats"]], sep=" ")

            # create a key for each separate actor (name + postcode)
            KvK_ver["key"] = KvK_ver["zaaknaam"].str.cat(KvK_ver[["postcode"]], sep=" ")

            # append dataframe to concatenate later
            all_versions.append(KvK_ver.copy())

    # concatenate & remove duplicates
    all_KvK = pd.concat(all_versions)
    all_KvK.drop_duplicates(subset=["key", "activenq"], inplace=True)

    # geolocate
    logging.info("Geolocate KvK dataset...")
    addresses = all_KvK[["adres", "postcode"]]
    all_KvK["location"] = geolocate.run(addresses)

    logging.info(f"Extract {len(all_KvK.index)} from KvK...")
    return all_KvK
