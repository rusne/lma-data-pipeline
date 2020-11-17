"""
This module cleans the dataframe columns and
prepares them for further analysis
"""
import logging
import numpy as np
import geolocate
import variables as var
import pandas as pd
import geopandas as gpd


def clean_description(desc):
    """
    Apply function to clean rows from the description column
    :param desc: row from description column
    :return: formatted row
    """
    desc = desc.strip()
    desc = desc.lower()
    desc = desc.replace(u"\xa0", u" ")
    desc = " ".join(desc.split())
    if desc == "nan":
        return np.NaN
    return desc


def clean_postcode(postcode):
    """
    Apply function to clean rows from the postcode column of each role
    :param desc: row from role postcode column
    :return: formatted row
    """
    postcode = postcode.strip()
    postcode = postcode.replace(" ","")
    postcode = postcode.upper()
    if "0000" in postcode:
        return ""
    return postcode


def clean_company_name(name):
    """
    Apply function to clean rows from the company name column of each role
    :param desc: row from company name column for each column
    :return: formatted row
    """

    # remove all non-ASCII characters
    orig_name = name
    printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c"
    name = "".join(filter(lambda x: x in printable, name))

    name = name.upper()

    litter = [" SV", "S V", "S.V.", " BV", "B V", "B.V.", " CV", "C.V.",
              " NV", "N.V.", "V.O.F", " VOF", "V O F", "\"T", "\"S"]
    # remove all the littering characters
    for l in litter:
        name = name.replace(l, "")

    name = " ".join(name.split())

    # check if company name does not contain only digits
    name_copy = name
    for dig in "0123456789":
        name_copy = name_copy.replace(dig, "")
    if len(name_copy) == 0:
        name = ""

    return name


def clean_address(address):
    """
    Apply function to clean rows from the street/city column of each role
    :param desc: row from street/city column
    :return: formatted row
    """
    address = address.strip()
    address = address.upper()
    address = " ".join(address.split())
    return address


def clean_huisnr(nr):
    """
    Apply function to clean rows from the house number column of each role
    :param desc: row from house number column
    :return: formatted row
    """
    nr = nr.split(".")[0]
    nr = "".join(filter(lambda x: x in "0123456789", nr))
    return nr


def clean_nace(nace):
    """
    Apply function to clean NACE codes
    :param desc: row with NACE code
    :return: formatted row
    """
    nace = "".join(filter(lambda x: x in "0123456789", nace))
    return nace


def run(dataframe):
    """
    Clean role columns & geolocate them
    to prepare for further analysis
    :param dataframe: dataframe filtered from erroneous entries
    :return: dataframe with formatted role info & geolocation
    """
    # clean the BenamingAfval column (waste descriptions)
    if "BenamingAfval" in dataframe.columns:
        logging.info("Clean descriptions (BenamingAfval)...")
        dataframe["BenamingAfval"] = dataframe["BenamingAfval"].astype("unicode")
        dataframe["BenamingAfval"] = dataframe["BenamingAfval"].apply(clean_description)

    # load geolocations (TO BE REMOVED IN THE FINAL VERSION)
    # geo = pd.read_csv("Private_data/geolocations.csv", low_memory=False)
    geo = pd.read_csv("Private_data/mapbox.csv", low_memory=False, sep='\t')

    geo["straat"] = geo["straat"].astype("str")
    geo["straat"] = geo["straat"].apply(clean_address)

    geo["huisnr"] = geo["huisnr"].astype("str")
    geo["huisnr"] = geo["huisnr"].apply(clean_huisnr)

    geo["postcode"] = geo["postcode"].astype("str")
    geo["postcode"] = geo["postcode"].apply(clean_postcode)

    geo["adres"] = geo["straat"].str.cat(geo[["huisnr", "postcode"]], sep=" ")
    geo.drop_duplicates(subset=['adres'], inplace=True)

    # clean role columns
    roles = var.roles
    for role in roles:
        logging.info(f"Clean & geolocate {role}s...")

        # list columns for cleaning
        orig_name = f"{role}_Origname"
        straat = f"{role}_Straat"
        huisnr = f"{role}_Huisnr"
        postcode = f"{role}_Postcode"
        plaats = f"{role}_Plaats"

        # clean company name
        # note: Herkomst does not have name!
        if role != "Herkomst":
            # preserve the original name
            dataframe[orig_name] = dataframe[role].copy()
            dataframe[role] = dataframe[role].astype("str")
            dataframe[role] = dataframe[role].apply(clean_company_name)

        # clean street name
        dataframe[straat] = dataframe[straat].astype("str")
        dataframe[straat] = dataframe[straat].apply(clean_address)

        # clean house number
        dataframe[huisnr] = dataframe[huisnr].astype("str")
        dataframe[huisnr] = dataframe[huisnr].apply(clean_huisnr)

        # clean postcode
        dataframe[postcode] = dataframe[postcode].astype("str")
        dataframe[postcode] = dataframe[postcode].apply(clean_postcode)

        # clean city name
        dataframe[plaats] = dataframe[plaats].astype("str")
        dataframe[plaats] = dataframe[plaats].apply(clean_address)

        # prepare address for geolocation
        dataframe[f"{role}_Adres"] = dataframe[straat].str.cat(dataframe[[huisnr, postcode]], sep=" ")

        # geolocate (TO BE REMOVED IN THE FINAL VERSION)
        addresses = pd.merge(dataframe[f"{role}_Adres"], geo, how='left', left_on=f"{role}_Adres", right_on="adres")

        addresses.index = dataframe.index  # keep original index
        # addresses.loc[addresses["x"].isnull(), "x"] = 0
        # addresses.loc[addresses["y"].isnull(), "y"] = 0
        locations = gpd.GeoDataFrame(addresses, geometry=gpd.points_from_xy(addresses.x, addresses.y), crs={"init":"epsg:4326"})
        locations = locations.to_crs("epsg:28992")
        dataframe[f"{role}_Location"] = geolocate.add_wkt(locations)

        # # geolocate (FINAL VERSION)
        # logging.info(f"Geolocate for {role}...")
        # addresses = dataframe[[f"{role}_Adres", postcode]]
        # addresses.columns = ["adres", "postcode"]
        # dataframe[f"{role}_Location"] = geolocate.run(addresses)

    return dataframe
