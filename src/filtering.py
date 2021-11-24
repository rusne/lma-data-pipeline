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
This module filters useful columns and data points in the original data file
logs how many erroneous data points have been filtered on which basis
and returns a dataframe ready for the further analysis
"""
import logging
import numpy as np
import variables as var
import pandas as pd


def run(dataframe):
    """
    Extract & filter columns from the original dataset
    :param dataframe: the original dataset
    :return: the filtered dataset
    """

    # selection of the columns and roles we want to include in our analysis
    LMA_columns = var.LMA_columns.copy()
    roles = var.roles.copy()

    # filter the requested columns from the original dataframe
    try:
        LMA = dataframe[LMA_columns]
        logging.info(f"Original dataset length: {len(LMA.index)} lines")
    except Exception as error:
        logging.critical(error)
        raise

    # record size of original dataframe & removals
    original_length = len(LMA.index)
    removals = 0

    # # keep flows where at least one role is within AMA
    # postcodes = pd.read_csv('Spatial_data/AMA_postcode.csv', low_memory=False)
    # postcodes['AMA_postcode'] = postcodes['AMA_postcode'].astype(str)
    # conditions = []
    # for role in roles:
    #     condition = LMA[f'{role}_Postcode'].astype(str).str[:4].isin(postcodes['AMA_postcode'])
    #     conditions.append(condition)
    # e = len(LMA[~np.logical_or.reduce(conditions)].index)
    # if e:
    #     removals += e
    #     LMA = LMA[np.logical_or.reduce(conditions)]
    #     logging.warning(f"{e} lines outside AMA removed")

    # if "Herkomst" has all columns empty, copy from "Ontdoener"
    if any('Herkomst' in col for col in LMA.columns):
        Herkomst_columns = [col for col in LMA.columns if "Herkomst" in col]
        all_null = [LMA[col].isnull() for col in Herkomst_columns]
        idx = LMA[np.bitwise_and.reduce(all_null)].index
        for col in Herkomst_columns:
            orig_col = "Ontdoener_" + col.split("_")[-1]
            LMA.loc[idx, col] = LMA[orig_col]

    if "Verwerker_Land" not in LMA.columns:
        LMA["Verwerker_Land"] = "Nederland"

    # filter empty fields
    # log all empty fields before removing them
    # empty role columns
    for role in roles:
        # empty company name
        # note: Herkomst does not have company name!
        if role != "Herkomst":
            e = len(LMA[LMA[role].isnull()].index)
            if e:
                removals += e
                LMA = LMA[LMA[role].notnull()]
                logging.warning(f"{e} lines without {role} removed")

        # # empty postcode
        # postcode = role + "_Postcode"
        # e = len(LMA[LMA[postcode].isnull()].index)
        # if e:
        #     removals += e
        #     LMA = LMA[LMA[postcode].notnull()]
        #     logging.warning(f"{e} lines without {postcode} removed")

    # empty year
    e = len(LMA[LMA["MeldPeriodeJAAR"].isnull()].index)
    if e:
        removals += e
        LMA = LMA[LMA.MeldPeriodeJAAR.notnull()]
        logging.warning(f"{e} lines without year removed")

    # empty month
    e = len(LMA[LMA["MeldPeriodeMAAND"].isnull()].index)
    if e:
        removals += e
        LMA = LMA[LMA.MeldPeriodeMAAND.notnull()]
        logging.warning(f"{e} lines without month removed")

    # zero amount
    e = len(LMA[LMA["Gewicht_KG"] < 1].index)
    if e:
        removals += e
        LMA = LMA[LMA["Gewicht_KG"] >= 1]
        logging.warning(f"{e} lines without weight removed")

    # zero trips
    e = len(LMA[LMA["Aantal_vrachten"] < 1].index)
    if e:
        removals += e
        LMA = LMA[LMA["Aantal_vrachten"] >= 1]
        logging.warning(f"{e} lines without trips removed")

    # >41t per trip
    # e = len(LMA[LMA["Gewicht_KG"] / LMA["Aantal_vrachten"] > 41000].index)
    e = len(LMA[LMA["Gewicht_KG"] / LMA["Aantal_vrachten"] > 30000].index)
    if e:
        removals += e
        # LMA = LMA[LMA["Gewicht_KG"] / LMA["Aantal_vrachten"] <= 41000]
        LMA = LMA[LMA["Gewicht_KG"] / LMA["Aantal_vrachten"] <= 30000]
        logging.warning(f"{e} lines with weight >41t removed")

    # log the final dataframe size after cleaning
    perc = round(len(LMA.index) / original_length * 100, 1)
    logging.info(f"Final dataset length: {len(LMA.index)} lines ({perc}%)")

    return LMA, removals
