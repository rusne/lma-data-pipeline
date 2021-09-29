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

import logging
import pandas as pd
from shapely import wkt
import geopandas as gpd

from src import classify, clean, connect_nace, filtering, prepare_kvk

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


if __name__ == "__main__":
    # logging: timestamp, warning level & message
    logging.basicConfig(filename="full_logs.log",  # file name
                        filemode="w",  # overwrite
                        level=logging.DEBUG,  # lowest warning level
                        format="%(asctime)s [%(levelname)s]: %(message)s")  # message format

    # start pipeline
    logging.info("START PIPELINE...\n")

    # load dataset
    logging.info("LOAD DATASET...")
    try:
        dataframe = pd.read_csv("Private_data/LMA_data_AMA_2018/afgiftemeldingen_AMA_2018.csv", low_memory=False)
        assert len(dataframe.index) > 0
    except Exception as error:
        if type(error) == FileNotFoundError:
            logging.critical('Dataset file not found!')
        elif type(error) == AssertionError:
            logging.critical('Dataset file is empty!')
        logging.critical(error)
        raise
    logging.info("LOAD COMPLETE!\n")

    # filter
    logging.info("FILTER DATASET...")
    filtered_dataframe, removals = filtering.run(dataframe)
    try:
        assert len(filtered_dataframe.index) + removals == len(dataframe.index)
    except AssertionError:
        logging.critical("Mismatch on number of lines!")
        raise
    logging.info("FILTER COMPLETE!\n")

    # clean
    logging.info("CLEAN DATASET...")
    cleaned_dataframe, removals = clean.run(filtered_dataframe)
    try:
        assert len(cleaned_dataframe.index) + removals == len(filtered_dataframe.index)
    except AssertionError:
        logging.critical("Mismatch on number of lines!")
        raise
    logging.info("CLEAN COMPLETE!\n")

    # connect nace
    logging.info("CONNECT NACE TO DATASET...")
    connected_dataframe = connect_nace.run(cleaned_dataframe)
    try:
        assert len(connected_dataframe.index) == len(cleaned_dataframe.index)
    except AssertionError:
        logging.critical("Mismatch on number of lines!")
        raise
    logging.info("CONNECT NACE COMPLETE!\n")

    # classify
    logging.info("CLASSIFY DATASET...")
    # classified_dataframe = classify.run(connected_dataframe)
    classified_dataframe = connected_dataframe
    try:
        assert len(classified_dataframe.index) == len(connected_dataframe.index)
    except AssertionError:
        logging.critical("Mismatch on number of lines!")
        raise
    logging.info("CLASSIFY COMPLETE!\n")

    # convert to WGS84 to export
    logging.info("CHANGE CRS...")
    locations = [col for col in classified_dataframe if 'Location' in col]
    for location in locations:
        logging.info(f"Change CRS for {location}s...")
        classified_dataframe = classified_dataframe[classified_dataframe[location].notnull()]
        role_locations = classified_dataframe[location].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(role_locations, geometry=location, crs={"init": "epsg:28992"})
        gdf = gdf.to_crs(epsg=4326)
        classified_dataframe[location] = gdf[gdf.geometry.notnull()].geometry.apply(lambda x: wkt.dumps(x))

    # end pipeline
    logging.info("EXPORT RESULT...")
    classified_dataframe.to_csv("Private_data/result.csv", index=False)
    logging.info("PIPELINE COMPLETE!")

    # # load KvK dataset
    # logging.info("PREPARE KVK DATASET...")
    # dataframe = prepare_kvk.run(dataframe)
