import logging
import pandas as pd
import pytest

import filtering
import clean
import connect_nace
import classify
# import prepare_kvk

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
        dataframe = pd.read_excel("Testing_data/1_full_dataset.xlsx")
        # dataframe = pd.read_csv("Private_data/ontvangstmeldingen.csv", low_memory=False)
        # dataframe = dataframe[:100000]
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
    filtered_dataframe = filtering.run(dataframe)
    logging.info("FILTER COMPLETE!\n")

    # clean
    logging.info("CLEAN DATASET...")
    cleaned_dataframe = clean.run(filtered_dataframe)
    try:
        assert len(cleaned_dataframe.index) == len(filtered_dataframe.index)
    except AssertionError:
        logging.critical('Dataset size changed!')
        raise
    logging.info("CLEAN COMPLETE!\n")

    # connect nace
    logging.info("CONNECT NACE TO DATASET...")
    connected_dataframe = connect_nace.run(cleaned_dataframe)
    logging.info("CONNECT NACE COMPLETE!\n")

    # classify
    logging.info("CLASSIFY DATASET...")
    classified_dataframe = classify.run(connected_dataframe)
    logging.info("CLASSIFY COMPLETE!\n")

    # end pipeline
    logging.info("PIPELINE COMPLETE!")

    # # load KvK dataset
    # logging.info("PREPARE KVK DATASET...")
    # dataframe = prepare_kvk.run(dataframe)

