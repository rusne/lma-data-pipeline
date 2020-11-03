import logging
import pandas as pd

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
    logging.info("START PIPELINE...")

    # TEST DATAFRAME
    logging.info("LOAD DATASET...")
    try:
        dataframe = pd.read_excel("Testing_data/1_full_dataset.xlsx")
        # dataframe = pd.read_csv("Private_data/ontvangstmeldingen.csv", low_memory=False)
        # dataframe = dataframe[:100000]
    except Exception as error:
        logging.critical(error)
        raise

    # filter
    logging.info("FILTER DATASET...")
    dataframe = filtering.run(dataframe)

    # clean
    logging.info("CLEAN DATASET...")
    dataframe = clean.run(dataframe)

    # connect nace
    logging.info("CONNECT NACE...")
    dataframe = connect_nace.run(dataframe)

    # classify
    logging.info("CLASSIFY...")
    dataframe = classify.run(dataframe)

    # end pipeline
    logging.info("END PIPELINE...")

    # # load KvK dataset
    # logging.info("PREPARE KVK DATASET...")
    # dataframe = prepare_kvk.run(dataframe)

