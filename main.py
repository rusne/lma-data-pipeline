#!/usr/bin/env python3

import logging
import sys
import pandas as pd

import clean_new
# import filtering
# import clean
# import enhance.run
# import classify.run
# import analyze.run
# import save.run

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

roles = ['Ontdoener', 'Herkomst', 'Verwerker']

if __name__ == '__main__':
    # logging: timestamp, warning level & message
    logging.basicConfig(filename='full.log',  # file name
                        filemode='w',  # overwrite
                        level=logging.DEBUG,  # lowest warning level
                        format='%(asctime)s [%(levelname)s]: %(message)s')  # message format

    # start pipeline
    logging.info('START PIPELINE...')

    # TEST DATAFRAME
    logging.info('LOAD DATASET...')
    try:
        # dataframe = pd.read_excel('Testing_data/1_full_dataset.xlsx')
        dataframe = pd.read_csv('Private_data/ontvangstmeldingen.csv', low_memory=False)
    except Exception as error:
        logging.critical(error)
        raise

    # CLEAN DATASET
    logging.info('CLEAN DATASET...')
    cleaned_df = clean_new.run(dataframe, roles)

    # end pipeline
    logging.info('END PIPELINE...')

    # # filter
    # logging.info('FILTER DATASET...')
    # filtered_df = filtering.run(dataframe)
    #
    # # clean
    # logging.info('CLEAN DATASET...')
    # cleaned_df = clean.run(filtered_df, roles)

    # # enhance
    # geolocated_df = enhance.run(cleaned_df)
    #
    # # enhance
    # connected_df = enhance.run(geolocated_df)
    #
    # # classify
    # classified_df = classify.run(connected_df)
    #
    # # analyze (emission)
    # analyzed_df = analyze.run(classified_df)
    #
    # # save
    # saved_df = save.run(analyzed_df)
