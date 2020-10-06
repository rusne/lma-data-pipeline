#!/usr/bin/env python3

import logging
import sys
import pandas as pd

import filtering
import clean.run
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
        dataframe = pd.read_excel('Testing_data/1_full_dataset.xlsx')
    except Exception as error:
        logging.critical(error)
        sys.exit(1)

    # filter
    logging.info('FILTER DATASET...')
    filtered_df = filtering.run(dataframe)

    # end pipeline
    logging.info('END PIPELINE...')

    # # clean
    cleaned_df = clean.run(filtered_df, roles)
    #
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
