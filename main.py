#!/usr/bin/env python3

import pandas as pd

import filter.run
import clean.run
import enhance.run
import classify.run
import analyze.run
import save.run


import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

# TEST DATAFRAME
dataframe = pd.read_excel('Testing_data/1_full_dataset.xlsx')

# 2016-2020 DATAFRAME
# dataframe = pd.read_excel('Private_data/1_full_dataset.xlsx')

if __name__ == '__main__':

    # filter
    filtered_df = filter.run(dataframe)

    # clean
    cleaned_df = clean.run(filtered_df)

    # enhance
    geolocated_df = enhance.run(cleaned_df)

    # enhance
    connected_df = enhance.run(geolocated_df)

    # classify
    classified_df = classify.run(connected_df)

    # analyze (emission)
    analyzed_df = analyze.run(classified_df)

    # save
    saved_df = save.run(analyzed_df)
