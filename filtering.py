#!/usr/bin/env python3

# filter lines:
# no year/month included
# empty company name
# invalid/empty postcodes --> unless it's not in the Netherlands
# invalid/empty addresses --> streetname anything but text

# filter too small or too big amounts Gewicht_KG/Aantal_vrachten

# output log : x lines have been removed because of reason y

"""
This module filters useful columns and data points in the original data file
logs how many erroneous data points have been filtered on which basis
and returns a dataframe ready for the further analysis
"""

import logging
import sys
import pandas as pd


def run(dataframe):
    """

    :param dataframe:
    """

    # selection of the columns we want to include in our analysis
    LMA_columns = ['Afvalstroomnummer', 'VerwerkingsmethodeCode',
                   'VerwerkingsOmschrijving', 'RouteInzameling',
                   'Inzamelaarsregeling', 'ToegestaanbijInzamelaarsregeling',
                   'EuralCode', 'BenamingAfval', 'MeldPeriodeJAAR',
                   'MeldPeriodeMAAND', 'Gewicht_KG', 'Aantal_vrachten',
                   # Ontdoener
                   'Ontdoener', 'Ontdoener_Postcode', 'Ontdoener_Plaats',
                   'Ontdoener_Straat', 'Ontdoener_Huisnr', 'Ontdoener_Land',
                   # Herkomst
                   'Herkomst_Postcode', 'Herkomst_Straat',
                   'Herkomst_Plaats', 'Herkomst_Huisnr', 'Herkomst_Land',
                   # # Afzender
                   # 'Afzender', 'Afzender_Postcode', 'Afzender_Straat',
                   # 'Afzender_Plaats', 'Afzender_Huisnummer',
                   # # Inzamelaar
                   # 'Inzamelaar', 'Inzamelaar_Postcode', 'Inzamelaar_Straat',
                   # 'Inzamelaar_Plaats', 'Inzamelaar_Huisnr',
                   # # Bemiddelaar
                   # 'Bemiddelaar', 'Bemiddelaar_Postcode', 'Bemiddelaar_Straat',
                   # 'Bemiddelaar_Plaats', 'Bemiddelaar_Huisnr',
                   # # Handelaar
                   # 'Handelaar', 'Handelaar_Postcode', 'Handelaar_Straat',
                   # 'Handelaar_Plaats', 'Handelaar_Huisnummer',
                   # # Ontvanger
                   # 'Ontvanger', 'Ontvanger_Postcode', 'Ontvanger_Straat',
                   # 'Ontvanger_Plaats', 'Ontvanger_Huisnummer',
                   # Verwerker
                   'Verwerker', 'Verwerker_Postcode', 'Verwerker_Straat',
                   'Verwerker_Plaats', 'Verwerker_Huisnummer']

    # filter columns from original dataframe
    logging.info('Filter requested columns...')
    try:
        LMA = dataframe[LMA_columns]
        logging.info('Original entries: {}'.format(len(LMA.index)))
    except Exception as error:
        logging.critical(error)
        sys.exit(1)

    # filter empty fields
    logging.info('Skip empty fields...')
    non_empty_fields = ['Afvalstroomnummer', 'VerwerkingsmethodeCode',
                        'EuralCode', 'MeldPeriodeJAAR', 'MeldPeriodeMAAND',
                        'Gewicht_KG', 'Aantal_vrachten',
                        # Ontdoener
                        'Ontdoener', 'Ontdoener_Postcode',
                        # Herkomst
                        'Herkomst_Postcode',
                        # # Afzender
                        # 'Afzender', 'Afzender_Postcode',
                        # # Inzamelaar
                        # 'Inzamelaar', 'Inzamelaar_Postcode',
                        # # Bemiddelaar
                        # 'Bemiddelaar', 'Bemiddelaar_Postcode',
                        # # Handelaar
                        # 'Handelaar', 'Handelaar_Postcode',
                        # # Ontvanger
                        # 'Ontvanger', 'Ontvanger_Postcode',
                        # Verwerker
                        'Verwerker', 'Verwerker_Postcode']

    # keep original EXCEL indexes
    LMA['idx'] = LMA.index + 2

    for field in non_empty_fields:
        # keep rows with non empty field
        not_empty = LMA[field].notnull()
        condition = not_empty

        # check postcode
        role = field.split('_')[0]
        if 'Postcode' in field and role != 'Verwerker':
            # valid postcode: 6 characters
            valid_postcode = LMA[field].str.len() == 6
            condition = condition & valid_postcode

            # check country if postcode is empty/invalid & keep non-Dutch
            # note: Verwerker is always in the Netherlands!
            country = role + '_Land'
            not_dutch = LMA[country].str.lower() != 'nederland'
            condition = condition | not_dutch

        # check for non-zero amounts
        elif field in ['Gewicht_KG', 'Aantal_vrachten']:
            non_zero_amounts = LMA[field] > 0
            condition = condition & non_zero_amounts

        # filter on condition
        error = list(LMA[~condition].idx)
        LMA = LMA[condition]
        if error:
            logging.warning('Lines {} with no {}'.format(error, field))

    logging.info('Final entries: {}'.format(len(LMA.index)))
