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

    for field in non_empty_fields:
        # count row where field is empty
        e = len(LMA.index) - LMA[field].count()
        if e > 0:
            logging.warning('{} lines with no {}'.format(e, field))

            # keep rows with non empty field
            not_empty = LMA[field].notnull()

            # if no postcode, check if country is not Netherlands
            # note: Verwerker is always in the Netherlands!
            role = field.split('_')[0]
            if 'Postcode' in field and role != 'Verwerker':
                country = role + '_Land'
                LMA = LMA[(not_empty) | (LMA[country] != 'Nederland')]
            else:
                LMA = LMA[not_empty]

    logging.info('Final entries: {}'.format(len(LMA.index)))

    # print(len(LMA.index) - LMA['MeldPeriodeMAAND'].count(),)
    # print('lines do not have a month specified and will be removed')
#
#     # Remove those data entries that have empty fields
#     LMA = LMA[LMA.MeldPeriodeJAAR.notnull()]
#     LMA = LMA[LMA.MeldPeriodeMAAND.notnull()]
#
#
#     # --2-- filter invalid fields
#
#     print(len(LMA.index) - LMA[LMA['Gewicht_KG'] < 1].count(),)
#     print('lines have specified 0 weight and will be removed')
#
#     print(len(LMA.index) - LMA[LMA['Aantal_vrachten'] < 1].count(),)
#     print('lines have specified 0 trips and will be removed')
#
#     for role in roles:
#
#         print(len(LMA.index) - LMA[len(LMA[role + '_Postcode']) < 6].count(),)
#         print('lines have invalid {0} postcode and will be removed'.format(role))
#
#
#     LMA = LMA[LMA[LMA['Gewicht_KG'] >= 1]
#     LMA = LMA[LMA[LMA['Aantal_vrachten'] >= 1]
#
#     for role in roles:
#
#         print(len(LMA.index) - LMA[len(LMA[role + '_Postcode']) < 6].count(),)
#         print('lines have invalid {0} postcode and will be removed'.format(role))
