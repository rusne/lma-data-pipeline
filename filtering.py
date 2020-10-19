"""
This module filters useful columns and data points in the original data file
logs how many erroneous data points have been filtered on which basis
and returns a dataframe ready for the further analysis
"""
import logging


def run(dataframe, roles):
    # selection of the columns we want to include in our analysis
    LMA_columns = ['Afvalstroomnummer', 'VerwerkingsmethodeCode',
                   'VerwerkingsOmschrijving', 'RouteInzameling',
                   'Inzamelaarsregeling', 'ToegestaanbijInzamelaarsregeling',
                   'EuralCode', 'BenamingAfval', 'MeldPeriodeJAAR',
                   'MeldPeriodeMAAND', 'Gewicht_KG', 'Aantal_vrachten',
                   # Ontdoener
                   'Ontdoener', 'Ontdoener_Postcode', 'Ontdoener_Plaats',
                   'Ontdoener_Straat', 'Ontdoener_Huisnr',
                   # Herkomst
                   'Herkomst_Postcode', 'Herkomst_Straat', 'Herkomst_Plaats',
                   'Herkomst_Huisnr',
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
                   'Verwerker_Plaats', 'Verwerker_Huisnr']

    # filter the requested columns from the original dataframe
    try:
        LMA = dataframe[LMA_columns]
        logging.info(f'Original dataset length: {len(LMA.index)} lines')
    except Exception as error:
        logging.critical(error)
        raise

    # filter empty fields
    # log all empty fields before removing them
    # empty role columns
    for role in roles:
        # empty company name
        # note: Herkomst does not have company name!
        if role != 'Herkomst':
            e = len(LMA[LMA[role].isnull()].index)
            if e:
                LMA = LMA[LMA[role].notnull()]
                logging.warning(f'{e} lines do not have a {role} specified and will be removed')

        # empty postcode
        postcode = role + '_Postcode'
        e = len(LMA[LMA[postcode].isnull()].index)
        if e:
            LMA = LMA[LMA[postcode].notnull()]
            logging.warning(f'{e} lines do not have a {postcode} specified and will be removed')

    # empty year
    e = len(LMA[LMA['MeldPeriodeJAAR'].isnull()].index)
    if e:
        LMA = LMA[LMA.MeldPeriodeJAAR.notnull()]
        logging.warning(f'{e} lines do not have a year specified and will be removed')

    # empty month
    e = len(LMA[LMA['MeldPeriodeMAAND'].isnull()].index)
    if e:
        LMA = LMA[LMA.MeldPeriodeMAAND.notnull()]
        logging.warning(f'{e} lines do not have a month specified and will be removed')

    # zero amount
    e = len(LMA[LMA['Gewicht_KG'] < 1].index)
    if e:
        LMA = LMA[LMA['Gewicht_KG'] >= 1]
        logging.warning(f'{e} lines have specified 0 weight and will be removed')

    # zero trips
    e = len(LMA[LMA['Aantal_vrachten'] < 1].index)
    if e:
        LMA = LMA[LMA['Aantal_vrachten'] >= 1]
        logging.warning(f'{e} lines have specified 0 trips and will be removed')

    logging.info(f'Final dataset length: {len(LMA.index)} lines')

    return LMA
