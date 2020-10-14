import logging
import numpy as np
import re


# clean descriptions
def clean_description(desc):
    # turn multiple spaces to single ones
    desc = re.sub('\s{2,}', ' ', desc)

    # turn to lowercase
    desc = desc.lower()

    # remove leading/ending spaces
    desc = desc.strip()

    return desc


# clean postcodes
def clean_postcode(postcode):
    # postcodes should contain only:
    # 1) numbers
    # 2) capital letters

    # turn to capital letters
    postcode = postcode.upper()

    # remove all characters except numbers & capital letters
    postcode = re.sub('[^0-9A-Z]', '', postcode)

    return postcode


# clean addresses
def clean_address(address):
    # city & street names should contain only:
    # 1) capital letters
    # 2) single space between

    # turn to capital letters
    address = address.upper()

    # replace non-letter characters with single space
    address = re.sub('[^A-Z]', ' ', address)

    # split by space & keep words with more that one letters
    address = ' '.join([word for word in address.split() if len(word) > 1])

    return address


# clean house numbers
def clean_huisnr(nr):
    # house numbers should contain only numbers
    # split with decimal point & keep integer part
    nr = nr.split('.')[0]

    # remove non-numeric characters
    nr = re.sub('[^0-9]', '', nr)

    return nr


# clean company names
def clean_company_name(name):
    # turn to capital letters
    name = name.upper()

    # split by space
    name = name.split()

    # remove non alphanumeric characters
    name = [re.sub('[^0-9A-Z]', '', word) for word in name]

    # remove company forms & no characters
    litter = ['BV', 'SV', 'CV', 'NV', 'VOF', 'VVE', 'BEDRIJ', 'GEMEENTE']
    name = [word for word in name if not any(l in word for l in litter) and len(word)]
    name = ' '.join(name)

    return name


def run(df, roles):
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
                   'Verwerker_Plaats', 'Verwerker_Huisnr']

    # filter columns from original dataframe
    logging.info('Filter requested columns...')
    try:
        df = df[LMA_columns]
        original_entries = len(df.index)
        logging.info('Original entries: {}'.format(original_entries))
    except Exception as error:
        logging.critical(error)
        raise

    # if 'Herkomst' has all columns empty, this means that 'Herkomst' is the same as 'Ontdoener'
    # copy all 'Herkomst' columns values from 'Ontdoener'
    Herkomst_columns = [col for col in df.columns if 'Herkomst' in col]
    all_null = [df[col].isnull() for col in Herkomst_columns]
    idx = df[np.bitwise_and.reduce(all_null)].index
    for col in Herkomst_columns:
        orig_col = 'Ontdoener_' + col.split('_')[-1]
        df.loc[idx, col] = df[orig_col]

    # clean descriptions
    logging.info('Clean descriptions (BenamingAfval)...')
    df.loc[df['BenamingAfval'].notnull(), 'BenamingAfval'] = df['BenamingAfval'].astype(str).apply(clean_description)

    # check actor columns for each role
    # cast non-null values as strings & apply cleaning function
    logging.info('Clean location name & info...')
    for role in roles:
        # clean postcode
        postcode = role + '_Postcode'
        df.loc[df[postcode].notnull(), postcode] = df[postcode].astype(str).apply(clean_postcode)

        # clean plaats
        plaats = role + '_Plaats'
        df.loc[df[plaats].notnull(), plaats] = df[plaats].astype(str).apply(clean_address)

        # clean straat
        straat = role + '_Straat'
        df.loc[df[straat].notnull(), straat] = df[straat].astype(str).apply(clean_address)

        # clean huisnr
        huisnr = role + '_Huisnr'
        df.loc[df[huisnr].notnull(), huisnr] = df[huisnr].astype(str).apply(clean_huisnr)

        # clean company name
        # Herkomst has none!
        if role != 'Herkomst':
            orig_name = 'Orig_' + role
            df[orig_name] = df[role]
            df.loc[df[role].notnull(), role] = df[role].astype(str).apply(clean_company_name)

            # # clean company names of:
            # # 1) postcodes
            # df[role] = df.apply(lambda x: x[role].replace(str(x[postcode]), ''), axis=1)
            #
            # # 2) street names
            # df[role] = df.apply(lambda x: x[role].replace(str(x[straat]), ''), axis=1)
            #
            # # 3) city names
            # df[role] = df.apply(lambda x: x[role].replace(str(x[plaats]), ''), axis=1)

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
    df['idx'] = df.index + 2

    for field in non_empty_fields:
        # keep rows with non empty field
        not_empty = df[field].notnull()
        condition = not_empty

        # check postcode
        role = field.split('_')[0]
        if 'Postcode' in field:
            # valid postcode: (4) leading numeric characters
            # note: to match later with postcode polygons if geocoding fails
            valid_postcode = df[field].str[:4].str.isnumeric()
            condition = condition & valid_postcode

            # check country if postcode is empty/invalid & keep non-Dutch
            # note: Verwerker is always in the Netherlands!
            if role != 'Verwerker':
                country = role + '_Land'
                # ignore nan values
                not_dutch = df[df[country].notnull()][country].str.lower() != 'nederland'
                condition = condition | not_dutch

        # check amounts
        elif field in ['Gewicht_KG', 'Aantal_vrachten']:
            non_zero_amounts = df[field] > 0
            condition = condition & non_zero_amounts

        # filter on condition
        error = list(df[~condition].idx)
        df = df[condition]
        if error:
            logging.warning('Lines {} with no/invalid {}'.format(len(error), field))

    # check average trip amount
    # it should between (1kg, 30t)
    condition = (df['Gewicht_KG'] /df['Aantal_vrachten']).between(1, 30000)
    error = list(df[~condition].idx)
    df = df[condition]
    if error:
        logging.warning('Lines {} with amount per trip not between (1kg, 30t)'.format(len(error)))

    final_entries = len(df.index)
    ratio = round(final_entries / original_entries * 100, 1)
    logging.info('Final entries: {} ({}%)'.format(final_entries, ratio))

    return df