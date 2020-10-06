# Clean typos
# Recognize the same companies

import numpy as np
import pandas as pd


def clean_company_name(name):

    # remove all non-ASCII characters
    orig_name = name
    printable = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c'
    name = "".join(filter(lambda x: x in printable, name))

    name = name.upper()

    litter = [' SV', 'S V', 'S.V.', ' BV', 'B V', 'B.V.', ' CV', 'C.V.',
              ' NV', 'N.V.', 'V.O.F', ' VOF', 'V O F', '\'T', '\'S']
    # remove all the littering characters
    for l in litter:
        name = name.replace(l, '')

    name = ' '.join(name.split())

    # check if company name does not contain only digits
    name_copy = name
    for dig in '0123456789':
        name_copy = name_copy.replace(dig, '')
    if len(name_copy) == 0:
        name = ''

    # if len(name) < 2:
    #     print orig_name

    # remove all city names and other typical keywords
    ['AANNEMERSBEDRIJF', 'GEMEENTE']

    return name


def clean_address(address):
    address = address.strip()
    address = address.upper()
    address = ' '.join(address.split())
    return address


def clean_postcode(postcode):
    postcode = postcode.strip()
    postcode = postcode.replace(' ','')
    postcode = postcode.upper()
    if '0000' in postcode:
        return ''
    return postcode


def clean_description(desc):
    desc = desc.strip()
    desc = desc.lower()
    desc = desc.replace(u'\xa0', u' ')
    desc = ' '.join(desc.split())
    if desc == 'nan':
        return np.NaN
    return desc


def clean_huisnr(nr):
    nr = nr.split('.')[0]
    nr = ''.join(filter(lambda x: x in '0123456789', nr))
    return nr


def clean_nace(nace):
    nace = ''.join(filter(lambda x: x in '0123456789', nace))
    return nace


def run(dataframe, roles):
    """

    :param dataframe:
    """

    # clean the BenamingAfval field
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].astype('unicode')
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].apply(clean_description)

    actorsets = []  # list of all available actors for recognising the same companies
    actor_data_cols = ['Name', 'Orig_name', 'Postcode']

    for role in roles:

        postcode = '{0}_Postcode'.format(role)
        plaats = '{0}_Plaats'.format(role)
        straat = '{0}_Straat'.format(role)
        huisnr = '{0}_Huisnr'.format(role)
        orig_name = '{0}_Origname'.format(role)

        # data cleaning
        dataframe[postcode] = dataframe[postcode].astype('unicode')
        dataframe[postcode] = dataframe[postcode].apply(clean_postcode)

        if role != 'Herkomst':
            # preserve the original name
            dataframe[orig_name] = dataframe[role].copy()
            dataframe[role] = dataframe[role].astype('unicode')
            dataframe[role] = dataframe[role].apply(clean_company_name)

        dataframe[plaats] = dataframe[plaats].astype('unicode')
        dataframe[plaats] = dataframe[plaats].apply(clean_address)

        dataframe[straat] = dataframe[straat].astype('unicode')
        dataframe[straat] = dataframe[straat].apply(clean_address)

        dataframe[huisnr] = dataframe[huisnr].astype('unicode')
        dataframe[huisnr] = dataframe[huisnr].apply(clean_huisnr)

        # making a list of all available actor names & postcodes to recognise the same company

        if role == 'Herkomst':
            continue  # herkomst is not a separate role but just a disposal location
        else:
            actorset = dataframe[[role, orig_name, postcode]]

        actorset.columns = actor_data_cols

        actorsets.append(actorset)

    # recognising the same companies
    actors = pd.concat(actorsets)
    actors.drop_duplicates(inplace=True)

    # 1) BY CLEANED NAME ONLY
    names = actors[['Name']].drop_duplicates()
    print(names)

    # 2) BY POSTCODE & SIMILAR NAME



# #### dev launch #### #

roles = ['Ontdoener', 'Herkomst', 'Verwerker']

dataframe = pd.read_excel('Testing_data/2_filtered_dataset.xlsx')

run(dataframe, roles)
