# Clean typos
import numpy as np
import logging


def clean_description(desc):
    desc = desc.strip()
    desc = desc.lower()
    desc = desc.replace(u'\xa0', u' ')
    desc = ' '.join(desc.split())
    if desc == 'nan':
        return np.NaN
    return desc


def clean_postcode(postcode):
    postcode = postcode.strip()
    postcode = postcode.replace(' ','')
    postcode = postcode.upper()
    if '0000' in postcode:
        return ''
    return postcode


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

    return name


def clean_address(address):
    address = address.strip()
    address = address.upper()
    address = ' '.join(address.split())
    return address


def clean_huisnr(nr):
    nr = nr.split('.')[0]
    nr = ''.join(filter(lambda x: x in '0123456789', nr))
    return nr


def run(dataframe, roles):
    # clean the BenamingAfval field
    logging.info('Cleaning descriptions...')
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].astype('unicode')
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].apply(clean_description)

    # clean role columns
    logging.info('Cleaning role columns...')
    for role in roles:
        # list columns for cleaning
        postcode = '{0}_Postcode'.format(role)
        plaats = '{0}_Plaats'.format(role)
        straat = '{0}_Straat'.format(role)
        huisnr = '{0}_Huisnr'.format(role)
        orig_name = '{0}_Origname'.format(role)

        # clean postcode
        dataframe[postcode] = dataframe[postcode].astype('unicode')
        dataframe[postcode] = dataframe[postcode].apply(clean_postcode)

        # clean company name
        # note: Herkomst does not have name!
        if role != 'Herkomst':
            # preserve the original name
            dataframe[orig_name] = dataframe[role].copy()
            dataframe[role] = dataframe[role].astype('unicode')
            dataframe[role] = dataframe[role].apply(clean_company_name)

        # clean city name
        dataframe[plaats] = dataframe[plaats].astype('unicode')
        dataframe[plaats] = dataframe[plaats].apply(clean_address)

        # clean street name
        dataframe[straat] = dataframe[straat].astype('unicode')
        dataframe[straat] = dataframe[straat].apply(clean_address)

        # clean house number
        dataframe[huisnr] = dataframe[huisnr].astype('unicode')
        dataframe[huisnr] = dataframe[huisnr].apply(clean_huisnr)

    return dataframe
