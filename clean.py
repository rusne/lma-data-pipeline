# Clean typos
import logging
import numpy as np
import geolocate
import variables as var

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


def clean_nace(nace):
    nace = ''.join(filter(lambda x: x in '0123456789', nace))
    return nace


def run(dataframe):
    # clean the BenamingAfval field
    logging.info('Cleaning descriptions...')
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].astype('unicode')
    dataframe['BenamingAfval'] = dataframe['BenamingAfval'].apply(clean_description)

    # clean role columns
    roles = var.roles
    for role in roles:
        logging.info(f'Cleaning {role} columns...')

        # list columns for cleaning
        orig_name = f'{role}_Origname'
        straat = f'{role}_Straat'
        huisnr = f'{role}_Huisnr'
        postcode = f'{role}_Postcode'
        plaats = f'{role}_Plaats'

        # clean company name
        # note: Herkomst does not have name!
        if role != 'Herkomst':
            # preserve the original name
            dataframe[orig_name] = dataframe[role].copy()
            dataframe[role] = dataframe[role].astype('str')
            dataframe[role] = dataframe[role].apply(clean_company_name)

        # clean street name
        dataframe[straat] = dataframe[straat].astype('str')
        dataframe[straat] = dataframe[straat].apply(clean_address)

        # clean house number
        dataframe[huisnr] = dataframe[huisnr].astype('str')
        dataframe[huisnr] = dataframe[huisnr].apply(clean_huisnr)

        # clean postcode
        dataframe[postcode] = dataframe[postcode].astype('str')
        dataframe[postcode] = dataframe[postcode].apply(clean_postcode)

        # clean city name
        dataframe[plaats] = dataframe[plaats].astype('str')
        dataframe[plaats] = dataframe[plaats].apply(clean_address)

        # prepare address for geolocation
        dataframe[f'{role}_Adres'] = dataframe[straat].str.cat(dataframe[[huisnr, postcode, plaats]], sep=' ')

        # geolocate
        # logging.info(f'Geolocate for {role}...')
        # addresses = dataframe[[f'{role}_Adres', postcode]]
        # addresses.columns = ['adres', 'postcode']
        # dataframe[f'{role}_Location'] = geolocate.run(addresses)

    return dataframe
