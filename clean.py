# Clean typos
# Recognize the same companies

import numpy as np


def clean_company_name(name):

    # remove all non-ASCII characters
    orig_name = name
    printable = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c'
    name = "".join(filter(lambda x: x in printable, name))

    name = name.upper()

    litter = [' SV', 'S V', 'S.V.', ' BV', 'B V', 'B.V.', ' CV', 'C.V.',
              ' NV', 'N.V.', 'V.O.F', ' VOF', 'V O F', '\'T'
              # '.', ',', ':', ';', '-', '!', '?', '_', '=',
              # '*', '^', '&', '%', '$', '#', '+', '~', '`', '|', '"',
              # '/', '\'', '\\', '@', '(', ')', '{', '}', '[', ']', '<', '>'
              ]
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
