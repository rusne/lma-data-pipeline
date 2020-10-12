# Clean typos
# Recognize the same companies

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
import logging
import re


# clean postcodes
def clean_postcode(postcode):
    # postcodes should contain only:
    # 1) numbers
    # 2) capital letters

    # turn to capital letters
    postcode = postcode.upper()

    # remove all characters except numbers & capital letters
    postcode = re.sub('[^0-9A-Z]', '', postcode)

    # postcode = postcode.strip()
    # postcode = postcode.replace(' ','')
    # postcode = postcode.upper()
    # if '0000' in postcode:
    #     return ''

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


# def clean_company_name(name):
#
#     # remove all non-ASCII characters
#     orig_name = name
#     printable = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c'
#     name = "".join(filter(lambda x: x in printable, name))
#
#     name = name.upper()
#
#     litter = [' SV', 'S V', 'S.V.', ' BV', 'B V', 'B.V.', ' CV', 'C.V.',
#               ' NV', 'N.V.', 'V.O.F', ' VOF', 'V O F', '\'T', '\'S']
#     # remove all the littering characters
#     for l in litter:
#         name = name.replace(l, '')
#
#     name = ' '.join(name.split())
#
#     # check if company name does not contain only digits
#     name_copy = name
#     for dig in '0123456789':
#         name_copy = name_copy.replace(dig, '')
#     if len(name_copy) == 0:
#         name = ''
#
#     # if len(name) < 2:
#     #     print orig_name
#
#     return name
#
#
#
#
#
# def clean_description(desc):
#     desc = desc.strip().lower()
#     desc = desc.replace(u'\xa0', u' ')
#     desc = ' '.join(desc.split())
#     if desc == 'nan':
#         return np.NaN
#     return desc
#
#
# def clean_huisnr(nr):
#     nr = nr.split('.')[0]
#     nr = ''.join(filter(lambda x: x in '0123456789', nr))
#     return nr
#
#
# def clean_nace(nace):
#     nace = ''.join(filter(lambda x: x in '0123456789', nace))
#     return nace
#
#
# def name_similarity(group):
#     # takes pandas group & returns the most similar name in the group and its similarity score
#
#     # check if there is more than one company in the group
#     groupdistinct = group.drop_duplicates(subset=['Company'])
#     if len(groupdistinct.index) == 1:
#         return group
#
#     # clean company names from all the typical keywords
#     keywords = ['GEMEENTE', 'AANNEMERSBEDRIJF']
#
#     for keyword in keywords:
#         group['Company'].replace(keyword, '', inplace=True)
#
#     group['Company'] = group['Company'].str.strip()
#
#     # check if there is still more than one company in the group
#     groupdistinct = group.drop_duplicates(subset=['Company'])
#     if len(groupdistinct.index) == 1:
#         return group
#
#     # find how similar company names are within the group
#     matrix = pd.merge(group[['Company', 'Postcode']], group[['Company', 'Postcode']], on='Postcode')
#     matrix['text_dist'] = matrix.apply(lambda x: fuzz.ratio(str(x['Company_x']), str(x['Company_y'])), axis=1)
#
#     # remove self matches
#     matrix = matrix[matrix['text_dist'] != 100]
#
#     print(matrix)
#
#     # find the right threshold for assuming name similarity
#
#     # distances.reset_index(inplace=True)
#     # text_distances = distances[distances['text_dist'] >= 50]
#     # matched_text = text_distances.loc[text_distances.groupby(['Key'])['text_dist'].idxmax()]


def run(df, roles):
    """

    :param dataframe:
    """
    # clean the BenamingAfval field
    # remove extra spaces & turn to lowercase
    # dataframe['BenamingAfval'] = dataframe['BenamingAfval'].astype('unicode')
    logging.info('Clean descriptions (BenamingAfval)...')
    df['BenamingAfval'] = df['BenamingAfval'].str.strip().str.lower()

    actorsets = []  # list of all available actors for recognising the same companies
    # actor_data_cols = ['Name', 'Orig_name', 'Postcode', 'Plaats', 'Straat', 'Huisnr']
    actor_cols = [
        ('Postcode', clean_postcode),
        ('Plaats', clean_address),
        ('Straat', clean_address)
    ]

    # check all actor columns for each role
    # cast non-null values as strings & apply cleaning function
    for role in roles:
        for col in actor_cols:
            name, function = col
            name = '_'.join([role, name])
            df.loc[df[name].notnull(), name] = df[name].astype(str).apply(function)
            print(list(df[name]))


    #
    #     dataframe[plaats] = dataframe[plaats].astype('unicode')
    #     dataframe[plaats] = dataframe[plaats].apply(clean_address)
    #
    #     dataframe[straat] = dataframe[straat].astype('unicode')
    #     dataframe[straat] = dataframe[straat].apply(clean_address)
    #
    #     dataframe[huisnr] = dataframe[huisnr].astype('unicode')
    #     dataframe[huisnr] = dataframe[huisnr].apply(clean_huisnr)
    #
    #     if role != 'Herkomst':
    #         # preserve the original name
    #         dataframe[orig_name] = dataframe[role].copy()
    #         dataframe[role] = dataframe[role].astype('unicode')
    #         dataframe[role] = dataframe[role].apply(clean_company_name)
    #
    #         # clean company names from the street & city names
    #         dataframe[role].replace({postcode, ''}, inplace=True)
    #         dataframe[role].replace({straat, ''}, inplace=True)
    #         dataframe[role].replace({plaats, ''}, inplace=True)
    #
    #
    #     # making a list of all available actor names & postcodes to recognise the same company
    #
    #     if role == 'Herkomst':
    #         continue  # herkomst is not a separate role but just a disposal location
    #     else:
    #         actorset = dataframe[[role, orig_name, postcode, plaats, straat, huisnr]]
    #
    #     actorset.columns = actor_data_cols
    #
    #     actorsets.append(actorset)
    #
    # # recognising the same companies
    # actors = pd.concat(actorsets)
    # actors.drop_duplicates(inplace=True)
    #
    # # 1) BY CLEANED NAME ONLY
    # actors['Company'] = actors['Name']
    #
    # # 2) BY POSTCODE & SIMILAR NAME
    # actors.groupby(actors['Postcode']).apply(name_similarity)



# #### dev launch #### #

# roles = ['Ontdoener', 'Herkomst', 'Verwerker']
#
# dataframe = pd.read_excel('Testing_data/2_filtered_dataset.xlsx')
#
# run(dataframe, roles)
