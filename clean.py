# Clean typos
# Recognize the same companies

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
import logging
import re


def clean_description(desc):
    # turn multiple spaces to single ones
    desc = re.sub('\s{2,}', ' ', desc)

    # turn to lowercase
    desc = desc.lower()

    # remove leading/ending spaces
    desc = desc.strip()
    
    return desc

#     desc = desc.strip().lower()
#     desc = desc.replace(u'\xa0', u' ')
#     desc = ' '.join(desc.split())
#     if desc == 'nan':
#         return np.NaN
#     return desc


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
    # litter = ['BV', 'SV', 'CV', 'NV', 'VOF', 'VVE', 'BEDRIJ', 'GEMEENTE']
    # not any(l in word for l in exact_match) and
    name = [word for word in name if len(word)]
    name = ' '.join(name)

    # # turn all characters into single space (except numbers & capital letters)
    # name = re.sub('[^0-9A-Z]', ' ', name)
    #
    # name = name.strip()
    #
    # # remove litter
    # litter = [' SV ', ' S V ',
    #           ' BV ', ' B V ',
    #           ' CV ', ' C V ',
    #           ' NV ', ' N V ',
    #           ' VOF ', ' V O F ']


    # # remove all non-ASCII characters
    # orig_name = name
    # printable = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c'
    # name = "".join(filter(lambda x: x in printable, name))
    #
    # name = name.upper()
    #
    # litter = [' SV', 'S V', 'S.V.', ' BV', 'B V', 'B.V.', ' CV', 'C.V.',
    #           ' NV', 'N.V.', 'V.O.F', ' VOF', 'V O F', '\'T', '\'S']
    # # remove all the littering characters
    # for l in litter:
    #     name = name.replace(l, '')
    #
    # name = ' '.join(name.split())
    #
    # # check if company name does not contain only digits
    # name_copy = name
    # for dig in '0123456789':
    #     name_copy = name_copy.replace(dig, '')
    # if len(name_copy) == 0:
    #     name = ''

    # if len(name) < 2:
    #     print orig_name

    return name


def name_similarity(group):
    # check if there is more than one company in the group
    distinct = group.drop_duplicates(subset=['Name'])
    if len(distinct.index) == 1:
        return group

    # dataframe inner join on postcode
    matrix = pd.merge(group, group, on='Postcode', how='inner')
    s = int(len(matrix.index)**0.5)
    matrix = matrix[np.triu(np.ones((s, s))).astype(np.bool).flatten()]

    # compute name similarity
    matrix['text_dist'] = matrix.apply(lambda x: fuzz.ratio(str(x['Name_x']), str(x['Name_y'])), axis=1)
    print(matrix[['Name_x', 'Name_y', 'text_dist']])

    # similarity over 50%
    matrix = matrix[matrix['text_dist'].between(50, 99)]

    # # takes pandas group & returns the most similar name in the group and its similarity score
    #
    # # check if there is more than one company in the group
    # groupdistinct = group.drop_duplicates(subset=['Name'])
    # if len(groupdistinct.index) == 1:
    #     return group

    # # clean company names from all the typical keywords
    # keywords = ['GEMEENTE', 'AANNEMERSBEDRIJF']
    #
    # for keyword in keywords:
    #     group['Company'].replace(keyword, '', inplace=True)
    #
    # group['Company'] = group['Company'].str.strip()
    #
    # # check if there is still more than one company in the group
    # groupdistinct = group.drop_duplicates(subset=['Company'])
    # if len(groupdistinct.index) == 1:
    #     return group
    #
    # # find how similar company names are within the group
    # matrix = pd.merge(group[['Company', 'Postcode']], group[['Company', 'Postcode']], on='Postcode')
    # matrix['text_dist'] = matrix.apply(lambda x: fuzz.ratio(str(x['Company_x']), str(x['Company_y'])), axis=1)
    #
    # # remove self matches
    # matrix = matrix[matrix['text_dist'] != 100]
    #
    # print(matrix)

    # find the right threshold for assuming name similarity

    # distances.reset_index(inplace=True)
    # text_distances = distances[distances['text_dist'] >= 50]
    # matched_text = text_distances.loc[text_distances.groupby(['Key'])['text_dist'].idxmax()]


def run(df, roles):
    """

    :param dataframe:
    """
    # clean the BenamingAfval field
    # remove extra spaces & turn to lowercase
    # dataframe['BenamingAfval'] = dataframe['BenamingAfval'].astype('unicode')
    logging.info('Clean descriptions (BenamingAfval)...')
    df.loc[df['BenamingAfval'].notnull(), 'BenamingAfval'] = df['BenamingAfval'].astype(str).apply(clean_description)\

    actorsets = []  # list of all available actors for recognising the same companies
    # actor_data_cols = ['Name', 'Orig_name', 'Postcode', 'Plaats', 'Straat', 'Huisnr']
    actor_data_cols = ['Name', 'Postcode']

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

        if role != 'Herkomst':
            # clean company name
            # Herkomst has none!
            orig_name = 'Orig_' + role
            df[orig_name] = df[role]
            df.loc[df[role].notnull(), role] = df[role].astype(str).apply(clean_company_name)

            # clean company nameos of:
            # 1) postcodes
            # df[role] = df.apply(lambda x: x[role].replace(str(x[postcode]), ''), axis=1)

            # 2) street names
            # df[role] = df.apply(lambda x: x[role].replace(str(x[straat]), ''), axis=1)

            # 3) city names
            # df[role] = df.apply(lambda x: x[role].replace(str(x[plaats]), ''), axis=1)

            # retrieve actor-related columns & add to actorsets
            # actorset = df[[role,postcode, plaats, straat, huisnr]]
            actorset = df[[role, postcode]]
            actorset.columns = actor_data_cols
            actorsets.append(actorset)

    # combine all actorsets to one dataframe
    actors = pd.concat(actorsets)

    # keep unique company names
    actors.drop_duplicates(inplace=True)

    # group by actors by 4-digit postcode
    code_groups = actors.groupby('Postcode')
    logging.info('Match companies...')
    code_groups.apply(name_similarity)

    # print(len(actors))

            # # preserve the original name
            # dataframe[orig_name] = dataframe[role].copy()
            # dataframe[role] = dataframe[role].astype('unicode')
            # dataframe[role] = dataframe[role].apply(clean_company_name)
            #
            # # clean company names from the street & city names

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
