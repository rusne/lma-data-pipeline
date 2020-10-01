# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 2019

@author: geoFluxus Team

"""

import pandas as pd
import time
from clean import clean_company_name
from clean import clean_address
from clean import clean_postcode
from clean import clean_nace
import math

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

# TODO!!! update description
# Reads the original KvK file, filters the relevant columns,
# unifies NACE codes to 4 or 5 digits,
# filters out companies without a NACE code,
# prepares unlocated entries for the geolocation

priv_folder = "Private_data/"
pub_folder = "Public_data/"

NACEtable = pd.read_excel(pub_folder + 'NACE_table.xlsx', sheet_name='NACE_nl')

print("\nLoading KvK dataset........")
start_time = time.time()
orig_KvK_dataset = priv_folder + "KvK_data/raw_data/KvK AMA 31-10-2018 _ all.xlsx"
# orig_KvK_dataset = priv_folder + "KvK_data/raw_data/KvK AMA 31-10-2018 _ 1.xlsx"
KvK = pd.read_excel(orig_KvK_dataset, dtype={'SBI': object})

print('KvK dataset has been loaded, ',)
print('dataset length:', len(KvK.index), 'lines,',)
m, s = divmod(time.time() - start_time, 60)
print('time elapsed:', m, 'min', s, 's')

# for testing
# KvK = KvK.head(100)

# filter incorrect NACE codes
pre = len(KvK.index)
KvK['SBI'] = KvK['SBI'].astype(str)
KvK['SBI'] = KvK['SBI'].apply(clean_nace)

KvK = KvK[KvK['SBI'].str.len() < 6]
KvK = KvK[KvK['SBI'].str.len() > 3]

KvK = KvK[KvK['SBI'].astype(int) != 100]
KvK['SBI'] = KvK['SBI'].apply(lambda x: str(x)[:4])

# match with the list of NACE activities, skip if not present
NACEtable['Digits'] = NACEtable['Digits'].astype(str)
NACEtable['Digits'] = NACEtable['Digits'].str.zfill(4)
KvK = pd.merge(KvK, NACEtable[['Digits']], left_on='SBI', right_on='Digits', validate='m:1')

print(pre - len(KvK.index), 'lines have been filtered due to an invalid NACE')

name_versions = ['HN_1X45', 'HN_1X2X30', 'HN_1X30']
loc_versions = ['_1', '_CA']

all_versions = []
for name_col in name_versions:
    for loc_col in loc_versions:

        # selection of the columns we want to include in our analysis
        KvK_columns = [name_col, 'STRAATNAAM' + loc_col, 'HUISNR' + loc_col,
                       'POSTCODE' + loc_col, 'WOONPLAATS' + loc_col, 'SBI']

        KvK_ver = KvK[KvK_columns]
        KvK_ver.columns = ['zaaknaam', 'straat', 'huisnr', 'postcode', 'plaats', 'activenq']

        # encoding as unicode (in case there are non-ascii characters)
        # KvK_ver['zaaknaam'] = KvK_ver['zaaknaam'].apply(lambda x: unicode(x))
        # KvK_ver['straat'] = KvK_ver['straat'].apply(lambda x: unicode(x))
        # KvK_ver['plaats'] = KvK_ver['plaats'].apply(lambda x: unicode(x))
        KvK_ver['zaaknaam'] = KvK_ver['zaaknaam'].astype(str)
        KvK_ver['straat'] = KvK_ver['straat'].astype(str)
        KvK_ver['plaats'] = KvK_ver['plaats'].astype(str)
        KvK_ver['postcode'] = KvK_ver['postcode'].astype(str)

        # filter invalid company names
        KvK_ver['orig_zaaknaam'] = KvK_ver['zaaknaam'].copy()  # copy of the orig name
        pre = len(KvK_ver.index)
        KvK_ver['zaaknaam'] = KvK_ver['zaaknaam'].apply(clean_company_name)
        KvK_ver = KvK_ver[KvK_ver['zaaknaam'].str.len() > 1]

        print(pre - len(KvK_ver.index), 'lines have been filtered due to an invalid name')

        # clean addresses
        KvK_ver['postcode'] = KvK_ver['postcode'].apply(clean_postcode)
        KvK_ver['straat'] = KvK_ver['straat'].apply(clean_address)
        KvK_ver['plaats'] = KvK_ver['plaats'].apply(clean_address)

        # filter invalid postcodes
        pre = len(KvK_ver.index)
        KvK_ver = KvK_ver[KvK_ver['postcode'].str.len() == 6]

        print(pre - len(KvK_ver.index), 'lines have been filtered due to an invalid postcode')

        # join address into a single column for easier geolocation
        KvK_ver['adres'] = KvK_ver['straat'].astype(str) + ' ' + KvK_ver['huisnr'].astype(str)

        # create a key for each separate actor (name + postcode)
        KvK_ver['key'] = KvK_ver['zaaknaam'].str.cat(KvK_ver[['postcode']], sep=' ')

        all_versions.append(KvK_ver.copy())

# ______________________________________________________________________________
#   Concatenating all the years into a single dataset
# ______________________________________________________________________________

# ASSUMPTION:
all_KvK = pd.concat(all_versions)
all_KvK.drop_duplicates(subset=['key', 'activenq'], inplace=True)

print('\nAfter removing duplicates',)
print(len(all_KvK.index), 'actors remain in total')

pre = len(all_KvK.index)
# find out how many companies with the same postcode have different NACE
all_KvK['count'] = all_KvK.groupby(['zaaknaam', 'postcode'])['activenq'].transform('count')
duplicates = all_KvK[all_KvK['count'] > 1]
duplicates.to_excel(priv_folder + 'KvK_data/auxiliary/duplicates.xlsx')

# for companies that have more than one NACE code, choose randomly which one to use
all_KvK.drop_duplicates(subset=['zaaknaam', 'postcode'], inplace=True)

print(pre - len(all_KvK.index), 'companies have been assigned to more than one NACE code')
print(len(all_KvK.index), 'unique company name and postcode combinations remain')

all_KvK['in2018'] = 'JA'

all_KvK.to_excel(priv_folder + 'KvK_data/all_KvK_part1.xlsx')

# all_KvK = pd.read_excel(priv_folder + 'KvK_data/all_KvK_part1.xlsx')
# ______________________________________________________________________________
#   Preparing dataset for geolocation
# ______________________________________________________________________________

all_KvK.drop_duplicates(subset=['key'], inplace=True)

# # TEMP: something has already been located....
# KvK_loc = pd.read_csv(priv_folder + 'KvK_data/KvK_RDnew_MRA.csv', sep=',')
#
#
# # KvK dataset needs to be connected back with the addresses
# KvK_key_add = all_KvK[['key', 'adres', 'postcode', 'plaats']].copy()
# KvK_key_add.drop_duplicates(inplace=True)
# KvK_loc = pd.merge(KvK_loc, KvK_key_add, on='key')
# KvK_loc.drop(columns=['key'], inplace=True)
# all_KvK = pd.merge(all_KvK, KvK_loc, on=['adres', 'postcode', 'plaats'], how='left')
#
# located = all_KvK[all_KvK['WKT'].isna() == False]
# unlocated = all_KvK[all_KvK['WKT'].isna() == True]
# unlocated = unlocated[['key', 'adres', 'postcode', 'plaats']]

unlocated = all_KvK[['key', 'adres', 'postcode', 'plaats']]
unlocated.drop_duplicates(subset=['adres', 'postcode', 'plaats'], inplace=True)
# unlocated.to_csv(priv_folder + 'KvK_data/KvK_unlocated.csv', index=False, encoding='utf8')
print(len(unlocated.index), 'actors need to be geolocated')

# check if all actors can be geolocated at once
if len(unlocated.index) > 25000:
    parts = int(math.ceil(len(unlocated.index) / 25000.0))
    for i in range(parts):
        start = i * 25000
        end = (i + 1) * 25000
        if end > len(unlocated.index):
            end = len(unlocated.index)
        print(start, end)
        part = unlocated[start:end]
        part.to_excel(priv_folder + 'KvK_data/KvK_unlocated_{0}_extra.xlsx'.format(end), index=False)
else:
    unlocated.to_excel(priv_folder + 'KvK_data/KvK_unlocated.xlsx', index=False)
