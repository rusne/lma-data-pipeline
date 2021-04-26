import pandas as pd
import logging

# from src import classify, clean, connect_nace, filtering, prepare_kvk
from src import assign_nace

logging.info("Import full LMA dataset...")
LMA_dataframe = pd.read_excel('Testing_data/3_cleaned_dataset.xlsx')

# import KvK dataset with NACE codes
# NOTE: this dataset is the result of prepare_kvk.py
logging.info("Import KvK dataset with geolocation...")
KvK_dataframe = pd.read_csv("Private_data/2018_KvK.csv", low_memory=False)

LMA_actors, KvK_companies = assign_nace.prepare_data(LMA_dataframe, KvK_dataframe)
matched, remaining = assign_nace.exact_match(LMA_actors, KvK_companies)
search_space = assign_nace.make_search_space(remaining, KvK_companies)

# search_space = pd.read_excel('Testing_data/4_search_space.xlsx')
# control, remaining = assign_nace.probable_match(search_space, geom_loaded=False)

control, remaining = assign_nace.probable_match(search_space, geom_loaded=True)

result = pd.concat([matched, control])
result = result[['LMA_key', 'LMA_origname', 'LMA_address', 'LMA_eural',
                 'KvK_origname', 'KvK_address', 'KvK_postcode', 'KvK_sbi',
                 'KvK_ag', 'match', 'dist', 'ratio']]
result.to_excel('result.xlsx')
remaining.to_excel('remaining.xlsx')

# assign_nace.validate()
#
# dataframe = pd.read_excel('Testing_data/4_search_space.xlsx')
# assign_nace.run(dataframe)


# KvK_actors = pd.read_csv("Private_data/all_KvK.csv", low_memory=False)
# KvK_actors['activenq'] = KvK_actors['activenq'].astype(str).str.zfill(4).str[:4]
#
# KvK_actors = KvK_actors[KvK_actors['in2018'] == 'JA']
#
# KvK_actors = KvK_actors[['zaaknaam', 'orig_zaaknaam', 'postcode', 'adres',
#        'plaats', 'activenq', 'key', 'wkt', 'AG']]
#
# KvK_actors.to_csv('Private_data/2018_KvK.csv')
