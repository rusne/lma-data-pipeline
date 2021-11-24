
import pandas as pd
import geopandas as gpd
from shapely import wkt
import sankey

# dataframe = pd.read_csv("Private_data/LMA_data_AMA/ontvangstmeldingen_AMA_2016_2020.csv", low_memory=False)
#
# df2018 = dataframe[dataframe['MeldPeriodeJAAR'] == 2018]
#
# df2018.to_excel("Private_data/LMA_data_AMA_2018/ontvangstmeldingen_AMA_2018.xlsx")
#
# print(len(dataframe.index))
# print(len(df2018.index))
#
# dataframe2 = pd.read_csv("Private_data/LMA_data_AMA/afgiftemeldingen_AMA_2016_2020.csv", low_memory=False)
#
# df20182 = dataframe2[dataframe2['MeldPeriodeJAAR'] == 2018]
#
# df20182.to_excel("Private_data/LMA_data_AMA_2018/afgiftemeldingen_AMA_2018.xlsx")
#
# print(len(dataframe2.index))
# print(len(df20182.index))

# dataframe = pd.read_csv("Private_data/all_KvK.csv", low_memory=False)
#
# print(dataframe.columns)

# ######################## QUERY 1: SPATIAL EXTENT #############################
# ------------------------------------------------------------------------------

# results = pd.read_csv('Private_data/results/AMA_2018/ontvangstmeldingen_AMA_2018_result.csv')
results = pd.read_csv('Private_data/ontvangstmeldingen_AMA_2018_result.csv')

# # convert wkt to geometry
# results['Herkomst_Location'] = results['Herkomst_Location'].apply(wkt.loads)
# results = gpd.GeoDataFrame(results, geometry="Herkomst_Location", crs={"init": "epsg:4326"})
#
# # load Amsterdam municipality and AMA boundaries
# try:
#     boundary_AMS = gpd.read_file("Spatial_data/Amsterdam_gemeente_WGS84.shp")
#     boundary_AMA = gpd.read_file("Spatial_data/Metropoolregio_WGS84.shp")
# except Exception as error:
#     logging.critical(error)
#     raise
#
# # PRODUCED IN AMS
# joined = gpd.sjoin(results, boundary_AMS, how="left", op="within")
# in_boundary = joined[joined["GM_NAAM"].isna() == False].index
# out_boundary = joined[joined["GM_NAAM"].isna()].index
# results.loc[in_boundary, "produced_in_AMS"] = True
# results.loc[out_boundary, "produced_in_AMS"] = False
#
# produced_in_AMS = results[results["produced_in_AMS"] == True]
#
# # convert treatment locations into geometries
#
# produced_in_AMS['Verwerker_Location'] = produced_in_AMS['Verwerker_Location'].apply(wkt.loads)
# produced_in_AMS = gpd.GeoDataFrame(produced_in_AMS, geometry="Verwerker_Location", crs={"init": "epsg:4326"})
#
# # TREATED IN AMS
# joined = gpd.sjoin(produced_in_AMS, boundary_AMS, how="left", op="within")
# in_boundary = joined[joined["GM_NAAM"].isna() == False].index
# out_boundary = joined[joined["GM_NAAM"].isna()].index
# produced_in_AMS.loc[in_boundary, "treated_in_AMS"] = True
# produced_in_AMS.loc[out_boundary, "treated_in_AMS"] = False
#
# # TREATED IN AMA
# joined = gpd.sjoin(produced_in_AMS, boundary_AMA, how="left", op="within")
# in_boundary = joined[joined["RG_NAAM"].isna() == False].index
# out_boundary = joined[joined["RG_NAAM"].isna()].index
# produced_in_AMS.loc[in_boundary, "treated_in_AMA"] = True
# produced_in_AMS.loc[out_boundary, "treated_in_AMA"] = False
#
# produced_in_AMS.to_excel('temp.xlsx')
# produced_in_AMS['Herkomst_Location'] = produced_in_AMS['Herkomst_Location'].apply(wkt.dumps)
# produced_in_AMS['Verwerker_Location'] = produced_in_AMS['Verwerker_Location'].apply(wkt.dumps)
#
# map_cols = ['Herkomst_Location', 'Verwerker_Location', 'Gewicht_KG']
#
# treated_in_AMS = produced_in_AMS[produced_in_AMS["treated_in_AMS"] == True]
# treated_in_AMS = treated_in_AMS[map_cols]
# treated_in_AMA = produced_in_AMS[(produced_in_AMS["treated_in_AMS"] == False) & produced_in_AMS["treated_in_AMA"] == True]
# treated_in_AMA = treated_in_AMA[map_cols]
# treated_in_NL = produced_in_AMS[produced_in_AMS["treated_in_AMA"] == False]
# treated_in_NL = treated_in_NL[map_cols]
#
# print(sum(produced_in_AMS['Gewicht_KG']) / 1000)
# print(sum(treated_in_AMS['Gewicht_KG']) / 1000 + sum(treated_in_AMA['Gewicht_KG']) / 1000 + sum(treated_in_NL['Gewicht_KG']) / 1000)
# print(sum(treated_in_AMS['Gewicht_KG']) / 1000)
# print(sum(treated_in_AMA['Gewicht_KG']) / 1000)
# print(sum(treated_in_NL['Gewicht_KG']) / 1000)
#
# # ##### MAKE A MAP ######
#
# treated_in_AMS = treated_in_AMS.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
# treated_in_AMS.reset_index(inplace=True)
# treated_in_AMS['tag'] = 'Locally treated waste'
# treated_in_AMS['colour'] = 'rgb(156,9,67)'
#
# treated_in_AMA = treated_in_AMA.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
# treated_in_AMA.reset_index(inplace=True)
# treated_in_AMA['tag'] = 'Waste treated in the metropolitan region'
# treated_in_AMA['colour'] = 'rgb(106,193,166)'
#
# treated_in_NL = treated_in_NL.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
# treated_in_NL.reset_index(inplace=True)
# treated_in_NL['tag'] = 'Waste treated outside of the metropolitan region'
# treated_in_NL['colour'] = 'rgb(94,82,160)'
#
# map = pd.concat([treated_in_AMS, treated_in_AMA, treated_in_NL])
#
# map.to_excel('Private_data/map.xlsx')

# ######################## QUERY 2: ECONOMIC SECTORS #############################
# ------------------------------------------------------------------------------


# def cutoff(aggset, factor, transpose=True):
#     # takes a dataframe, sorts and aggregates small values into the "other" field
#     # factor tells how big the "other" can be
#     # in comparison to the biggest value of the set
#
#     if transpose:
#         aggset = aggset.transpose()
#     aggset['sum'] = aggset.sum(axis=1)
#     aggset.sort_values(by='sum', inplace=True)
#     total = 0
#     maxval = list(aggset['sum'])[-1] * factor
#     # # print(maxval)
#     for index, row in aggset.iterrows():
#         total += row['sum']
#         aggset.loc[index, 'cutoff'] = total
#
#     aggset.reset_index(inplace=True)
#     # index_col = aggset.columns[0]
#     aggset.loc[aggset['cutoff'] < maxval, 'agg'] = 'all other'
#     aggset.loc[aggset['cutoff'] > maxval, 'agg'] = aggset[aggset.columns[0]]
#     aggset = aggset[[aggset.columns[0], 'agg']]
#     # aggset.drop(columns=['sum', 'cutoff'], inplace=True)
#     # aggset = aggset[aggset.columns[:2]].drop_duplicates()
#     if transpose:
#         aggset = aggset.transpose()
#     return aggset
#
#
# results = results[results['Ontdoener_in_AMA'] == True]
# results = results[results['RouteInzameling'] == 'N']
# # results = results[results['Ontdoener_AG'] != 'E']
# results = results[results['match'].isin(['1a', '2a', '2b', '3a', '3b', '4a', '4b'])]
#
# print(sum(results['Gewicht_KG']))
#
# results = results[['Gewicht_KG', 'Ontdoener_AG', 'VerwerkingsmethodeCode']]
# results['VerwerkingsmethodeCode'] = results['VerwerkingsmethodeCode'].str[0]+"."
# sectors = results.groupby(['Ontdoener_AG', 'VerwerkingsmethodeCode']).sum()
# sectors.reset_index(inplace=True)
#
# agg_sectors = cutoff(sectors[['Ontdoener_AG', 'Gewicht_KG']].groupby(['Ontdoener_AG']).sum(), 0.2, transpose=False)
# sectors = sectors.merge(agg_sectors, how='left', on='Ontdoener_AG')
#
# print(sum(sectors['Gewicht_KG']))
# sectors = sectors.groupby(['agg', 'VerwerkingsmethodeCode']).sum()
# sectors.reset_index(inplace=True)
#
# print(sum(sectors['Gewicht_KG']))
#
# sectors['sector_percent'] = sectors.groupby('agg')['Gewicht_KG'].transform('sum') * 100 / sectors['Gewicht_KG'].sum()
# sectors['process_percent'] = sectors.groupby('VerwerkingsmethodeCode')['Gewicht_KG'].transform('sum') * 100 / sectors['Gewicht_KG'].sum()
#
# sectors['sector'] = sectors['agg'] + ', ' + sectors['sector_percent'].round(1).astype(str) + '%'
# sectors['process'] = sectors['VerwerkingsmethodeCode'] + ', ' + sectors['process_percent'].round(1).astype(str) + '%'
#
# sankeyframe = sectors[['sector', 'process', 'Gewicht_KG']]
# sankeyframe.columns = ['source', 'target', 'amount']
# sankey.draw_sankey(sankeyframe)

# ######################## QUERY 3: MATERIALS #############################
# ------------------------------------------------------------------------------

# classes = pd.read_excel('Private_data/Classification/EURAL_classification_v1.8_EN.xlsx')
#
# classes['EURAL6'] = classes['EURAL6'].apply(lambda x: ''.join(x.split(' ')).strip('*'))
# results['EuralCode'] = results['EuralCode'].astype('str').str.zfill(6)
#
# amounts = results[['Gewicht_KG', 'EuralCode']]
# full_amount = amounts['Gewicht_KG'].sum()
#
# classified = amounts.merge(classes, left_on='EuralCode', right_on='EURAL6', validate='m:1')
# classified_amount = classified['Gewicht_KG'].sum()
#
# print(classified_amount / full_amount * 100, '% of all waste produced or treated in AMA got classified')
#
# sankeyframe = classified.groupby(['Purity', 'Cleanliness', 'Organic', 'Biotic']).sum()
# sankeyframe.reset_index(inplace=True)
#
# sankeyframe['purity_percent'] = sankeyframe.groupby('Purity')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
# sankeyframe['cleanliness_percent'] = sankeyframe.groupby('Cleanliness')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
# sankeyframe['organic_percent'] = sankeyframe.groupby('Organic')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
# sankeyframe['biotic_percent'] = sankeyframe.groupby('Biotic')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
#
# sankeyframe['Purity'] = sankeyframe['Purity'] + ', ' + sankeyframe['purity_percent'].round(1).astype(str) + '%'
# sankeyframe['Cleanliness'] = sankeyframe['Cleanliness'] + ', ' + sankeyframe['cleanliness_percent'].round(1).astype(str) + '%'
# sankeyframe['Organic'] = sankeyframe['Organic'] + ', ' + sankeyframe['organic_percent'].round(1).astype(str) + '%'
# sankeyframe['Biotic'] = sankeyframe['Biotic'] + ', ' + sankeyframe['biotic_percent'].round(1).astype(str) + '%'
#
# sankeyframe['amount'] = sankeyframe['Gewicht_KG']
#
# sankeyframe = sankeyframe[['Cleanliness', 'Purity', 'Organic', 'Biotic', 'amount']]
# sankeyframe.sort_values(['Purity', 'amount'], ascending=False, inplace=True)
#
# print(sankeyframe)
#
# sankey.draw_sankey(sankeyframe, scattered=True)

# #################### BENAMING AFVAL USEFUL PERCENTAGE ########################
# ------------------------------------------------------------------------------

eural = pd.read_excel('Private_data/Classification/EWC_table6dig.xlsx')

results['EuralCode'] = results['EuralCode'].astype('str').str.zfill(6)
eural['EuralCode'] = eural['EuralCode'].astype('str').str.zfill(6)

desc = results[['EuralCode', 'BenamingAfval']]

desc2 = desc.merge(eural, on='EuralCode')

diff = desc[(desc['EuralCode'].isin(desc2['EuralCode']) == False)]
print(diff)
