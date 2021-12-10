
import pandas as pd
import geopandas as gpd
import json
from shapely import wkt
import sankey
from fuzzywuzzy import fuzz


# ######################## QUERY 1: SPATIAL EXTENT #############################
# ------------------------------------------------------------------------------


def point2latlon(point):

    x, lon, lat = point.split(' ')
    lon = lon.strip('(')
    lat = lat.strip(')')

    return {"lon": float(lon), "lat": float(lat)}


def anonymise(point):

    x, lon, lat = point.split(' ')
    lon = lon.strip('(')
    lat = lat.strip(')')
    lon = lon[:4]
    lat = lat[:5]

    return(f'{x} ({lon} {lat})')


# results = pd.read_csv('Private_data/results/AMA_2018/ontvangstmeldingen_AMA_2018_result.csv')
ontvangst = pd.read_csv('Private_data/ontvangstmeldingen_AMA_2018_result.csv')
# afgifte = pd.read_csv('Private_data/afgiftemeldingen_AMA_2018_result.csv')

# filter secondary waste
ontvangst['EuralCode'] = ontvangst['EuralCode'].astype('str').str.zfill(6)
results = ontvangst[ontvangst['EuralCode'].str[:2] != '19']

# convert wkt to geometry
results['Herkomst_Location'] = results['Herkomst_Location'].apply(wkt.loads)
results = gpd.GeoDataFrame(results, geometry="Herkomst_Location", crs={"init": "epsg:4326"})

# load Amsterdam municipality and AMA boundaries
try:
    boundary_AMS = gpd.read_file("Spatial_data/Amsterdam_gemeente_WGS84.shp")
    boundary_AMA = gpd.read_file("Spatial_data/Metropoolregio_WGS84.shp")
except Exception as error:
    logging.critical(error)
    raise

# PRODUCED IN AMS
joined = gpd.sjoin(results, boundary_AMS, how="left", op="within")
in_boundary = joined[joined["GM_NAAM"].isna() == False].index
out_boundary = joined[joined["GM_NAAM"].isna()].index
results.loc[in_boundary, "produced_in_AMS"] = True
results.loc[out_boundary, "produced_in_AMS"] = False

produced_in_AMS = results[results["produced_in_AMS"] == True]

# convert treatment locations into geometries

produced_in_AMS['Verwerker_Location'] = produced_in_AMS['Verwerker_Location'].apply(wkt.loads)
produced_in_AMS = gpd.GeoDataFrame(produced_in_AMS, geometry="Verwerker_Location", crs={"init": "epsg:4326"})

# TREATED IN AMS
joined = gpd.sjoin(produced_in_AMS, boundary_AMS, how="left", op="within")
in_boundary = joined[joined["GM_NAAM"].isna() == False].index
out_boundary = joined[joined["GM_NAAM"].isna()].index
produced_in_AMS.loc[in_boundary, "treated_in_AMS"] = True
produced_in_AMS.loc[out_boundary, "treated_in_AMS"] = False

# TREATED IN AMA
joined = gpd.sjoin(produced_in_AMS, boundary_AMA, how="left", op="within")
in_boundary = joined[joined["RG_NAAM"].isna() == False].index
out_boundary = joined[joined["RG_NAAM"].isna()].index
produced_in_AMS.loc[in_boundary, "treated_in_AMA"] = True
produced_in_AMS.loc[out_boundary, "treated_in_AMA"] = False

produced_in_AMS.to_excel('temp.xlsx')
produced_in_AMS['Herkomst_Location'] = produced_in_AMS['Herkomst_Location'].apply(wkt.dumps)
produced_in_AMS['Verwerker_Location'] = produced_in_AMS['Verwerker_Location'].apply(wkt.dumps)

map_cols = ['Herkomst_Location', 'Verwerker_Location', 'Gewicht_KG']

treated_in_AMS = produced_in_AMS[produced_in_AMS["treated_in_AMS"] == True]
treated_in_AMS = treated_in_AMS[map_cols]
treated_in_AMA = produced_in_AMS[(produced_in_AMS["treated_in_AMS"] == False) & produced_in_AMS["treated_in_AMA"] == True]
treated_in_AMA = treated_in_AMA[map_cols]
treated_in_NL = produced_in_AMS[produced_in_AMS["treated_in_AMA"] == False]
treated_in_NL = treated_in_NL[map_cols]

print(sum(produced_in_AMS['Gewicht_KG']) / 1000)
print(sum(treated_in_AMS['Gewicht_KG']) / 1000 + sum(treated_in_AMA['Gewicht_KG']) / 1000 + sum(treated_in_NL['Gewicht_KG']) / 1000)
print(sum(treated_in_AMS['Gewicht_KG']) / 1000)
print(sum(treated_in_AMA['Gewicht_KG']) / 1000)
print(sum(treated_in_NL['Gewicht_KG']) / 1000)

# ##### MAKE A MAP ######

treated_in_AMS = treated_in_AMS.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
treated_in_AMS.reset_index(inplace=True)
treated_in_AMS['tag'] = 'Locally treated waste'
treated_in_AMS['colour'] = 'rgb(156,9,67)'

treated_in_AMA = treated_in_AMA.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
treated_in_AMA.reset_index(inplace=True)
treated_in_AMA['tag'] = 'Waste treated in the metropolitan region'
treated_in_AMA['colour'] = 'rgb(106,193,166)'

treated_in_NL = treated_in_NL.groupby(['Herkomst_Location', 'Verwerker_Location']).sum()
treated_in_NL.reset_index(inplace=True)
treated_in_NL['tag'] = 'Waste treated outside of the metropolitan region'
treated_in_NL['colour'] = 'rgb(94,82,160)'

map = pd.concat([treated_in_NL, treated_in_AMA, treated_in_AMS])

# print(map)
# anonymise location
map['Herkomst_Location'] = map['Herkomst_Location'].apply(anonymise)
map['Verwerker_Location'] = map['Verwerker_Location'].apply(anonymise)

map.to_excel('results/query1.xlsx')

# prepare json file for visualisation

map['source'] = map['Herkomst_Location'].apply(point2latlon)
map['target'] = map['Verwerker_Location'].apply(point2latlon)
map['amount'] = map['Gewicht_KG'] / 1000

map_json = map[['source', 'target', 'amount', 'tag']].to_json('results/query1.json', orient="records")

######################## QUERY 2: ECONOMIC SECTORS #############################
------------------------------------------------------------------------------


def cutoff(aggset, factor, transpose=True):
    # takes a dataframe, sorts and aggregates small values into the "other" field
    # factor tells how big the "other" can be
    # in comparison to the biggest value of the set

    if transpose:
        aggset = aggset.transpose()
    aggset['sum'] = aggset.sum(axis=1)
    aggset.sort_values(by='sum', inplace=True)
    total = 0
    maxval = list(aggset['sum'])[-1] * factor
    # # print(maxval)
    for index, row in aggset.iterrows():
        total += row['sum']
        aggset.loc[index, 'cutoff'] = total

    aggset.reset_index(inplace=True)
    # index_col = aggset.columns[0]
    aggset.loc[aggset['cutoff'] < maxval, 'agg'] = 'all other'
    aggset.loc[aggset['cutoff'] > maxval, 'agg'] = aggset[aggset.columns[0]]
    aggset = aggset[[aggset.columns[0], 'agg']]
    # aggset.drop(columns=['sum', 'cutoff'], inplace=True)
    # aggset = aggset[aggset.columns[:2]].drop_duplicates()
    if transpose:
        aggset = aggset.transpose()
    return aggset


results = ontvangst[ontvangst['Ontdoener_in_AMA'] == True]
results = results[results['RouteInzameling'] == 'N']
# results = results[results['Ontdoener_AG'] != 'E']
results = results[results['match'].isin(['1a', '2a', '2b', '3a', '3b', '4a', '4b'])]

print(sum(results['Gewicht_KG']))

results = results[['Gewicht_KG', 'Ontdoener_AG', 'VerwerkingsmethodeCode']]
results['VerwerkingsmethodeCode'] = results['VerwerkingsmethodeCode'].str[0]+"."
sectors = results.groupby(['Ontdoener_AG', 'VerwerkingsmethodeCode']).sum()
sectors.reset_index(inplace=True)

agg_sectors = cutoff(sectors[['Ontdoener_AG', 'Gewicht_KG']].groupby(['Ontdoener_AG']).sum(), 0.2, transpose=False)
sectors = sectors.merge(agg_sectors, how='left', on='Ontdoener_AG')

print(sum(sectors['Gewicht_KG']))
sectors = sectors.groupby(['agg', 'VerwerkingsmethodeCode']).sum()
sectors.reset_index(inplace=True)

print(sum(sectors['Gewicht_KG']))

sectors['sector_percent'] = sectors.groupby('agg')['Gewicht_KG'].transform('sum') * 100 / sectors['Gewicht_KG'].sum()
sectors['process_percent'] = sectors.groupby('VerwerkingsmethodeCode')['Gewicht_KG'].transform('sum') * 100 / sectors['Gewicht_KG'].sum()

sectors['sector'] = sectors['agg'] + ', ' + sectors['sector_percent'].round(1).astype(str) + '%'
sectors['process'] = sectors['VerwerkingsmethodeCode'] + ', ' + sectors['process_percent'].round(1).astype(str) + '%'

sankeyframe = sectors[['sector', 'process', 'Gewicht_KG']]
sankeyframe.columns = ['source', 'target', 'amount']
sankeyframe['amount'] = sankeyframe['amount'] / 1000
sankey.draw_sankey(sankeyframe)

sankeyframe.to_excel('results/query2.xlsx')

######################## QUERY 3: MATERIALS #############################
------------------------------------------------------------------------------

classes = pd.read_excel('Private_data/Classification/EURAL_classification_v1.8_EN.xlsx')

classes['EURAL6'] = classes['EURAL6'].apply(lambda x: ''.join(x.split(' ')).strip('*'))
ontvangst['EuralCode'] = ontvangst['EuralCode'].astype('str').str.zfill(6)

amounts = ontvangst[['Gewicht_KG', 'EuralCode']]
full_amount = amounts['Gewicht_KG'].sum()

classified = amounts.merge(classes, left_on='EuralCode', right_on='EURAL6', validate='m:1')
classified_amount = classified['Gewicht_KG'].sum()

print(classified_amount / full_amount * 100, '% of all waste produced or treated in AMA got classified, ', classified_amount)

materials = classified.groupby(['Materiaal_NL']).sum()
print(materials.sort_values('Gewicht_KG'))

sankeyframe = classified.groupby(['Purity', 'Cleanliness', 'Organic', 'Biotic']).sum()
sankeyframe.reset_index(inplace=True)

sankeyframe['purity_percent'] = sankeyframe.groupby('Purity')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
sankeyframe['cleanliness_percent'] = sankeyframe.groupby('Cleanliness')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
sankeyframe['organic_percent'] = sankeyframe.groupby('Organic')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()
sankeyframe['biotic_percent'] = sankeyframe.groupby('Biotic')['Gewicht_KG'].transform('sum') * 100 / sankeyframe['Gewicht_KG'].sum()

sankeyframe['Purity'] = sankeyframe['Purity'] + ', ' + sankeyframe['purity_percent'].round(1).astype(str) + '%'
sankeyframe['Cleanliness'] = sankeyframe['Cleanliness'] + ', ' + sankeyframe['cleanliness_percent'].round(1).astype(str) + '%'
sankeyframe['Organic'] = sankeyframe['Organic'] + ', ' + sankeyframe['organic_percent'].round(1).astype(str) + '%'
sankeyframe['Biotic'] = sankeyframe['Biotic'] + ', ' + sankeyframe['biotic_percent'].round(1).astype(str) + '%'

sankeyframe['amount'] = sankeyframe['Gewicht_KG']

sankeyframe = sankeyframe[['Cleanliness', 'Purity', 'Organic', 'Biotic', 'amount']]
sankeyframe.sort_values(['Purity', 'amount'], ascending=False, inplace=True)

sankeyframe['amount'] = sankeyframe['amount'] / 1000
print(sankeyframe)

sankey.draw_sankey(sankeyframe, scattered=True)


#
sankeyframe.to_excel('results/query3.xlsx')

#################### BENAMING AFVAL USEFUL PERCENTAGE ########################
------------------------------------------------------------------------------

eural = pd.read_excel('Private_data/Classification/EWC_table6dig.xlsx')

results['EuralCode'] = results['EuralCode'].astype('str').str.zfill(6)
eural['EuralCode'] = eural['EuralCode'].astype('str').str.zfill(6)

desc = results[['EuralCode', 'BenamingAfval']]
desc = desc.merge(eural, on='EuralCode')
desc["similarity ratio"] = desc.apply(lambda x: fuzz.token_sort_ratio(x.BenamingAfval, x.EuralName), axis=1, result_type="reduce")

# desc.to_excel('Private_data/descriptions.xlsx')
total = len(desc.index)
same = len(desc[desc["similarity ratio"] >= 75].index)

print(same / total * 100)

#################### MASS BALANCE STATS ########################
------------------------------------------------------------------------------

ontvangst['EuralCode'] = ontvangst['EuralCode'].astype('str').str.zfill(6)

print(ontvangst['Gewicht_KG'].sum() / 1000)
primary = ontvangst[ontvangst['EuralCode'].str[:2] != '19']
secondary = ontvangst[ontvangst['EuralCode'].str[:2] == '19']

print(primary['Gewicht_KG'].sum() / ontvangst['Gewicht_KG'].sum() * 100)
print(secondary['Gewicht_KG'].sum() / ontvangst['Gewicht_KG'].sum() * 100)

print('Primary: ', primary['Gewicht_KG'].sum() / 1000)
print('Secondary: ', secondary['Gewicht_KG'].sum() / 1000)

print(afgifte['Gewicht_KG'].sum() / 1000)

#################### FOREIGN FLOWS ########################
------------------------------------------------------------------------------

# print(ontvangst.columns)
total = ontvangst['Gewicht_KG'].sum() + afgifte['Gewicht_KG'].sum()

foreign_ontvangst = ontvangst[ontvangst['Ontdoener_Land'] != 'NEDERLAND']
foreign_ontvangst = foreign_ontvangst['Gewicht_KG'].sum()
foreign_afgifte = afgifte[afgifte['EerstAfnemer_Land'] != 'NEDERLAND']
foreign_afgifte = foreign_afgifte['Gewicht_KG'].sum()

print(foreign_ontvangst)
print(foreign_afgifte)
print(foreign_ontvangst / total * 100)
print(foreign_afgifte / total * 100)
