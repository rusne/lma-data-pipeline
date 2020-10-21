# Find the right economic activity of a company --> now works on
# Match company with KvK data (by name and location)
# Validate NACE with EWC code
# Recognize waste management companies
# process KvK dataset (cache things like geolocation, and update with new data)

import logging
import pandas as pd
import geopandas as gpd
from shapely import wkt
# import variables as var
# from fuzzywuzzy import fuzz

import warnings  # ignore unnecessary warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


def run(dataframe):
    # extract ontdoeners from LMA dataset to connect nace
    logging.info('Extract ontdoeners...')
    ontdoener_columns = [col for col in dataframe.columns if 'Ontdoener' in col]
    ontdoeners = dataframe[ontdoener_columns]
    logging.info(f'Original ontdoeners: {len(ontdoeners.index)}')

    # prepare key to match with KvK dataset (clean name + postcode)
    ontdoeners['key'] = ontdoeners['Ontdoener'].str.cat(ontdoeners[['Ontdoener_Postcode']], sep=' ')

    # import KvK dataset with NACE codes
    logging.info('Import KvK dataset with geolocation...')
    try:
        # KvK_actors = pd.read_excel("Private_data/KvK_data/raw_data/all_LISA_part2.xlsx")
        KvK_actors = pd.read_csv("Private_data/KvK_data/raw_data/all_LISA_part2.csv", low_memory=False)
    except Exception as error:
        logging.critical(error)
        raise

    # load casestudy boundary
    logging.info('Import casestudy boundary...')
    try:
        MRA_boundary = gpd.read_file('Spatial_data/Metropoolregio_RDnew.shp')
    except Exception as error:
        logging.critical(error)
        raise

    # first ontdoeners need to be matched with their own locations
    # taking out route actors first
    ontdoeners.loc[ontdoeners['key'].str.contains('route'), 'route'] = 'J'
    ontdoeners.loc[ontdoeners['route'] == 'J', 'key'] = ontdoeners['key'].apply(lambda x: str(x).strip(' route'))

    # remove any ontdoeners with no location
    missing_locations = ontdoeners[ontdoeners['Ontdoener_Location'].isnull()]
    if len(missing_locations.index):
        logging.warning(f'Remove {len(missing_locations.index)} ontdoeners with missing locations...')
        ontdoeners.dropna(subset=['Ontdoener_Location'], inplace=True)

    # after filtering missing locations, add route info again
    ontdoeners.loc[ontdoeners['route'] == 'J', 'key'] = ontdoeners['key'] + ' route'
    # logging.info(f'{ontdoeners["key"].nunique()} ontdoeners to connect NACE...')

    # convert WKT to geometry
    ontdoeners['Ontdoener_Location'] = ontdoeners['Ontdoener_Location'].apply(wkt.loads)
    LMAgdf = gpd.GeoDataFrame(ontdoeners, geometry='Ontdoener_Location', crs={'init': 'epsg:28992'})

    # check which ontdoeners are within the casestudy area
    joined = gpd.sjoin(LMAgdf, MRA_boundary, how='left', op="within")
    in_boundary = joined[joined['OBJECTID'].isna() == False]
    out_boundary = joined[joined['OBJECTID'].isna()]

    # logging.info(f'{in_boundary["key"].nunique()} ontdoeners are inside the casestudy area')
    if len(out_boundary.index):
        logging.info(f'Remove {out_boundary["key"].nunique()} ontdoeners outside the casestudy area')

    # further matching only happens for the actors inside the boundary
    LMA_inbound = in_boundary[ontdoeners.columns]

    # route inzameling gets a separate nace code as well
    route = LMA_inbound[LMA_inbound['route'] == 'J']
    if len(route.index):
        logging.info(f'Remove {route["key"].nunique()} ontdoeners belonging to route inzameling')

    LMA_inbound = LMA_inbound[LMA_inbound['route'] != 'J']
    LMA_inbound.drop(columns=['route'])

    total_inbound = LMA_inbound['key'].nunique()

    # ______________________________________________________________________________
    # 1. BY NAME AND ADDRESS
    #    both name and address are the same
    # ______________________________________________________________________________

    LMA_inbound1 = LMA_inbound[['key', 'Ontdoener_Origname', 'Ontdoener_Adres']].copy()
    LMA_inbound1.drop_duplicates(subset=['key'], inplace=True)

    by_name_and_address = pd.merge(LMA_inbound1, KvK_actors, on='key')

    # matching control output
    control_output = by_name_and_address[['key', 'Ontdoener_Origname', 'Ontdoener_Adres', 'orig_zaaknaam', 'adres', 'activenq', 'AG']]
    control_output['match'] = 1

    # OUTPUT BY NAME AND ADDRESS
    output_by_name_address = by_name_and_address[['key', 'activenq', 'Ontdoener_Origname']].copy()
    output_by_name_address['how'] = 'by name and address'

    perc = round(len(output_by_name_address.index) / float(total_inbound) * 100, 2)
    logging.info(f'{len(output_by_name_address.index)} actors matched by name & postcode ({perc}%)')

    # take out those actors that had not been matched
    remaining = LMA_inbound[(LMA_inbound['key'].isin(output_by_name_address['key']) == False)]

    # ______________________________________________________________________________
    # 2. BY NAME ONLY
    #    geographically closer one gets a priority
    # ______________________________________________________________________________

    LMA_inbound2 = remaining[['key', 'Ontdoener', 'Ontdoener_Origname', 'Ontdoener_Adres', 'Ontdoener_Location']].copy()
    LMA_inbound2.drop_duplicates(subset=['key'], inplace=True)

    by_name = pd.merge(LMA_inbound2, KvK_actors, left_on='Ontdoener', right_on='zaaknaam')
    by_name['wkt'] = by_name['wkt'].apply(wkt.loads)
    print(by_name.dtypes)
    by_name['dist'] = by_name.apply(lambda x: x['wkt'].distance(x['Ontdoener_Location']), axis=1)
    # closest = by_name.loc[by_name.groupby(['key'])['dist'].idxmin()]
    # print(closest)
#
# # matching control output
# control_output_2 = closest[['Key', 'Orig_name', 'Adres', 'orig_zaaknaam', 'adres', 'activenq', 'AGcode']]
# control_output_2['match'] = 2
# control_output = control_output.append(control_output_2)
#
# # OUTPUT BY NAME
# output_by_name = closest[['Key', 'activenq', 'Orig_name']].copy()
# output_by_name['how'] = 'by name'
#
# print(len(output_by_name.index), 'actors have been matched by name',)
# print(round(len(output_by_name.index) / float(total_inbound) * 100, 2), '%')
#
# # take out those actors that had not been matched
# remaining = remaining[(remaining['Key'].isin(output_by_name['Key']) == False)]
#
# print(remaining['Key'].nunique(), 'remaining')
#
# # ______________________________________________________________________________
# # 3. BY ADDRESS ONLY
# #    correct year gives priority in matching
# # ______________________________________________________________________________
#
# LMA_inbound3 = remaining[['Key', 'Orig_name', 'Adres', 'Jaar', 'Postcode']].copy()
# LMA_inbound3.drop_duplicates(subset=['Key'], inplace=True)
#
# by_address = pd.merge(LMA_inbound3, LISA_actors, left_on=['Adres', 'Postcode'], right_on=['adres', 'postcode'])
#
# # find those that got matched to only one NACE group
# by_address['count'] = by_address.groupby(['Key'])['AGcode'].transform('count')
# matched_by_address = by_address[by_address['count'] == 1]
#
# print(matched_by_address['Key'].nunique(), 'actors have been matched only by address',)
#
# ambiguous = by_address[by_address['count'] > 1]
#
# # give priority by year if possible, otherwise discard the matching
# temp = pd.DataFrame(columns=ambiguous.columns)
# for year in var.map_years:
#     col = 'in{0}'.format(year)
#     m = ambiguous[(ambiguous['Jaar'] == year) & (ambiguous[col].astype(str) == 'JA')]
#     temp.append(m)
#
# ambiguous['count'] = ambiguous.groupby(['Key'])['AGcode'].transform('count')
# matched_ambiguous = ambiguous[ambiguous['count'] == 1]
#
# print(matched_ambiguous['Key'].nunique(), 'additional actors have been matched by address and year')
#
# discard = ambiguous[ambiguous['count'] > 1]
# print(discard['Key'].nunique(), 'matches have been discarded due to multiple NACE codes')
#
# by_address = pd.concat([matched_by_address, matched_ambiguous])
#
# # matching control output
# control_output_3 = by_address[['Key', 'Orig_name', 'Adres', 'orig_zaaknaam', 'adres', 'activenq', 'AGcode']]
# control_output_3['match'] = 3
# control_output = control_output.append(control_output_3)
#
# by_address = by_address[['Key', 'activenq', 'Orig_name']]
# by_address.drop_duplicates(subset=['Key'], inplace=True)
#
# # OUTPUT BY ADDRESS
# output_by_address = by_address[['Key', 'activenq', 'Orig_name']]
# output_by_address['how'] = 'by address'
#
# print(len(output_by_address.index), 'actors have been matched only by address',)
# print(round(len(output_by_address.index) / float(total_inbound) * 100, 2), '%')
#
# # take out those actors that had not been matched
# remaining = remaining[(remaining['Key'].isin(output_by_address['Key']) == False)]
#
# print(remaining['Key'].nunique(), 'remaining')
# #
# # remaining.to_excel('remaining.xlsx')
#
# # ______________________________________________________________________________
# # 4. BY  GEO AND TEXT PROXIMITY
# #    closest name within a certain radius
# # ______________________________________________________________________________
#
# # remaining = pd.read_excel('remaining.xlsx')
# # remaining['WKT'] = remaining['WKT'].apply(wkt.loads)
# remaining_geo = gpd.GeoDataFrame(remaining, geometry='WKT', crs={'init': 'epsg:28992'})
# # LISA_actors = pd.read_excel(priv_folder + 'LISA_data/all_LISA_part2.xlsx',
# #                             nrows=50000)
# #
# LISA_actors['wkt'] = LISA_actors['wkt'].apply(wkt.loads)
# LISA_actors_geo = gpd.GeoDataFrame(LISA_actors[['key', 'orig_zaaknaam', 'adres', 'activenq', 'AGcode', 'wkt']], geometry='wkt', crs={'init': 'epsg:28992'})
#
# LMA_inbound4 = remaining_geo[['Key', 'Orig_name', 'Adres', 'WKT']]
# LMA_inbound4.drop_duplicates(subset=['Key'], inplace=True)
# LMA_inbound4['buffer'] = LMA_inbound4['WKT'].buffer(var.buffer_dist)
# buffers = gpd.GeoDataFrame(LMA_inbound4[['Key', 'buffer']], geometry='buffer', crs={'init': 'epsg:28992'})
# #
# # LISA_actors_geo.to_file('LISA.shp')
# # buffers.to_file('LMA.shp')
#
# # LISA_actors_geo = gpd.read_file('LISA.shp')
# # buffers = gpd.read_file('LMA.shp')
#
# contains = gpd.sjoin(buffers, LISA_actors_geo, how='inner', op='intersects')
#
# distances = pd.merge(contains, LISA_actors_geo[['wkt']], left_on='index_right', right_index=True)
# distances = pd.merge(distances, LMA_inbound4[['Orig_name', 'Adres', 'WKT']], left_index=True, right_index=True)
# distances['orig_zaaknaam'] = distances['orig_zaaknaam'].apply(lambda x: str(x))
# distances['Orig_name'] = distances['Orig_name'].apply(lambda x: str(x))
# distances['orig_zaaknaam'] = distances['orig_zaaknaam'].apply(lambda x: x.encode('ascii', 'ignore'))
# distances['Orig_name'] = distances['Orig_name'].apply(lambda x: x.encode('ascii', 'ignore'))
#
# distances['dist'] = distances.apply(lambda x: x['wkt'].distance(x['WKT']), axis=1)
# distances['text_dist'] = distances.apply(lambda x: fuzz.ratio(str(x['orig_zaaknaam']), str(x['Orig_name'])), axis=1)
#
# distances.reset_index(inplace=True)
# text_distances = distances[distances['text_dist'] >= 50]
# matched_text = text_distances.loc[text_distances.groupby(['Key'])['text_dist'].idxmax()]
#
# # matching control output
# control_output_4 = matched_text[['Key', 'Orig_name', 'Adres', 'orig_zaaknaam', 'adres', 'activenq', 'AGcode']]
# control_output_4['match'] = 4
# control_output = control_output.append(control_output_4)
#
# matched_by_text_proximity = matched_text[['Key', 'activenq', 'Orig_name']].drop_duplicates(subset=['Key'])
#
# # OUTPUT BY TEXT PROXIMITY
# output_by_text_proximity = matched_by_text_proximity.copy()
# output_by_text_proximity['how'] = 'by text proximity'
#
# print(len(output_by_text_proximity.index), 'actors have been matched by the closest name match in the area (<{0}m)'.format(var.buffer_dist),)
# print(round(len(output_by_text_proximity.index) / float(total_inbound) * 100, 2), '%')
#
# # take out those actors that had not been matched
# remaining = remaining[(remaining['Key'].isin(output_by_text_proximity['Key']) == False)]
#
# print(remaining['Key'].nunique(), 'remaining unmatched')
#
# # ______________________________________________________________________________
# # 5. BY GEO PROXIMITY
# # ______________________________________________________________________________
#
# remaining_dist = pd.merge(remaining[['Key']], distances, on='Key')
#
# matched_by_geo_proximity = remaining_dist.loc[remaining_dist.groupby(['Key'])['dist'].idxmin()]
#
# # matching control output
# control_output_5 = matched_by_geo_proximity[['Key', 'Orig_name', 'Adres', 'orig_zaaknaam', 'adres', 'activenq', 'AGcode']]
# control_output_5['match'] = 5
# control_output = control_output.append(control_output_5)
#
# matched_by_geo_proximity = matched_by_geo_proximity[['Key', 'activenq', 'Orig_name']].drop_duplicates(subset=['Key'])
#
# # OUTPUT BY PROXIMITY
# output_by_geo_proximity = matched_by_geo_proximity.copy()
# output_by_geo_proximity['how'] = 'by geo proximity'
#
# print(len(output_by_geo_proximity.index), 'actors have been matched by proximity',)
# print(round(len(output_by_geo_proximity.index) / float(total_inbound) * 100, 2), '%')
#
# # take out those actors that had not been matched
# remaining = remaining[(remaining['Key'].isin(output_by_geo_proximity['Key']) == False)]
#
# print(remaining['Key'].nunique(), 'remaining unmatched')
#
# control_output.to_excel(priv_folder + EXPORT + 'control_output.xlsx')
#
# # ______________________________________________________________________________
# # 5. UNMATCHED
# #    not matched with anything, gets a dummy NACE code
# #    points outside the LISA boundary also get a dummy code
# # ______________________________________________________________________________
#
# remaining['activenq'] = '0000'
# out_boundary['activenq'] = '0001'
# route['activenq'] = '0002'
#
# output_unmatched = pd.concat([remaining[['Key', 'activenq', 'Orig_name']], out_boundary[['Key', 'activenq', 'Orig_name']], route[['Key', 'activenq', 'Orig_name']]])
# output_unmatched.drop_duplicates(subset=['Key'], inplace=True)
# output_unmatched['how'] = 'unmatched'
#
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# # G I V I N G   N A C E   T O  V E R W E R K E R
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# # print('Loading LMA verwerkers.......')
# # verwerkers = pd.read_excel(priv_folder + PART1 + 'Export_LMA_verwerker.xlsx')
# #
# # verwerkers = verwerkers.drop_duplicates(subset=['Key'])
# # print(len(verwerkers), 'verwekers have been found')
# #
# # verwerkers['activenq'] = verwerkers['Key'].apply(lambda x: x.split()[-1])
# #
# # output_verwerker = verwerkers[['Key', 'activenq', 'Orig_name']]
# # output_verwerker['how'] = 'verwerker'
#
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# #  C O N C A T E N A T E   O U T P U T S
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# all_nace = pd.concat([output_by_name_address, output_by_name, output_by_address,
#                      output_by_text_proximity, output_by_geo_proximity, output_unmatched])  # , output_verwerker])
#
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# # G I V I N G   N A C E   T O   A L L   O T H E R   R O L E S
# # ______________________________________________________________________________
# # ______________________________________________________________________________
#
# role_map = {'Ontdoener': '0000',
#             'Afzender': '3810',
#             'Inzamelaar': '3810',
#             'Bemiddelaar': '3810',
#             'Handelaar': '3810',
#             'Ontvanger': '3820',
#             'Verwerker': '3820'}
#
# var.map_roles.remove('Ontdoener')
# var.map_roles.remove('Herkomst')
#
# for role in var.map_roles:
#
#     print('Loading LMA {0}s.......'.format(role))
#     LMA_role = pd.read_excel(priv_folder + PART1 + 'Export_LMA_{0}.xlsx'.format(role))
#
#     keys = LMA_role[['Key', 'Orig_name']].copy()
#     keys.drop_duplicates(subset=['Key'], inplace=True)
#     print(len(keys), '{0}s have been found'.format(role))
#
#     keys['activenq'] = role_map[role]
#
#     output_role = keys.copy()
#     output_role['how'] = role
#     all_nace = all_nace.append(output_role)
#
# print(len(all_nace))
# print(all_nace['Key'].nunique())
