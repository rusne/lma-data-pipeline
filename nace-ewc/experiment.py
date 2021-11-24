"""
Copyright (C) 2021  Rusne Sileryte
Modified based on the original code under the same license available at https://github.com/rusne/geoFluxus
rusne.sileryte@gmail.com

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""

import pandas as pd
import functions

# # --------- DATA PREPROCESSING KVK ---------------------------------------------
#
# KvK_dataframe = pd.read_csv("Private_data/2018_KvK.csv", low_memory=False)
# KvK_companies = functions.prepare_KvK_data(KvK_dataframe)
#
# --------- DATA PREPROCESSING LMA ---------------------------------------------

LMA_dataframe = pd.read_excel('Private_data/LMA_data_AMA/ontvangstmeldingen_AMA_2018_cleaned.xlsx')
LMA_actors = functions.prepare_LMA_data(LMA_dataframe)

# intermediary output
LMA_actors.to_excel('Private_data/LMA_ontdoeners_AMA_2018_test.xlsx')
#
# # --------- SETS 1 & 2 ---------------------------------------------------------
#
# # intermediary input
# LMA_actors = pd.read_excel('Private_data/LMA_ontdoeners_AMA_2018.xlsx')
# # intermediary input --- SAMPLE
# # LMA_actors = pd.read_excel('Private_data/LMA_ontdoeners_AMA_2018_sample.xlsx')
# matched_1, remaining = functions.match_name_postcode(LMA_actors, KvK_companies)
#
# # --------- INDEXING: SEARCH SPACE ---------------------------------------------
# search_space = functions.make_search_space(remaining, KvK_companies)
#
# # intermediary output
# # search_space.to_excel('Private_data/search_space.xlsx')
#
# # --------- PAIRWISE COMPARISON & CLASSIFICATION -------------------------------
# # intermediary input
# # search_space = read_excel('Private_data/search_space.xlsx')
#
# matched_2 = functions.match_criteria(search_space)
#
# # --------- RESULTS ------------------------------------------------------------
# result = pd.concat([matched_1, matched_2])
# result = result[['LMA_key', 'LMA_origname', 'LMA_address', 'LMA_loc', 'LMA_eural',
#                  'KvK_origname', 'KvK_address', 'KvK_postcode', 'KvK_sbi',
#                  'KvK_ag', 'match', 'dist', 'ratio']]
#
# remaining = LMA_actors[(LMA_actors['LMA_key'].isin(result['LMA_key']) == False)]
# remaining = remaining[['LMA_key', 'LMA_origname', 'LMA_address',
#                        'LMA_loc', 'LMA_eural']]
# remaining.drop_duplicates(inplace=True)
#
#
# result = pd.concat([result, remaining])
#
# # output
# result.to_excel('Private_data/result.xlsx')
# # output --- SAMPLE
# result.to_excel('Private_data/sample/sample_result.xlsx')


# --------- REFINEMENT: NACE-EWC -----------------------------------------------

# results of the full dataset
# result = pd.read_excel('Private_data/result.xlsx')
# results --- SAMPLE
# result = pd.read_excel('Private_data/sample/sample_validation.xlsx')
#
# # print matching statistics
# functions.make_stats(result)
#
# # validated results full dataset
# # validated_result = result[result['match'].isin(['1a', '2a', '2b', '3a', '3b', '4a', '4b'])]
# # validated results --- SAMPLE
# validated_result = result[result['validity'] == 2]
#
# functions.validate(validated_result, level='AG')
# functions.validate(validated_result, level='2')
# functions.validate(validated_result, level='4')
