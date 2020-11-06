import logging
import pandas as pd


def validate(flows, nace_ewc, lvl=1):
    # NACE level on which EWC is applied
    digits = {
        1: 1,  # lvl1: A
        2: 4,  # lvl2: A-11
        3: 5,  # lvl3: A-112
        4: 6   # lvl4: A-1121
    }

    # format NACE to level
    flows['NACE'] = flows['NACE'].str[:digits[lvl]]
    nace_ewc = nace_ewc[nace_ewc['level'] == lvl]
    nace_ewc['NACE'] = nace_ewc['NACE'].str[:digits[lvl]]
    nace_ewc.drop_duplicates(inplace=True)

    # TEST 1: match flows with NACE-EWC validation only by NACE
    # if unmatched (no level), the NACE level is not included in NACE-EWC validation
    # for unmatched flows, do not proceed with validation
    nace = nace_ewc[['NACE', 'level']]
    nace.drop_duplicates(inplace=True)
    indices = flows.index  # keep original indices to restore after pd.merge
    match = pd.merge(flows, nace, how='left', on='NACE')
    match.index = indices  # restore original indices after merge
    match = match[match['level'].notnull()]
    flows = flows.loc[match.index]

    # TEST 2: match flows with NACE-EWC validation both on NACE & EWC codes
    # for matched flows in TEST 1, search in NACE-EWC validation
    # if unmatched (no level), the NACE-EWC is not included in NACE-EWC validation
    # remove unmatched flows as invalid from the original dataframe
    indices = flows.index
    match = pd.merge(flows, nace_ewc, how='left', on=['NACE', 'EWC_code'])
    match.index = indices
    return match[match['level'].isnull()].index


def run(dataframe):
    # ------------------------------------------------------------------------------
    # NACE - EWC VALIDATION
    # ------------------------------------------------------------------------------

    # read NACE-EWC validation file
    nace_ewc = pd.read_csv('Private_data/NACE-EWC.csv', low_memory=False)

    # clean EWC codes in NACE-EWC validation
    codes = ['EWC_2', 'EWC_4', 'EWC_6']
    nace_ewc[codes] = nace_ewc['EWC_code'].str.replace('*', '')\
                                          .str.strip()\
                                          .str.split(expand=True)
    for code in codes:
        nace_ewc[code] = nace_ewc[code].str.zfill(2)
    nace_ewc['EWC_code'] = nace_ewc[codes[0]].str.cat(nace_ewc[codes[1:]], sep="")
    nace_ewc.rename(columns={'NACE level on which EWC is applied': 'level'}, inplace=True)
    nace_ewc = nace_ewc[['NACE', 'level', 'EWC_code']]

    # validate flows with NACE-EWC on 4 NACE levels
    original_entries = len(dataframe.index)
    dataframe['EuralCode'] = dataframe['EuralCode'].astype(str).str.zfill(6)
    for lvl in range(1, 5):
        flows = dataframe[['Ontdoener_NACE', 'EuralCode']]
        flows.columns = ['NACE', 'EWC_code']
        dataframe.drop(validate(flows, nace_ewc, lvl), inplace=True)

    final_entries = len(dataframe.index)
    logging.warning(f"Remove {original_entries-final_entries} lines as invalid on NACE-EWC")

    # ------------------------------------------------------------------------------
    # NACE - VALUE CHAIN
    # ------------------------------------------------------------------------------

    logging.info(f"Add value chain based on NACE...")

    # read NACE-EWC validation file
    value_chains = pd.read_excel('Private_data/NACE_valuechains.xlsx')
    value_chains = value_chains[['Code', 'Value chain (Based on AG code)']]
    value_chains.columns = ['Ontdoener_NACE', 'Value_chain']

    indices = dataframe.index
    dataframe = pd.merge(dataframe, value_chains, how='left', on='Ontdoener_NACE')
    dataframe.index = indices

    # ------------------------------------------------------------------------------
    # EURAL CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------

    logging.info(f"Add chain position based on EWC code...")

    chain_positions = pd.read_excel('Private_data/EURAL_classification_v1.2_EN.xlsx', sheet_name='Categories')
    chain_positions = chain_positions[['Chain position', 'Euralcode']]
    chain_positions.columns = ['Chain_position', 'EuralCode']
    chain_positions['EuralCode'] = chain_positions['EuralCode'].str.replace(' ', '').str.replace('*', '')

    indices = dataframe.index
    dataframe = pd.merge(dataframe, chain_positions, how='left', on='EuralCode')
    dataframe.index = indices

    return dataframe
