import logging
import pandas as pd


def run(dataframe):
    # ------------------------------------------------------------------------------
    # NACE - VALUE CHAIN
    # ------------------------------------------------------------------------------

    logging.info(f"Add value chain based on NACE...")

    # read NACE-EWC validation file
    value_chains = pd.read_csv('Private_data/NACE_valuechains.csv', low_memory=False)
    value_chains = value_chains[['Code', 'Value chain (Based on AG code)']]
    value_chains.columns = ['Ontdoener_NACE', 'Value_chain']

    indices = dataframe.index
    dataframe = pd.merge(dataframe, value_chains, how='left', on='Ontdoener_NACE')
    dataframe.index = indices

    # ------------------------------------------------------------------------------
    # EURAL CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------

    logging.info(f"Add chain position based on EWC code...")

    chain_positions = pd.read_csv('Private_data/EURAL_classification_v1.2_EN.csv', low_memory=False)
    chain_positions = chain_positions[['Chain position', 'Euralcode']]
    chain_positions.columns = ['Chain_position', 'EuralCode']
    chain_positions['EuralCode'] = chain_positions['EuralCode'].str.replace(' ', '').str.replace('*', '')

    indices = dataframe.index
    dataframe["EuralCode"] = dataframe["EuralCode"].astype(str).str.zfill(6)
    dataframe = pd.merge(dataframe, chain_positions, how='left', on='EuralCode')
    dataframe.index = indices

    return dataframe
