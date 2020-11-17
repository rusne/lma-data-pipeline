import logging
import pandas as pd
import variables as var


def run(dataframe):
    connect_nace = var.connect_nace

    # ------------------------------------------------------------------------------
    # NACE - VALUE CHAIN
    # ------------------------------------------------------------------------------

    logging.info(f"Add value chain based on NACE...")

    # read NACE-EWC validation file
    value_chains = pd.read_csv('Private_data/NACE_valuechains.csv', low_memory=False)
    value_chains = value_chains[['Code', 'Value chain (Based on AG code)']]
    value_chains.columns = [f'{connect_nace}_NACE', 'Value_chain']

    dataframe = pd.merge(dataframe, value_chains, how='left', on=f'{connect_nace}_NACE').set_axis(dataframe.index)

    # ------------------------------------------------------------------------------
    # EURAL CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------

    logging.info(f"Add chain position based on EWC code...")

    chain_positions = pd.read_csv('Private_data/EURAL_classification_v1.2_EN.csv', low_memory=False)
    chain_positions = chain_positions[['Chain position', 'Euralcode']]
    chain_positions.columns = ['Chain_position', 'EuralCode']
    chain_positions['EuralCode'] = chain_positions['EuralCode'].str.replace(' ', '').str.replace('*', '')

    dataframe["EuralCode"] = dataframe["EuralCode"].astype(str).str.zfill(6)
    dataframe = pd.merge(dataframe, chain_positions, how='left', on='EuralCode').set_axis(dataframe.index)

    return dataframe
