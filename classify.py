import logging
import pandas as pd
import variables as var


def run(dataframe):
    connect_nace = var.connect_nace

    # ------------------------------------------------------------------------------
    # NACE - TRANSITION AGENDA
    # ------------------------------------------------------------------------------

    logging.info(f"Add transition agenda based on NACE...")

    transition_agendas = pd.read_csv('Private_data/NACE_Transitieagendas_NL_v1.3.csv', low_memory=False)
    transition_agendas = transition_agendas[['Code', 'Transitieagenda_indicatief']]
    transition_agendas.columns = [f'{connect_nace}_NACE', 'Transitieagenda_NACE']

    dataframe = pd.merge(dataframe, transition_agendas, how='left', on=f'{connect_nace}_NACE').set_axis(dataframe.index)
    dataframe.loc[dataframe['Transitieagenda_NACE'].isnull(), 'Transitieagenda_NACE'] = "non-specifiek"

    # ------------------------------------------------------------------------------
    # EURAL CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------

    logging.info(f"Add transition agenda based on EWC code...")

    transition_agendas = pd.read_csv('Private_data/EURAL_classification_v1.6_NL.csv', low_memory=False)
    transition_agendas = transition_agendas[['Transitieagenda_indicatief', 'EURAL_6_cijfer_code']]
    transition_agendas.columns = ['Transitieagenda_EWC', 'EuralCode']
    transition_agendas['EuralCode'] = transition_agendas['EuralCode'].str.replace(' ', '').str.replace('*', '')

    dataframe.loc[dataframe["EuralCode"].notnull(), "EuralCode"] = dataframe["EuralCode"].astype('Int64').astype(str).str.zfill(6)
    dataframe = pd.merge(dataframe, transition_agendas, how='left', on='EuralCode').set_axis(dataframe.index)

    # ------------------------------------------------------------------------------
    # GN CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------
    if "GNcode" in dataframe.columns:
        logging.info(f"Add transition agenda based on GN code...")

        transition_agendas = pd.read_csv('Private_data/CN_Transitieagenda_v1.1.csv', low_memory=False)
        transition_agendas = transition_agendas[['Transitieagenda_NL', 'CN_LMA']]
        transition_agendas.columns = ['Transitieagenda_GNcode', 'GNcode']
        transition_agendas.dropna(subset=['GNcode'], inplace=True)
        transition_agendas['GNcode'] = transition_agendas['GNcode'].str.upper()

        dataframe.loc[dataframe["GNcode"].notnull(), "GNcode"] = dataframe["GNcode"].str.upper()
        dataframe = pd.merge(dataframe, transition_agendas, how='left', on='GNcode').set_axis(dataframe.index)

    return dataframe
