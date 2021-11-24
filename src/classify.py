"""
Copyright (C) 2020  Rusne Sileryte
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

import logging
import pandas as pd
import variables as var
import numpy as np


def run(dataframe):
    connect_nace = var.connect_nace

    # # ------------------------------------------------------------------------------
    # # NACE - TRANSITION AGENDA
    # # ------------------------------------------------------------------------------
    #
    # logging.info(f"Add transition agenda based on NACE...")
    #
    # transition_agendas = pd.read_csv('Private_data/NACE_Transitieagendas_NL_v1.3.csv', low_memory=False)
    # transition_agendas.dropna(how='all', axis='columns', inplace=True)
    # transition_agendas.columns = [f'NACE_{col}' for col in transition_agendas.columns]
    #
    # dataframe = pd.merge(dataframe, transition_agendas, how='left',
    #                      left_on=f'{connect_nace}_NACE', right_on='NACE_Code').set_axis(dataframe.index)
    #
    # dataframe.loc[dataframe['NACE_Transitieagenda_indicatief'].isnull(), 'NACE_Transitieagenda_indicatief'] = "non-specifiek"

    # ------------------------------------------------------------------------------
    # EURAL CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------

    logging.info(f"Add transition agenda based on EWC code...")

    transition_agendas = pd.read_csv('Private_data/Classification/EURAL_classification_v1.5_NL.csv', low_memory=False)
    transition_agendas.dropna(how='all', axis='columns', inplace=True)
    transition_agendas.columns = [f'EURAL_{col}' for col in transition_agendas.columns]
    transition_agendas['EURAL_EURAL_6_cijfer_code'] = transition_agendas['EURAL_EURAL_6_cijfer_code'].str.replace(' ', '').str.replace('*', '')

    dataframe.loc[dataframe["EuralCode"].notnull(), "EuralCode"] = dataframe["EuralCode"].astype('Int64').astype(str).str.zfill(6)
    dataframe = pd.merge(dataframe, transition_agendas, how='left',
                         left_on='EuralCode', right_on='EURAL_EURAL_6_cijfer_code').set_axis(dataframe.index)

    no_agenda = [dataframe['EURAL_Transitieagenda_indicatief'].isnull()]

    # ------------------------------------------------------------------------------
    # GN CODE - CHAIN POSITION
    # ------------------------------------------------------------------------------
    if "GNcode" in dataframe.columns:
        logging.info(f"Add transition agenda based on GN code...")

        transition_agendas = pd.read_csv('Private_data/Classification/CN_Transitieagenda_v1.2.csv', sep="\t", low_memory=False)
        transition_agendas.dropna(how='all', axis='columns', inplace=True)
        transition_agendas.columns = [f'GN_{col}' for col in transition_agendas.columns]
        transition_agendas = transition_agendas[transition_agendas['GN_CN_LMA'].notnull()]
        transition_agendas['GN_CN_LMA'] = transition_agendas['GN_CN_LMA'].str.upper()

        dataframe.loc[dataframe["GNcode"].notnull(), "GNcode"] = dataframe["GNcode"].str.upper()
        dataframe = pd.merge(dataframe, transition_agendas, how='left',
                             left_on='GNcode', right_on='GN_CN_LMA').set_axis(dataframe.index)

        no_agenda.append(dataframe['GN_Transitieagenda_NL'].isnull())

    unclassified = [
        "EURAL_Gevaarlijk?",
        "EURAL_Ketenpositie",
        "EURAL_(a)biotisch",
        "EURAL_(an)organisch",
        "EURAL_Tags_algemeen",
        "EURAL_Transitieagenda_indicatief",
        "EURAL_Tags_industrie",
        "EURAL_Tags_type_code",
        "EURAL_schoon/vervuild",
        "EURAL_gemengd/puur"
    ]
    dataframe.loc[np.logical_and.reduce(no_agenda), unclassified] = "ongeclassificeerd"

    return dataframe
