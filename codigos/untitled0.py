# -*- coding: utf-8 -*-
"""
Created on Mon May 19 09:54:38 2025

@author: Marcos.Perrude
"""
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from emissionsGrid import EmssionsGrid

combDt = propEmiCid[propEmiCid['ANO'] == 2022]
def EmissionsEstimateGLP (combDt ,br_mun, gridGerado):
    br_mun['CD_MUN'] = br_mun['CD_MUN'].astype(int)#Shape com geometria
    combDt['CODIGO IBGE'] = combDt['CODIGO IBGE'].astype(int)
    combDt.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace = True)
    
    geoCombDt = pd.merge(
        combDt,
        br_mun[['CD_MUN', 'geometry']],
        on='CD_MUN',
        how='left'  # left para manter todos de combDt, mesmo sem geometria
    )
    
    geoCombDt['CD_MUN'] = geoCombDt['CD_MUN'].astype(object)
    geoCombDt = gpd.GeoDataFrame(geoCombDt, geometry='geometry')

    poluentes = ['PM', 'SO2', 'Nox', 'N2O', 'CO2', 'CO', 'CH4']
    
    emiGrid = EmssionsGrid(geoCombDt, gridGerado, poluentes)

    return emiGrid
