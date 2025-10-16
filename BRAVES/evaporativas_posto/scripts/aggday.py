#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 12:46:44 2025

@author: marcos
"""

import os
import pandas as pd 

dataPath = '/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto/outputs'

emiPath = os.path.join(dataPath, "emissoes_postos", "emissoes_2023_01")

arquivos_parquet = [os.path.join(emiPath, f) for f in os.listdir(emiPath)
    if f.endswith(".parquet")]

emissoes = pd.concat((pd.read_parquet(f) for f in arquivos_parquet))
emissoes['cell_id'] = emissoes['cell_id'] - 1
emissoes = emissoes.drop(columns=['city_id'])
emissoes['datetime'] = pd.to_datetime(emissoes['datetime'])

emissoes.rename(columns={"emis_total" : "REFUEL" }, inplace = True)
                         
for dia in emissoes['datetime'].dt.day.unique():
    
    emissoes_dia = emissoes[emissoes['datetime'].dt.day == dia]
    emissoes_dia = emissoes_dia.set_index(['datetime', 'cell_id']).sort_index()
    nome_arquivo = os.path.join(dataPath, "emissoes_postos","emissoes_2023_01_day.parquet",
                                f"2023_01_{str(dia).zfill(2)}")
    print(f"dia {dia} salvo")
    
    emissoes_dia.to_parquet(nome_arquivo)
