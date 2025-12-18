#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 12:46:44 2025

    Codigo utilizado para agrupar .parquet emissões diária por cidade, para
    um .parquet por dia

@author: marcos
"""

import os
import pandas as pd 

dataPath = '/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto/outputs'

years = [2021]
months = range(1, 13)

for year in years:
    for month in months:
        month = f'{month:02}'
    
        emiPath = os.path.join(
            dataPath, "emissoes_postos", f"emissoes_{year}_{month}")
        
        arquivos_parquet = [
            os.path.join(emiPath, f) for f in os.listdir(emiPath)
            if f.endswith(".parquet")]
        
        emissoes = pd.concat((pd.read_parquet(f) for f in arquivos_parquet))
        # emissoes['cell_id'] = emissoes['cell_id'] - 1
        emissoes = emissoes.drop(columns=['city_id'])
        emissoes['datetime'] = pd.to_datetime(emissoes['datetime'])
        
        # emissoes.rename(columns={"emis_total" : "REFUEL" }, inplace = True)
                                 
        for dia in emissoes['datetime'].dt.day.unique():    
            emissoes_dia = emissoes[emissoes['datetime'].dt.day == dia]
            emissoes_dia = emissoes_dia.groupby(["datetime","cell_id"]).sum()
            # emissoes_dia = emissoes_dia.set_index(x
            #     ['datetime', 'cell_id']).sort_index()
            nome_arquivo = os.path.join(
                dataPath, "emissoes_postos","emissoes_2021",
                f"{year}-{month}-{dia:02}.parquet")
            print(f"dia {dia} salvo")
            
            emissoes_dia.to_parquet(nome_arquivo)
#%%

a = pd.read_parquet(nome_arquivo)


#%%

dataTemp = os.path.join(dataPath, 'temperatura_csv')

for i in range(1,13):
    temp = pd.read_csv("{}/temperatura_cidade_2023_{}.csv".format(dataTemp, str(i).zfill(2)))
    medtemp = temp.groupby('CD_MUN')['TEMP_C'].mean()
    medtemp.to_csv(dataTemp + f'/Temperatura_media_mensal/2023-{i}.csv')



