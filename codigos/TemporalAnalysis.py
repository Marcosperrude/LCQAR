# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:58:24 2025

@author: Marcos.Perrude
"""
import pandas as pd
import seaborn as sbn
import matplotlib.pyplot as plt
import numpy as np
import os

DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
#Pasta dados
DataPath = os.path.join(DataDir,'Inputs')
OutPath = os.path.join(DataDir, 'Outputs')


#%% Ànalise mensal
df = pd.read_csv(DataPath + '\GLP_Vendas_Historico.csv', encoding='latin1')

glp = df[
    (df['Mercado Destinatário'] == 'CONSUMIDOR FINAL') &
    (~df["Código de Embalagem GLP"].isin(["P 190", "A Granel"]))
    ]

glp['Quantidade de Produto(mil ton)'] = glp['Quantidade de Produto(mil ton)'].astype(int)

glp_mes_uf = glp.groupby(['UF Destino', 'Mês'])['Quantidade de Produto(mil ton)'].mean().reset_index()

def calcula_peso(x):
    return x / x.sum()

glp_mes_uf['Peso'] = glp_mes_uf.groupby('UF Destino')['Quantidade de Produto(mil ton)'].transform(calcula_peso)

fatdes = glp_mes_uf.drop("Quantidade de Produto(mil ton)", axis='columns')

uf_codigos = {
    "RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17,
    "MA": 21, "PI": 22, "CE": 23, "RN": 24, "PB": 25, "PE": 26, "AL": 27, "SE": 28, "BA": 29,
    "MG": 31, "ES": 32, "RJ": 33, "SP": 35,
    "PR": 41, "SC": 42, "RS": 43,
    "MS": 50, "MT": 51, "GO": 52, "DF": 53
}
fatdes['CD_UF'] = fatdes["UF Destino"].map(uf_codigos)
fatdes.to_csv(os.path.join(DataPath, 'fatdes.csv'), index = False)

#%% Análise Anual

#consumo em tep
df= pd.read_csv(DataPath + '\\EPE_Consumo_Historico.csv',index_col = [0],  encoding='latin1', )
df= df.replace(',', '', regex=True).astype(float)


df2023 = df["2023"]

fatdesEPE = df.div(df['2023'], axis=0)

fatdesEPE.to_csv(os.path.join(OutPath, 'fatdesEPE.csv'), index = True)

#%%

import xarray as xr
import geopandas as gpd
import re 
import os

Cams = os.path.join(DataPath, 'CAMS')

arquivos  = [f for f in os.listdir(Cams) if f.endswith(".nc")]
ds = xr.open_dataset(os.path.join(Cams , arquivos[0]))

br = gpd.read_file(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_Pais_2022 (1)\BR_Pais_2022.shp")
br = br.to_crs(epsg=4326)
minx, miny, maxx, maxy = br.total_bounds
ds_br = ds.sel(
    latitude=slice(miny, maxy),
    longitude=slice(minx, maxx)
)

import rioxarray
from rioxarray.merge import merge_datasets

# Primeiro, atribuir o CRS corretamente
ds_br = ds_br.rio.write_crs("EPSG:4326")

# Convertendo a geometria do Brasil para formato GeoJSON
br_geom = br.geometry.values[0]  # Como é um shapefile do país inteiro, provavelmente só tem 1 polígono

# Mascara com base no polígono
masked = ds_br.rio.clip(
                    [br_geom],
                    crs="EPSG:4326",
                    all_touched=True
                )

fig, ax = plt.subplots(figsize=(10, 8))
masked.FD_res.isel(time=0).plot(ax=ax)
br.boundary.plot(ax=ax, color='red', linewidth=1.5)

lat_min = masked.latitude.min().item()
lat_max = masked.latitude.max().item()
lon_min = masked.longitude.min().item()
lon_max = masked.longitude.max().item()

print(f"Latitude min: {lat_min}, max: {lat_max}")
print(f"Longitude min: {lon_min}, max: {lon_max}")


lat_joi = -26.304
lon_joi = -48.848

# Selecionar o pixel mais próximo
fd_joinville = masked.FD_res.sel(
    latitude=lat_joi,
    longitude=lon_joi,
    method='nearest'
)


fd_joinville.plot(marker='o')
plt.title("Série Histórica do Fator de Desagregação - Joinville (2011)")
plt.xlabel("Data")
plt.ylabel("Fator de Desagregação")
plt.grid(True)
plt.show()

# soma_fd = fd_joinville.sum().item()

# # Número de dias
# n_dias = fd_joinville.sizes['time']

# # Média dos fatores diários
# media_fd = soma_fd / n_dias

# print(f"Soma total: {soma_fd:.6f}")
# print(f"Média: {media_fd:.6f}")
