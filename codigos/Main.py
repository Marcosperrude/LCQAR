# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 14:43:17 2025

@author: Marcos.Perrude
"""
import pandas as pd 
import geopandas as gpd
import numpy as np
from EmissionsEstimate import emissionEstimateWoodCoal
import os
from EmissionsPixels import EmissionsPixelsWoodCoal,geoGrid2mat, cellTimeZone , EmissionsPixelsGLP
from CreateGrid import CreateGrid
import matplotlib.pyplot as plt
import xarray as xr

#Pasta do repositório
DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"

#Pasta dados
DataPath = os.path.join(DataDir,'Inputs')
OutPath = os.path.join(DataDir, 'Outputs')
br_uf = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))
br_mun = gpd.read_file(DataPath + '\BR_Municipios_2022\BR_Municipios_2022.dbf')
setores = os.path.join(DataPath, 'Setores')

# Estimando as emissões para lenha e carvao por setor
#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
WoodCoalDf = pd.read_csv(DataPath + '\BR_setores_CD2022.csv')
woodEmission, coalEmission, totalEmission, poluentesWoodCoal = emissionEstimateWoodCoal(
    WoodCoalDf,DataPath,OutPath)

#%%
# Definições da grade
Tam_pixel = 1  # 0.1 Equivale a ~1km se o CRS for metros
minx = -53.90  # longitude mínima de SC (oeste)
maxx = -48.30  # longitude máxima de SC (leste)
miny = -29.35  # latitude mínima de SC (sul)
maxy = -26  # latitude máxima de SC (norte)

gridGerado, xx, yy = CreateGrid(Tam_pixel,minx,maxx,miny,maxy)

# fig, ax = plt.subplots(figsize=(10, 10))
# gridGerado.boundary.plot(ax=ax, color='gray')
# br_uf.boundary.plot(ax=ax, color='black')

# Estados
estados_intersectados = br_uf[br_uf.intersects(gridGerado.unary_union)].copy()
ufs = list(estados_intersectados['SIGLA_UF'])

# fig,ax = plt.subplots()
# estados_intersectados.boundary.plot(ax=ax)
# gridGerado.boundary.plot(ax=ax)
ltcGrid = cellTimeZone(xx,yy)

datasets = {}
for Combustivel in ('Lenha','Carvao'):
    for ii, uf in enumerate(ufs):
        if ii == 0:
            # Transformando em uma matriz x por y
            gridMat4D = np.zeros((len(poluentesWoodCoal),12, np.shape(np.unique(gridGerado.lat))[0],
                                  np.shape(np.unique(gridGerado.lon))[0]))
    
        
        # Colocando emissões de cada estado na grade
        emiGrid = EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, woodEmission, coalEmission, 
                               totalEmission, gridGerado, DataPath, OutPath,uf, br_uf ,
                               poluentesWoodCoal, setores)
    
        
        # transforma em matriz e soma as emissões de cada estado
        gridMat4D = geoGrid2mat(emiGrid,gridMat4D,poluentesWoodCoal,uf,ltcGrid,DataPath)
     
    data_vars = {}
    for i, nome in enumerate(poluentesWoodCoal):
        data_vars[nome] = xr.DataArray(
            gridMat4D[i,:,:,:],  # shape: (tempo, lat, lon)
            dims=["tempo", "lat", "lon"],
            coords={
                "tempo": np.arange(gridMat4D.shape[1]),
                "lat": yy[:,0],
                "lon": xx[0,:]
                }
        )
    
    # Criar o Dataset
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {Combustivel}"
    datasets[Combustivel] = ds
    
emiCoal = datasets['Carvao'].copy()
emiWood = datasets['Lenhao'].copy()
#%%
from EmissionsEstimateGLP import emissionEstimateGLP 

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis
glpDf = pd.read_csv(DataPath + '\\vendas-anuais-de-glp-por-municipio.csv',encoding ='utf-8')
glpDf = glpDf[glpDf['ANO'] >= 2000]
propEmiCid, butEmiCid, poluentesGLP = emissionEstimateGLP(DataPath, OutPath, glpDf)

# gridMat4D = np.zeros((len(poluentesWoodCoal),12, np.shape(np.unique(gridGerado.lat))[0],
#                       np.shape(np.unique(gridGerado.lon))[0]))

# for ii in (propEmiCid, butEmiCid):
#     emiGrid = EmissionsPixelsGLP(ii, br_mun, gridGerado)
#     for ii, pol in enumerate(poluentesGLP):
#         gridMat = np.reshape(emiGrid[pol].fillna(0),
#                              (np.shape(np.unique(emiGrid.lon))[0],
#                               np.shape(np.unique(emiGrid.lat))[0])).transpose()
















