# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 14:43:17 2025

@author: Marcos.Perrude
"""
import pandas as pd 
import geopandas as gpd
import numpy as np
from EmissionsEstimateWoodCoal import emissionEstimateWoodCoal
import os
from EmissionsPixels import EmissionsPixelsWoodCoal,geoGrid4mat,geoGrid5mat, cellTimeZone , EmissionsPixelsGLP
from CreateGrid import CreateGrid
import matplotlib.pyplot as plt
import xarray as xr

#Pasta do repositório
DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"

#Pasta dados
DataPath = os.path.join(DataDir,'Inputs')

#Pasta de Outputs
OutPath = os.path.join(DataDir, 'Outputs')

br_uf = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))

br_mun = gpd.read_file(DataPath + '\BR_Municipios_2022\BR_Municipios_2022.dbf')

setores = os.path.join(DataPath, 'Setores')

# Estimando as emissões para lenha e carvao por setor
#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
WoodCoalDf = pd.read_csv(DataPath + '\BR_setores_CD2022.csv')
woodEmission, coalEmission, poluentesWoodCoal = emissionEstimateWoodCoal(
    WoodCoalDf,DataPath,OutPath)

#%% Definindo o grid
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

#%% Xarray de emissoes Lenha e Carvão
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
            gridMat4D = np.zeros((len(poluentesWoodCoal),54, np.shape(np.unique(gridGerado.lat))[0],
                                  np.shape(np.unique(gridGerado.lon))[0]))
            
            gridMat5D = np.zeros((len(poluentesWoodCoal),54, 12, np.shape(np.unique(gridGerado.lat))[0],
                                  np.shape(np.unique(gridGerado.lon))[0]))
        
        # Colocando emissões de cada estado na grade
        emiGrid = EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, woodEmission, coalEmission 
                               , gridGerado, DataPath, OutPath,uf, br_uf ,
                               poluentesWoodCoal, setores)
    
        
        # transforma em matriz e soma as emissões de cada estado
        gridMat4D = geoGrid4mat(Combustivel, emiGrid,gridMat4D,poluentesWoodCoal,DataPath)
        
        gridMat5D = geoGrid5mat(gridMat4D, gridMat5D, poluentesWoodCoal, uf, ltcGrid, DataPath)

    
    gridMat4Dtemp = gridMat5D.reshape(gridMat5D.shape[0], 
                                  gridMat5D.shape[1] * gridMat5D.shape[2], 
                                  gridMat5D.shape[3],
                                  gridMat5D.shape[4])
    
    data_vars = {}
    for i, nome in enumerate(poluentesWoodCoal):
        data_vars[nome] = xr.DataArray(
            gridMat4Dtemp[i,:,:,:],  # shape: (pol, time, lat, lon)
            dims=["time", "lat", "lon"],
            coords={
                "time": pd.date_range(start=f"{1970}-01",
                                       periods=gridMat5D.shape[1] * gridMat5D.shape[2],
                                       freq="MS"),
                "lat": yy[:,0],
                "lon": xx[0,:]
                }
        )
    
    # Criar o Dataset
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {Combustivel}"
    datasets[Combustivel] = ds
    
emiCoal = datasets['Carvao'].copy()
emiWood = datasets['Lenha'].copy()
#%% Verificando se esta certo


# pesos = {}

# for var in emiCoal.data_vars:
#     # Soma total de emissões por ano (tempo, lat, lon somados)
#     total_anual = emiCoal[var].groupby("tempo.year").sum(dim=["tempo", "lat", "lon"])

#     # Valor total para o ano de 2023
#     valor_2023 = total_anual.sel(year=2023)

#     # Peso relativo de cada ano em relação a 2023
#     pesos[var] = (total_anual / valor_2023).to_pandas()

# #Tem que bater com o csv dos pesos anuais
# print(pesos["CO"])

#%%
from EmissionsEstimateGLP import emissionEstimateGLP 

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis
glpDf = pd.read_csv(DataPath + '\\vendas-anuais-de-glp-por-municipio.csv',encoding ='utf-8')
glpDf = glpDf[glpDf['ANO'] >= 2000]
propEmiCid, butEmiCid, poluentesGLP = emissionEstimateGLP(DataPath, OutPath, glpDf)


for ii, combustivel in enumerate(propEmiCid, butEmiCid):
    
    gridMatGLP4D = np.zeros((len(poluentesGLP),len(glpDf['ANO'].unique()), np.shape(np.unique(gridGerado.lat))[0],
                          np.shape(np.unique(gridGerado.lon))[0]))
    
    for jj, ano in enumerate(glpDf['ANO'].unique()):
        print(f"Processando {ano}...")
        emiGridGLP = EmissionsPixelsGLP(propEmiCid[propEmiCid['ANO'] == 2022], br_mun,
                                        gridGerado, poluentesGLP)
        
        
        for ii, pol in enumerate(poluentesGLP):
            print(f"Processando {pol}...")
            gridMatGLP = np.reshape(emiGridGLP[pol].fillna(0),
                                 (np.shape(np.unique(emiGridGLP.lon))[0],
                                  np.shape(np.unique(emiGridGLP.lat))[0])).transpose()
            
            gridMatGLP4D[ii,jj,:,:] += gridMatGLP







# gridMat4D = np.zeros((len(poluentesWoodCoal),12, np.shape(np.unique(gridGerado.lat))[0],
#                       np.shape(np.unique(gridGerado.lon))[0]))

# for ii in (propEmiCid, butEmiCid):
#     emiGrid = EmissionsPixelsGLP(ii, br_mun, gridGerado)
#     for ii, pol in enumerate(poluentesGLP):
#         gridMat = np.reshape(emiGrid[pol].fillna(0),
#                              (np.shape(np.unique(emiGrid.lon))[0],
#                               np.shape(np.unique(emiGrid.lat))[0])).transpose()
















