# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 14:43:17 2025

@author: Marcos.Perrude
"""
import pandas as pd 
import geopandas as gpd
import numpy as np
from EmissionsEstimate import EmissionsEstimate
import os
from EmissionsPixels import EmissionsPixels,geoGrid2mat, cellTimeZone
from CreateGrid import CreateGrid
import matplotlib.pyplot as plt
import xarray as xr
#Pasta do repositório
DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
#Pasta dados
DataPath = os.path.join(DataDir,'Inputs')
OutPath = os.path.join(DataDir, 'Outputs')
br_uf = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))

# Estimando as emissões por setores
#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
dff = pd.read_csv(DataPath + '\BR_setores_CD2022.csv')
woodEmission, coalEmission, totalEmission, poluentes = EmissionsEstimate(
    dff,DataPath,OutPath)

#%% Transformando em netCDF
Combustivel = "All"

# Definições da grade
Tam_pixel = 0.1  # 0.1 Equivale a ~1km se o CRS for metros
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
for ii, uf in enumerate(ufs):
    if ii == 0:
        # Transformando em uma matriz x por y
        gridMat4D = np.zeros((len(poluentes),12, np.shape(np.unique(gridGerado.lat))[0],
                              np.shape(np.unique(gridGerado.lon))[0]))

    
    # Colocando emissões de cada estado na grade
    emiGrid = EmissionsPixels(Tam_pixel, Combustivel, woodEmission, coalEmission, 
                           totalEmission, gridGerado, DataPath, OutPath,uf, br_uf)
    
    # transforma em matriz e soma as emissões de cada estado
    gridMat4D = geoGrid2mat(emiGrid,gridMat4D,poluentes,uf,ltcGrid,DataPath)

a = gridMat4D[0,0,:,:]
# soma_emiGrid = emiGrid['PM'].sum(skipna=True)
# soma_gridMat4D = gridMat4D[0,:,:,:].sum()  # soma em todos os meses e pixels

data_vars = {}
for i, nome in enumerate(poluentes):
    data_vars[nome] = xr.DataArray(
        gridMat4D[i],  # shape: (tempo, lat, lon)
        dims=["tempo", "lat", "lon"],
        coords={
            "tempo": np.arange(gridMat4D.shape[1]),
            "lat": np.unique(yy[:,0]),
            "lon": np.unique(xx[0,:])
            }
    )

# Criar o Dataset
ds = xr.Dataset(data_vars)
 
# fig, ax= plt.subplots(figsize=(10, 8))
# ds["PM"].sel(tempo=0).plot(ax=ax)
# br_uf.boundary.plot(ax=ax)


