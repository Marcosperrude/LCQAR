# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 14:24:15 2025

@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os
from timezonefinder import TimezoneFinder
from emissionsGrid import EmssionsGrid
import dask
from dask import delayed, compute
from dask.diagnostics import ProgressBar

#
def EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, woodEmission, coalEmission
                       , gridGerado, DataPath, OutPath, uf, br_uf, 
                       poluentesWoodCoal, setores):
    # df = WoodCoalDf
    # # Padroniza CD_SETOR
    for df in [woodEmission, coalEmission]:
        df['CD_SETOR'] = df['CD_SETOR'].astype(str)
    # Processa os estados dentro do grid
    #estados_intersectados = br_uf[br_uf.intersects(gridGerado.unary_union)].copy()
    #ufs = list(estados_intersectados['SIGLA_UF'])
    
    setores_brasil = []
    #for uf in ufs:
    print(f"Processando {uf}...")
    pasta_uf = [f for f in os.listdir(setores) if f.startswith(uf)][0]
    shapefile_path = os.path.join(setores, pasta_uf)
    shp_file = [f for f in os.listdir(shapefile_path) if f.endswith(".shp")][0]
    gdf_uf = gpd.read_file(os.path.join(shapefile_path, shp_file))
    gdf_uf['CD_SETOR'] = gdf_uf['CD_SETOR'].astype(str)
    
    if Combustivel == "Lenha":
        emissoes_uf = pd.merge(woodEmission, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
    elif Combustivel == "Carvao":
        emissoes_uf = pd.merge(coalEmission, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
        
    # emissoes_uf = pd.merge(WoodCoalDf, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
    
    emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry', crs=gdf_uf.crs)
    setores_brasil.append(emissoes_uf)
    emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
    emiGrid = EmssionsGrid(emissoes, gridGerado, poluentesWoodCoal)
    
    # emiGrid = gridGerado.copy()
    # for pol in poluentesWoodCoal:
    #     print(f"Calculando {pol}...")
    #     valores = []
    #     for cell in gridGerado.geometry:
    #         setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
    #         if setores_intersectados.empty:
    #             valores.append(np.nan)
    #             continue
    #         setores_intersectados["area_total"] = setores_intersectados.geometry.area
    #         setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
    #         setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
    #         valor = (setores_intersectados[pol] * setores_intersectados["peso"]).sum()
    #         valores.append(valor)  

    #     emiGrid[pol] = valores
        
    # emissoes_uf.plot()
    # emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
    
    return emiGrid

# #%%
# import geopandas as gpd

# emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry')
# emissoes_uf.to_file(r'C:\Users\marcos perrude\Downloads\emissoes_uf.shp')
# import matplotlib.pyplot as plt

# fig, ax = plt.subplots(figsize=(10, 8))
# emissoes_uf.plot(
#     column='Consumo_lenha[ton/ano]',
#     ax=ax,
#     legend=True,
#     cmap='OrRd',
#     edgecolor='black'  # só se for geometria Polygon
# )
# plt.show()

#%%
import xarray as xr
#Gerar Grid de emissoes de GLP
def EmissionsPixelsGLP (combDf ,br_mun, gridGerado, poluentesGLP):
    br_mun['CD_MUN'] = br_mun['CD_MUN'].astype(int)#Shape com geometria
    combDf['CODIGO IBGE'] = combDf['CODIGO IBGE'].astype(int)
    combDf.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace = True)
    
    geoCombDf = pd.merge(
        combDf,
        br_mun[['CD_MUN', 'geometry']],
        on='CD_MUN',
        how='left'  # left para manter todos de combDt, mesmo sem geometria
    )
    
    geoCombDf['CD_MUN'] = geoCombDf['CD_MUN'].astype(object)
    geoCombDf = gpd.GeoDataFrame(geoCombDf, geometry='geometry')

    
    emiGrid = EmssionsGrid(geoCombDf, gridGerado, poluentesGLP)

    return emiGrid

def cellTimeZone(xx,yy):
    
    #Tempo de teste apena sapara extrair o fuso
    test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    
    #Biblioteca que retorno o fuso
    tf = TimezoneFinder(in_memory=True)
    ltc =[]
    
    for ii in range(0,xx.shape[0]):# Para todo x da grade 
        #Loop over each cel in y direction
        for jj in range(0,xx.shape[1]):# Para todo y da grade
            local_time_zone = tf.timezone_at(lng=xx[ii,jj], lat=yy[ii,jj]) #Localiza o time zone
            ltc.append(float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1])) #Extrai o numero do time zone
            
    ltcGrid = np.reshape(ltc,
                         (np.shape(xx)[0],
                          np.shape(xx))[1])
    return ltcGrid


def GridMat5D(combustivel, emiGrid, poluentes, DataPath, uf):

    # Carrega fatores temporais mensais
    temporalFactor = pd.read_csv(DataPath + '\\fatdes.csv')
    temporalFactorUF = temporalFactor[temporalFactor['UF Destino'] == uf]
    
    
    if combustivel in ['Lenha', 'Carvao']:
        temporalEscale = 54
        temporalFactorHist = pd.read_csv(DataPath + '\\fatdesEPE.csv', index_col=0)
        temporalFactorCombustivel = temporalFactorHist.loc[combustivel].reset_index()
        
    elif combustivel == 'GLP':
        temporalEscale = 23
        
    data_vars = {}

    # Loop para cada poluente
    for ii, pol in enumerate(poluentes):
    # pol = 'PM'        
    # ii = 0   
        pivot = emiGrid.pivot_table(index='lat', columns='lon', values=pol, fill_value=0)
        gridMat = pivot.values  # Transposta correta
        
        print(f'Processando {pol}')
        
        gridMat4d = np.zeros((temporalEscale, 12, len(emiGrid['lat'].unique()),
                              len(emiGrid['lon'].unique())))

                                                                                                                 

        for jj in range(temporalEscale):
            if combustivel in ['Lenha', 'Carvao']:
                # Desagregação anual
                gridMat2d = gridMat * temporalFactorCombustivel[combustivel].iloc[jj]
                # Desagregação mensal
            else : 
                gridMat2d = gridMat
                
            for kk in range(12):
                
                gridMat4d[jj,kk,:,:] = (gridMat2d * temporalFactorUF['Peso'].reset_index(drop=True).iloc[kk]).astype(np.float16)
            
        
        gridMat3r = gridMat4d.reshape(temporalEscale * 12, len(emiGrid['lat'].unique()),
                                      len(emiGrid['lon'].unique()))
        fim = pd.Timestamp('2023-12-01')
        # Começa no primeiro ano contando para trás
        inicio = fim.year - (temporalEscale -1)
        inicio = pd.Timestamp(year= inicio, month=1, day=1)
        
        da = xr.DataArray(
        gridMat3r,
        dims=["time", "lat", "lon"],
        coords={
            "time": pd.date_range(
                start= inicio,
                periods=gridMat3r.shape[0],
                freq="MS"
            ),
            "lat": emiGrid['lat'].unique(),
            "lon": emiGrid['lon'].unique()
            }
        )
        data_vars[pol] = da
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {combustivel} - UF {uf}"

    return ds

