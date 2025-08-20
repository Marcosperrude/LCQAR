# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 14:24:15 2025

@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os
# from timezonefinder import TimezoneFinder
from emissionsGrid import EmssionsGrid
import xarray as xr

#
def EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, dt
                       , gridGerado, DataPath, OutPath, uf, br_uf, 
                       poluentesWoodCoal, setores):
    # uf = 'SP'
     # dt = woodEmission
    # # Padroniza CD_SETOR
    dt['CD_SETOR'] = dt['CD_SETOR'].astype(str)
    # Processa os estados dentro do grid
    #estados_intersectados = br_uf[br_uf.intersects(gridGerado.unary_union)].copy()
    #ufs = list(estados_intersectados['SIGLA_UF'])
    

    #for uf in ufs:
    print(f"Processando {uf}...")
    pasta_uf = [f for f in os.listdir(setores) if f.startswith(uf)][0]
    shapefile_path = os.path.join(setores, pasta_uf)
    shp_file = [f for f in os.listdir(shapefile_path) if f.endswith(".shp")][0]
    gdf_uf = gpd.read_file(os.path.join(shapefile_path, shp_file))
    gdf_uf['CD_SETOR'] = gdf_uf['CD_SETOR'].astype(str)
    
    emissoes_uf = gpd.GeoDataFrame(
    pd.merge(dt, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right'),
    geometry='geometry',
    crs=br_uf.crs)

    emiGrid = EmssionsGrid(emissoes_uf, gridGerado, poluentesWoodCoal)
    
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

def EmissionsPixelsGLP(dt, BR_MUN, gridGerado, poluentesGLP, DataPath, Combustivel, ufs):
    from emissionsGrid import EmssionsGrid
    """
    Calcula emissões de GLP para todos os anos e UFs de uma só vez.
    Retorna a matriz 5D preenchida.
    """

    gridMat5Dglp = np.zeros((len(poluentesGLP),
                             len(dt['ANO'].unique()), 12,
                             np.shape(np.unique(gridGerado.lat))[0],
                             np.shape(np.unique(gridGerado.lon))[0]))


    # Merge com geometrias
    BR_MUN['CD_MUN'] = BR_MUN['CD_MUN'].astype(int)
    dt['CODIGO IBGE'] = dt['CODIGO IBGE'].astype(int)
    dt = dt.rename(columns={'CODIGO IBGE': 'CD_MUN'})
    geoCombDf = pd.merge(dt, 
                         BR_MUN[['CD_MUN', 'geometry']], 
                         on='CD_MUN',
                         how='left')
    geoCombDf = gpd.GeoDataFrame(geoCombDf, geometry='geometry')

    # Loop ano/UF
    for ano in geoCombDf['ANO'].unique():
        # ano = 2020
        print(f"Processando {ano}...")
        dados_ano = geoCombDf[geoCombDf['ANO'] == ano]

        
        for uf in ufs:
            # uf= 'SC'
            dados_ano_uf = dados_ano[dados_ano['UF'] == uf]
            emiGridGLP = EmssionsGrid(dados_ano_uf, gridGerado, poluentesGLP)
            gridMat5Dglp = GridMat5D(Combustivel, emiGridGLP, gridMat5Dglp,
                                     poluentesGLP, DataPath, uf)

    return gridMat5Dglp

# #Gerar Grid de emissoes de GLP
# def EmissionsPixelsGLP (combDf ,BR_MUN, gridGerado, poluentesGLP):
    
#     # combDf = propEmiCid[(propEmiCid['ANO'] == 2000) & (propEmiCid['UF'] == 'SP')]
    
#     BR_MUN['CD_MUN'] = BR_MUN['CD_MUN'].astype(int)#Shape com geometria
#     combDf['CODIGO IBGE'] = combDf['CODIGO IBGE'].astype(int)
#     combDf.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace = True)
    
#     geoCombDf = pd.merge(
#         combDf,
#         BR_MUN[['CD_MUN', 'geometry']],
#         on='CD_MUN',
#         how='left'  # left para manter todos de combDt, mesmo sem geometria
#     )
    
    
#     geoCombDf['CD_MUN'] = geoCombDf['CD_MUN'].astype(object)
#     geoCombDf = gpd.GeoDataFrame(geoCombDf, geometry='geometry')

    
#     emiGrid = EmssionsGrid(geoCombDf, gridGerado, poluentesGLP)

#     return emiGrid

# def cellTimeZone(xx,yy):
    
#     #Tempo de teste apena sapara extrair o fuso
#     test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    
#     #Biblioteca que retorno o fuso
#     tf = TimezoneFinder(in_memory=True)
#     ltc =[]
    
#     for ii in range(0,xx.shape[0]):# Para todo x da grade 
#         #Loop over each cel in y direction
#         for jj in range(0,xx.shape[1]):# Para todo y da grade
#             local_time_zone = tf.timezone_at(lng=xx[ii,jj], lat=yy[ii,jj]) #Localiza o time zone
#             ltc.append(float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1])) #Extrai o numero do time zone
            
#     ltcGrid = np.reshape(ltc,
#                          (np.shape(xx)[0],
#                           np.shape(xx))[1])
#     return ltcGrid


def GridMat5D(Combustivel, emiGrid, gridMat5D, poluentes, DataPath, uf):
   
    # Combustivel= 'Propano'
    # emiGrid = emiGridGLP
    # gridMat5D = gridMat5Dglp
    # poluentes = poluentesGLP
               
    
   
    temporalFactorHist = pd.read_csv(DataPath + '/fatdesEPE.csv', index_col=0)
    temporalFactor = pd.read_csv(DataPath + '/fatdes.csv')
    temporalFactorUF = temporalFactor[temporalFactor['UF Destino'] == uf]
    pesosMensais = temporalFactorUF['Peso'].reset_index(drop=True).values
   
    for ii, pol in enumerate(poluentes):
      # ii = 0
      # pol = 'CO'
      gridMat = np.reshape(
          emiGrid[pol].fillna(0),
          (np.shape(np.unique(emiGrid.lon))[0],
           np.shape(np.unique(emiGrid.lat))[0])
      ).transpose()   
      
      if Combustivel in ['Lenha', 'Carvao']:
            temporalFactorCombustivel = temporalFactorHist.loc[Combustivel].reset_index(drop=True)
            for jj in range(gridMat5D.shape[1]):
                gridMat4D = gridMat * temporalFactorCombustivel.iloc[jj]  # (lat, lon)
                # Vetoriza os 12 meses
                gridMat5D[ii, jj, :, :, :] += gridMat4D[np.newaxis, :, :] * pesosMensais[:, np.newaxis, np.newaxis]
      else:
            # vetoriza direto os 12 meses
            for jj in range(gridMat5D.shape[1]):
                gridMat5D[ii, jj, :, :, :] += gridMat[np.newaxis, :, :] * pesosMensais[:, np.newaxis, np.newaxis]

    return gridMat5D
