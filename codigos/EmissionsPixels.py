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


#
def EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, woodEmission, coalEmission
                       , gridGerado, DataPath, OutPath, uf, br_uf, 
                       poluentesWoodCoal, setores):


    # Padroniza CD_SETOR
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

    emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry', crs=gdf_uf.crs)
    setores_brasil.append(emissoes_uf)
        
    emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
        # Calcula emissões por célula
        
    emiGrid = EmssionsGrid(emissoes, gridGerado, poluentesWoodCoal)

    return emiGrid


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

def geoGrid4mat(combustivel, emiGrid,gridMat4D,poluentes,DataPath):
    temporalFactorHist = pd.read_csv(DataPath + '\\fatdesEPE.csv', index_col = [0])
    
    #Localiza o historico de consumo
    temporalFactorCombustivel = temporalFactorHist.loc[combustivel].reset_index()
    
    #Loop para cada poluente
    for ii, pol in enumerate(poluentes):
        gridMat = np.reshape(emiGrid[pol].fillna(0),
                             (np.shape(np.unique(emiGrid.lon))[0],
                              np.shape(np.unique(emiGrid.lat))[0])).transpose()
   
        #Loop para cada Ano
        for jj in range(gridMat4D.shape[1]):
            gridMat4D[ii,jj,:] = gridMat4D[ii,jj,:,:]  + gridMat * temporalFactorCombustivel[combustivel].iloc[jj]
                
    return gridMat4D

def geoGrid5mat(gridMat4D,gridMat5D,poluentes,uf,ltcGrid,DataPath):
    temporalFactor = pd.read_csv(DataPath + '\\fatdes.csv')
    
    #Localiza a desagregação temporal no csv de acordo com UF
    temporalFactorUF = temporalFactor[temporalFactor['UF']==uf]
    # Loop para cara poluente
    for ii, pol in enumerate(poluentes):
     # apara cad apoluente e mes, agrupar pelo fuso horário
     # apara cad apoluente e mes, agrupar pelo fuso horário
        # for jj in range(0,12):
        #     gridMat4D[ii, jj, :, :] == gridMat4D[ii, jj, :, :] + gridMat * pesos[jj]
            #gridMat4D[ii,jj,:] =gridMat4D[ii,jj,:,:]  + gridMat*temporalFactorUF['Peso'].reset_index().iloc[jj].Peso
            # gridMat4D[ii,jj,idx]=gridMat4D[ii,jj,idx]+ gridMat[idx]* np.roll(temporalFactorUF['Peso'],
            #int(utcoff))[jj]
        
        # Loop para cada Ano    
        for yy in range(gridMat4D.shape[1]): 
            
            # Loop para cada mes
            for jj in range(12):
                gridMat5D[ii,yy,jj,:,:] = gridMat4D[ii,yy,:,:]  +  gridMat4D[ii,yy,:,:] * temporalFactorUF['Peso'].reset_index().iloc[jj].Peso 
            # for jj in range(0,12):
            #     jj=0
            #     utcoffs = np.unique(ltcGrid)
            #     for utcoff in utcoffs:
            #         utcoff = utcoffs[0]
            #         idx = ltcGrid==utcoff
            #         gridMat4D[ii,jj,idx] =gridMat4D[ii,jj,idx]  + gridMat[idx]*temporalFactorUF['Peso'].iloc[jj]
            #         # gridMat4D[ii,jj,idx]=gridMat4D[ii,jj,idx]+ gridMat[idx]* np.roll(temporalFactorUF['Peso'],
            #         #                                                                          int(utcoff))[jj]            
            
    return gridMat5D

