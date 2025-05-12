# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 14:24:15 2025

@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import os
from shapely.geometry import box
from timezonefinder import TimezoneFinder

def EmissionsPixels(Tam_pixel, Combustivel, woodEmission, coalEmission, 
                       totalEmission, gridGerado, DataPath, OutPath, uf, br_uf):
    # Diretórios de dados
    poluentes = ['PM', 'PM10', 'NOx', 'SO2', 'CO']
    setores = os.path.join(DataPath, 'Setores')

    # Padroniza CD_SETOR
    for df in [woodEmission, coalEmission, totalEmission]:
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
    elif Combustivel == "All":
        emissoes_uf = pd.merge(totalEmission, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')

    emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry', crs=gdf_uf.crs)
    setores_brasil.append(emissoes_uf)
        
    emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
        # Calcula emissões por célula
        
    emiGrid = gridGerado.copy()
    for pol in poluentes:
        print(f"Calculando {pol}...")
        valores = []
        for cell in gridGerado.geometry:
            setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
            if setores_intersectados.empty:
                valores.append(np.nan)
                continue
            setores_intersectados["area_total"] = setores_intersectados.geometry.area
            setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
            setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
            valor = (setores_intersectados[pol] * setores_intersectados["peso"]).sum()
            valores.append(valor)  

        emiGrid[pol] = valores
        
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


def geoGrid2mat(emiGrid,gridMat4D,poluentes,uf,ltcGrid,DataPath):
    
    uf = 'SC'
    temporalFactor = pd.read_csv(DataPath + '\\fatdes.csv')
    
    #Localiza a desagregação temporal no csv de acordo com UF
    temporalFactorUF = temporalFactor[temporalFactor['UF']==uf]
    
    # Loop para cara poluente
    for ii, pol in enumerate(poluentes):
        pol = 'PM'
        gridMat = np.reshape(emiGrid[pol].fillna(0),
                             (np.shape(np.unique(emiGrid.lon))[0],
                              np.shape(np.unique(emiGrid.lat))[0])).transpose()
     # apara cad apoluente e mes, agrupar pelo fuso horário
    for jj in range(0,12):
        jj= 1
        for ii, pol in enumerate (poluentes):
            utcoffs = np.unique(ltcGrid)
            for utcoff in utcoffs:
                idx = ltcGrid==utcoff
                gridMat4D[ii,jj,idx]=gridMat4D[ii,jj,idx]+ gridMat[idx]* np.roll(temporalFactorUF['Peso'],
                                                                                     int(utcoff))[jj]
                
    return gridMat4D

