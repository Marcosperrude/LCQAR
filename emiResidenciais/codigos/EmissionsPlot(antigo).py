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


def gerar_mapa(Poluente, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total,grid):
    # Diretórios de dados
    DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
    #Pasta dados
    DataPath = os.path.join(DataDir,'Inputs')
    setores = os.path.join(DataPath, 'Setores')
    br_uf = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))

    # Padroniza CD_SETOR
    for df in [emissoes_lenha, emissoes_carvao, emissoes_total]:
        df['CD_SETOR'] = df['CD_SETOR'].astype(str)

    # Processa os estados dentro do grid
    estados_intersectados = br_uf[br_uf.intersects(grid.unary_union)].copy()
    ufs = list(estados_intersectados['SIGLA_UF'])
    
    setores_brasil = []
    for uf in ufs:
        print(f"Processando {uf}...")
        pasta_uf = [f for f in os.listdir(setores) if f.startswith(uf)][0]
        shapefile_path = os.path.join(setores, pasta_uf)
        shp_file = [f for f in os.listdir(shapefile_path) if f.endswith(".shp")][0]
        gdf_uf = gpd.read_file(os.path.join(shapefile_path, shp_file))
        gdf_uf['CD_SETOR'] = gdf_uf['CD_SETOR'].astype(str)
        if Combustivel == "Lenha":
            emissoes_uf = pd.merge(emissoes_lenha, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
        elif Combustivel == "Carvao":
            emissoes_uf = pd.merge(emissoes_carvao, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
        elif Combustivel == "All":
            emissoes_uf = pd.merge(emissoes_total, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')

        emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry', crs=gdf_uf.crs)
        setores_brasil.append(emissoes_uf)
        
    emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
        # Calcula emissões por célula
    valores = []
    for cell in grid.geometry:
        setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
        if setores_intersectados.empty:
            valores.append(np.nan)
            continue
        setores_intersectados["area_total"] = setores_intersectados.geometry.area
        setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
        setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
        valor = (setores_intersectados[Poluente] * setores_intersectados["peso"]).sum()
        valores.append(valor)
    grid[Poluente] = valores
        
    fig, ax = plt.subplots(figsize=(15, 12))
    br_uf.boundary.plot(ax=ax, edgecolor='black', linewidth=1)
    grid.plot(ax=ax, column=Poluente, cmap="viridis", alpha=0.75, edgecolor=None, legend=True)
    plt.title(f"Distribuição de {Poluente} no Brasil - Grid {Tam_pixel}°", fontsize=14)
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)

    return grid, emissoes
