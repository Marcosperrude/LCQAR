# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 13:49:39 2025

@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import os
from shapely.geometry import box


# Diretórios de dados
DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
DataPath = DataDir + '\dados'
setores = DataPath + '\Setores'
br_uf = gpd.read_file(DataPath + '\BR_UF_2023\BR_UF_2023.shp')

# Função para encontrar o shapefile do setor censitário do estado
def encontrar_arquivo(Estado):
    pastas = [filename for filename in os.listdir(setores) if filename.startswith(Estado)]
    dirpasta = os.path.join(setores, pastas[0])
    arquivos = os.listdir(dirpasta)
    shp_files = [f for f in arquivos if f.endswith(".shp")]
    dirarquivo = os.path.join(dirpasta, shp_files[0])
    gdf = gpd.read_file(dirarquivo)
    return gdf

# Vincula geometria ao dataframe de emissões
def vincular_geometria(Combustivel, Estado, emissoes_lenha, emissoes_carvao, emissoes_total):
    gdf = encontrar_arquivo(Estado)
    for df in [emissoes_lenha, emissoes_carvao, emissoes_total]:
        df['CD_SETOR'] = df['CD_SETOR'].astype(str)
    estado_filtrado = gdf[['CD_SETOR', 'geometry']].copy()
    estado_filtrado['CD_SETOR'] = estado_filtrado['CD_SETOR'].astype(str)

    if Combustivel == "Lenha":
        emissoes = pd.merge(emissoes_lenha, estado_filtrado, on='CD_SETOR', how='right')
    elif Combustivel == "Carvao":
        emissoes = pd.merge(emissoes_carvao, estado_filtrado, on='CD_SETOR', how='right')
    elif Combustivel == "All":
        emissoes = pd.merge(emissoes_total, estado_filtrado, on='CD_SETOR', how='right')

    emissoes = gpd.GeoDataFrame(emissoes, geometry='geometry', crs=gdf.crs)
    return emissoes

# Calcula emissão ponderada por célula do grid
def calcular_emissoes_por_pixel(cell, Poluente, emissoes):
    setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
    if setores_intersectados.empty:
        return np.nan
    setores_intersectados["area_total"] = setores_intersectados.geometry.area
    setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
    setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
    return (setores_intersectados[Poluente] * setores_intersectados["peso"]).sum()

# Cria o grid e calcula emissões por célula
def plotar_emissoes(Poluente, Estado, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total):
    emissoes = vincular_geometria(Combustivel, Estado, emissoes_lenha, emissoes_carvao, emissoes_total)
    uf = br_uf.query('SIGLA_UF == @Estado')
    minx, miny, maxx, maxy = uf.total_bounds
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    grid = gpd.GeoDataFrame(geometry=grid_cells, crs=uf.crs)

    valores = []
    for i in grid.index:
        cell = grid.geometry[i]
        valor = calcular_emissoes_por_pixel(cell, Poluente, emissoes)
        valores.append(valor)

    grid[Poluente] = valores
    return grid

# Gera mapa e salva a imagem

def gerar_mapa(Poluente, Estado, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total):
    if Estado == 'BR':
        ufs = list([filename[:2] for filename in os.listdir(setores)])
        brasil = []
        for i in ufs:
            print(f"Processando {i}...")
            cidades = plotar_emissoes(Poluente, i, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total)
            brasil.append(cidades)
        mapa = gpd.GeoDataFrame(pd.concat(brasil, ignore_index=True), crs=brasil[0].crs)
        mapa_fundo = br_uf
    else:
        mapa = plotar_emissoes(Poluente, Estado, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total)
        mapa_fundo = br_uf.query('SIGLA_UF == @Estado')

    os.makedirs(os.path.join(DataDir, 'figuras', Estado, Combustivel), exist_ok=True)

    # Caminho corrigido para salvar os arquivos
    caminho = os.path.join(DataDir, 'figuras', Estado, Combustivel)
    nome_arquivo = f"{Estado}_{Poluente}_pix{Tam_pixel}.png"
    caminho_imagem = os.path.join(caminho, nome_arquivo)

    fig, ax = plt.subplots(figsize=(15, 10))
    mapa_fundo.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)
    mapa.plot(ax=ax, column=Poluente, cmap="jet", alpha=0.75, edgecolor=None, legend=True)
    plt.savefig(caminho_imagem, dpi=300)

