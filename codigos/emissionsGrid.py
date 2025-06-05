# -*- coding: utf-8 -*-
"""
Created on Mon May 19 10:24:58 2025
    Calcula emissões por célula de grid com base em geometrias e poluentes fornecidos.

    Parâmetros:
    - geo_df: GeoDataFrame com colunas dos poluentes e geometria
    - grid: GeoDataFrame representando o grid
    - poluent: lista com nomes das colunas de poluentes

    Retorna:
    - GeoDataFrame do grid com colunas de emissões adicionadas

@author: Marcos Henrique Perrude
"""

import geopandas as gpd

def EmssionsGrid(geo_df, gridGerado, poluentes):
    # poluentes = poluentesWoodCoal
    # geo_df = emissoes
    emiGrid = gridGerado.copy()
    intersec = gpd.sjoin( geo_df,gridGerado, how='inner', predicate='intersects')
    
    for n in poluentes:
        emiGrid[n]= 0.0
        
    intersec = intersec.merge(
        gridGerado[['geometry']],
        left_on='index_right', right_index=True, suffixes=('', '_right')
    )
    
    intersec_area = intersec.geometry.intersection(intersec['geometry_right']).area
    peso = intersec_area / intersec.geometry.area

    for pol in poluentes:
        # Calcula o valor ponderado de emissões diretamente
        ponderado = intersec[pol] * peso
        soma_ponderada = ponderado.groupby(intersec["index_right"]).sum()
        emiGrid.loc[soma_ponderada.index, pol] = soma_ponderada.values.astype(float)
        
    return emiGrid  
# stá gerando certo
# import matplotlib.pyplot as plt

# index_cell = 10

# fig, ax = plt.subplots(figsize=(8, 8))

# # Plota o grid inteiro como referência (em cinza claro)
# gridGerado.boundary.plot(ax=ax, color='lightgrey', linewidth=0.5)

# # Plota os setores originais
# geo_df.boundary.plot(ax=ax, color='black', linewidth=0.5, alpha=0.3)

# # Plota a célula do grid selecionada em vermelho
# gpd.GeoSeries(gridGerado.loc[index_cell, 'geometry']).boundary.plot(
#     ax=ax, color='red', linewidth=2, label='Célula do Grid'
# )

# # Plota os setores que intersectaram com essa célula em azul
# intersec[intersec['index_right'] == index_cell].geometry.boundary.plot(
#     ax=ax, color='blue', linewidth=1, label='Setores Intersectados'
# )

# plt.title(f'Interseção da Célula {index_cell} com Setores')
# plt.xlabel('Longitude')
# plt.ylabel('Latitude')

# plt.legend(['Grid', 'Setores', 'Célula do Grid', 'Setores Intersectados'])
# plt.show()




