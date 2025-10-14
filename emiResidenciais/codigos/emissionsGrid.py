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


def EmssionsGrid(geo_df, gridGerado, poluentes, inplace=False):
    
    # geo_df = emissoes_uf
    # geo_df = dados_ano_uf
    # poluentes = poluentesGLP
    
    # poluentes = poluentesWoodCoal
    # geo_df = emissoes_uf
    if not inplace:
        emiGrid = gridGerado.copy()
    
    gridGerado['Area'] = gridGerado.area
    
    intersec = gpd.sjoin( geo_df,gridGerado, how='inner', predicate='intersects')
    
        
    # intersec = intersec.merge(
    #     gridGerado[['geometry']],
    #     left_on='index_right', right_index=True, suffixes=('', '_right')
    # )
    
    # intersec_area = intersec.geometry.intersection(intersec['geometry_right']).area
    
    intersec_area = intersec.geometry.intersection(gridGerado.loc[intersec.index_right], align=False).area
    
    peso = intersec_area / intersec.geometry.area

    # Multiplica todos os poluentes pela série de pesos (broadcasting por linha)
    ponderado = intersec[poluentes].multiply(peso, axis=0)
    
    # Soma ponderada por célula de grid (index_right)
    soma_ponderada = ponderado.groupby(intersec["index_right"]).sum()
    
    if not inplace:
        # Atribui ao grid (linha por índice, colunas por poluentes)
        emiGrid.loc[soma_ponderada.index, poluentes] = soma_ponderada
    
        return emiGrid  
    else:
        gridGerado.loc[soma_ponderada.index, poluentes] = soma_ponderada
        
        return None

# import matplotlib.pyplot as plt

# index_cell = 2

# fig, ax = plt.subplots(figsize=(8, 8))

# # Plota o grid inteiro como referência (em cinza claro)
# gridGerado.boundary.plot(ax=ax, color='lightgrey', linewidth=0.5)

# # Plota os setores originais
# geo_df.boundary.plot(ax=ax, color='black', linewidth=0.5, alpha=0.3)

# # Plota a célula do grid selecionada em vermelho
# gpd.GeoSeries(gridGerado.loc[intersec['index_right'][0], 'geometry']).boundary.plot(
#     ax=ax, color='red', linewidth=2, label='Célula do Grid'
# )

# # Plota os setores que intersectaram com essa célula em azul
# intersec[intersec['index_right'] == intersec['index_right'][0]].geometry.boundary.plot(
#     ax=ax, color='blue', linewidth=1, label='Setores Intersectados'
# )

# plt.title(f'Interseção da Célula {index_cell} com Setores')
# plt.xlabel('Longitude')
# plt.ylabel('Latitude')

# plt.legend(['Grid', 'Setores', 'Célula do Grid', 'Setores Intersectados'])
# plt.show()




