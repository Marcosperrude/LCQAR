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

import numpy as np

def EmssionsGrid(geo_df, gridGerado, poluentes):

    emiGrid = gridGerado.copy()
    for pol in poluentes:
        print(f"Calculando {pol}...")
        valores = []

        for cell in gridGerado.geometry:
            intersec = geo_df[geo_df.intersects(cell)].copy()

            if intersec.empty:
                valores.append(np.nan)
                continue

            intersec["area_total"] = intersec.geometry.area
            intersec["area_intersectada"] = intersec.geometry.intersection(cell).area
            intersec["peso"] = intersec["area_intersectada"] / intersec["area_total"]
            valor_ponderado = (intersec[pol] * intersec["peso"]).sum()
            valores.append(valor_ponderado)

        emiGrid[pol] = valores

    return emiGrid