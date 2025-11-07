# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 13:33:31 2025

@author: Marcos.Perrude
"""

import geopandas as gpd
import numpy as np
from shapely.geometry import box

def CreateGrid(Tam_pixel, minx, maxx, miny, maxy, decimal_places=6):
    
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)

    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    
    gridGerado = gpd.GeoDataFrame(geometry=grid_cells, crs='EPSG:4674')
    
    # Extraindo coordenadas do centroide COM ARREDONDAMENTO
    gridGerado['lon'] = gridGerado.geometry.centroid.x.round(decimal_places)
    gridGerado['lat'] = gridGerado.geometry.centroid.y.round(decimal_places)
    
    # Garantir que não há duplicatas devido ao arredondamento
    gridGerado = gridGerado.drop_duplicates(subset=['lat', 'lon'])
    
    # Determinando as coordenadas de cada célula 
    xx, yy = np.meshgrid(np.sort(np.unique(gridGerado.lon)),
                         np.sort(np.unique(gridGerado.lat)))
    
    return gridGerado, xx, yy