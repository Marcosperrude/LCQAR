# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 13:33:31 2025

@author: Marcos.Perrude
"""

import geopandas as gpd
import numpy as np
from shapely.geometry import box

def CreateGrid(Tam_pixel,minx,maxx,miny,maxy):
    
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    gridGerado = gpd.GeoDataFrame(geometry=grid_cells, crs='EPSG:4674')
    
    # Extraindo coordenadas do centroide
    gridGerado['lon'] = gridGerado.geometry.centroid.x
    gridGerado['lat'] = gridGerado.geometry.centroid.y
    
    # Determinando as coordenadas de cada célula 
    xx, yy = np.meshgrid(np.sort(np.unique(gridGerado.lon)), #Cria matriz 2d, em ordem crescente dos valores unidos de lonlat
                         np.sort(np.unique(gridGerado.lat)))
    
    return gridGerado, xx, yy
