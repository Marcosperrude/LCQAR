# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 15:28:29 2025

@author: Marcos.Perrude
"""
from EmissionsEstimate import EmissionsEstimate
from EmissionsPixels import EmissionsPixels
import numpy as np
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import os
from CreateGrid import CreateGrid
import geopandas as gpd 

def PixelsXarray(grid,DataPath, estados_intersectados,br_uf):
    fatdes = pd.read_csv(os.path.join(DataPath, 'fatdes.csv'))
    poluentes = ['PM', 'PM10', 'NOx', 'SO2', 'CO']
    
    for idx, row in br_uf.iterrows():
        mask = grid.geometry.intersects(row.geometry)
        grid.loc[mask, 'CD_UF'] = row['CD_UF']

    grid['lon'] = grid.geometry.centroid.x
    grid['lat'] = grid.geometry.centroid.y
    
    gridMat4D = np.zeros((len(poluentes), 12, np.shape(np.unique(grid.lat))[0],
                          np.shape(np.unique(grid.lon))[0]))
    gridMat4D[gridMat4D == 0] = np.nan
    
    # monthdis = np.ones((12, np.shape(np.unique(grid.lat))[0],
    #                       np.shape(np.unique(grid.lon))[0]))
    
    # for ii, row in fatdes[fatdes.UF==Estado].reset_index().iterrows():
    #     print(ii)
    #     monthdis[ii,:,:] = monthdis[ii,:,:]*row.PESO

    for ii, pol in enumerate(poluentes):
        xx, yy = np.meshgrid(np.sort(np.unique(grid.lon)), np.sort(np.unique(grid.lat)))
        gridMat = np.reshape(grid[pol],
                             (np.shape(np.unique(grid.lon))[0],
                              np.shape(np.unique(grid.lat))[0])).transpose()
        gridMat4D[ii, :, :, :] = gridMat

        for jj, row in fatdes.iterrows():
           estado = row['CD_UF'] 
           peso = row['Peso'] 
           filtro = grid['CD_UF'] == estado
           
           # Converter filtro para numpy array
           filtro = filtro.to_numpy()  # Converte para numpy array (não mais pandas Series)
           filtro_expandido = np.broadcast_to(filtro, gridMat4D[ii, jj, :, :].shape[2:])  # Expande para (35, 57)
           filtro_expandido_4D = np.expand_dims(filtro_expandido, axis=(0, 1))  # Expande para (1, 1, 35, 57)
           filtro_expandido_4D = np.broadcast_to(filtro_expandido_4D, gridMat4D[ii, jj, :, :].shape)  # Broadcast para (1995, 12, 35, 57)
           gridMat4D[ii, jj, :, :] = np.where(
                filtro_expandido_4D,  # Máscara booleana expandida
                gridMat4D[ii, jj, :, :] * peso,  # Aplicar o peso
                gridMat4D[ii, jj, :, :]  # Manter o valor original caso o filtro seja falso
            )
    gridMat4D = xr.DataArray(
        data=gridMat4D,
        dims=["poluente", "time", "y", "x"],
        coords=dict(
            var=poluentes,
            lat=(["y", "x"], yy),
            lon=(["y", "x"], xx),
        ),
        attrs=dict(
            description="Residential emissions by pollutant (no time dimension)",
            units="ton/mes",
        ),
    )
   
#%%
fig, ax = plt.subplots(1, 4, figsize=(20, 5))
ax[0].pcolor(gridMat4D[0,0,:,:])
ax[1].pcolor(gridMat4D[0,1,:,:])
ax[2].pcolor(gridMat4D[0,2,:,:])

var.sel(var=poluentes[0], time=var.time[0]).plot(ax=ax[0])
var.sel(var=poluentes[1], time=var.time[0]).plot(ax=ax[1])
var.sel(var=poluentes[2], time=var.time[0]).plot(ax=ax[2])
var.sel(var=poluentes[3], time=var.time[0]).plot(ax=ax[3])


