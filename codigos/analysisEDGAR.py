# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 13:39:13 2025

@author: Marcos.Perrude
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import os
import xarray as xr
import cartopy.crs as ccrs
from shapely.geometry import mapping
import matplotlib.cm as cm

DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
DataPath = DataDir + '\dados'
setores = DataPath + '\Setores'

#Dados EDGAR https://edgar.jrc.ec.europa.eu/gallery?release=v81_A
EDGARPath = DataPath + '\\v8.1_FT2022_AP_CO_2022_RCO_emi_nc'
br_uf = gpd.read_file(DataPath + '\BR_UF_2023\BR_UF_2023.shp')
#%%

def analysisEDGAR (Estado):
    os.chdir(EDGARPath)
    prefixed = [f for f in os.listdir(EDGARPath) if f.startswith("v8")]
    xds = xr.open_dataset(prefixed[0])

    # Corrigir nome da variável e criar DataArray com coordenadas nomeadas corretamente
    da0 = xr.DataArray(
        data = xds['emissions'].values,
        dims = ["y", "x"],  # renomeando para as dimensões esperadas por rioxarray
        coords = dict(
            x = xds['lon'].values,
            y = xds['lat'].values,
        ),
        name = "emissions"
    )
    da0.rio.write_crs("EPSG:4326", inplace=True)
    geodf_br = br_uf.copy()
    
    if Estado == 'BR':
        clipped_estado = da0.rio.clip(geodf_br.geometry.apply(mapping),
                                      geodf_br.crs,
                                      all_touched=True )
        ax = plt.axes(projection=ccrs.PlateCarree())
        clipped_estado.plot(
            ax=ax,
            transform=ccrs.PlateCarree(),
            cmap=cm.jet,
            alpha=0.8,
            cbar_kwargs={'label': 'Emissões'}
        )
        
    else:   
        geodf_estado = geodf_br.query("SIGLA_UF == @Estado")
        clipped_estado = da0.rio.clip(geodf_estado.geometry.apply(mapping), 
                                      geodf_estado.crs,
                                      all_touched=True)

        # Plot do estado recortado
        ax = plt.axes(projection=ccrs.PlateCarree())
        clipped_estado.plot(
            ax=ax,
            transform=ccrs.PlateCarree(),
            cmap=cm.jet,
            alpha=0.8,
            cbar_kwargs={'label': 'Emissões'}
        )
    clipped_estado.to_netcdf(f'Clip_{Estado}.nc')
    # Adiciona limites do estado
    geodf_estado.boundary.plot(ax=ax, color='black', linewidth=0.5)
    
    plt.tight_layout()
    plt.title(f'Clip de Emissões - {Estado}')
    plt.show()
    
da0.plot()
