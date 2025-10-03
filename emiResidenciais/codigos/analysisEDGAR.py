# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 13:39:13 2025

@author: Marcos.Perrude
"""
# main_edgar.py

import os
import xarray as xr
from shapely.geometry import mapping
from funcoes_edgar import *

DataDir = r"C:\Users\marcos perrude\Documents\ENS5132"
DataPath = os.path.join(DataDir, 'Inputs')
EDGARPath = os.path.join(DataPath, 'bkl_BUILDINGS_emi_nc')
FigPath = os.path.join(DataDir, 'figuras')



#%% Carregar municipios

estado = 'SC'
br_uf, municipio = carregar_limites(DataPath, estado)
SC = analysisEDGAR(EDGARPath, br_uf, estado)

ds = xr.open_dataset(os.path.join(DataPath, "emissoes_totai.nc"))
da = xr.DataArray(
    data=ds['CO'].values,
    dims=["time", "lat", "lon"],
    coords={"time": ds['time'], "lat": ds['lat'], "lon": ds['lon']},
    name="emissions"
).rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=False).rio.write_crs("EPSG:4326", inplace=False)

geodf_estado = br_uf.query("SIGLA_UF == @estado")
da = da.rio.clip(geodf_estado.geometry.apply(mapping), geodf_estado.crs, all_touched=True)

# Plots espaciais e temporais
dataarrays = [da, SC]
titulos = ["Estimado", "EDGAR"]

for data, titulo in zip(dataarrays, titulos):
    plot_mapa_espacial(data, titulo, municipio, estado, FigPath)
    plot_emissions_subplots(data, titulo, estado, FigPath)

# Mann-Kendall
results = apply_mann_kendall(SC)
plot_correct_trend_map(results, estado, DataPath)
