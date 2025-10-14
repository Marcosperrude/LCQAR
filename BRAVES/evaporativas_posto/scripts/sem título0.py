#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  7 16:43:51 2025

@author: marcosperrude
"""

#%%

import geopandas as gpd
import pandas as pd
import os
from scipy.optimize import curve_fit
import xarray as xr
from tqdm import tqdm   # barra de progresso
import matplotlib.pyplot as plt
import numpy as np 
import geopandas as gpd


#%%

# Caminhos
tablePath = "/home/marcosperrude/Documents/LCQAR/BRAVES/evaporativas_posto"
dataPath = tablePath + '/inputs'
saidaPath = tablePath + "/outputs"
os.makedirs(saidaPath, exist_ok=True)


# 1. Abrir shapefile de municípios (IBGE)
shp_mun = gpd.read_file(
    tablePath + '/inputs/BR_Municipios_2022/BR_Municipios_2022.shp').to_crs("EPSG:4326")
shp_mun['CD_MUN'] = shp_mun['CD_MUN'].astype(int)


# NetCDF de temperatura de 2023 (escala)
temp =  xr.open_mfdataset(dataPath  +'/WRF/T2.nc')["T2"] - 273.15 
temp_xr = xr.DataArray(
    data=temp,
    dims=["time", "y", "x"],
    coords=dict(
        x= temp['XLONG'][0,0,:].to_numpy(),
        y= temp['XLAT'][0,:,0].to_numpy(),
        time =temp["XTIME"].values
    ),
    name="T2"
)

temp_xr = temp_xr.rio.write_crs("epsg:4326", inplace=True)
temp_xr['time'] = temp_xr['time'] - pd.Timedelta(hours=3)
temp_xr = temp_xr.sel(time=slice("2023-01-01", "2023-12-31"))

#%%
# Obter média de temperatura por hora em cada cidade 
# temp_xr = temp_xr.rio.write_crs(a.crs)

temp_monthly = temp_xr.rio.write_crs(shp_mun.crs)

# Loop por município
for _, mun in tqdm(shp_mun.iterrows(), total=len(shp_mun), desc="Processando municípios"):
    # Clipar a cidade no xarray
    temp_clip = temp_monthly.rio.clip([mun.geometry], shp_mun.crs, drop=True, all_touched=True)
    
    # Calcular a média de todos os pixels da cidade
    temp_vals = temp_clip.mean(dim=("x", "y")).values

    # Processar cada data
    for ii, date in enumerate(temp_monthly["time"].values):
        dt = pd.to_datetime(date)

        # Criar DataFrame de uma linha
        df_row = pd.DataFrame([{
            "CD_MUN": mun['CD_MUN'],
            "year":  dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "TEMP_C": float(temp_vals[ii]),
        }])

       
        filename_csv = f"{saidaPath}/temperatura_csv/temperatura_cidade_{dt.year}_{str(dt.month).zfill(2)}.csv"
        if not os.path.exists(filename_csv):
            df_row.to_csv(filename_csv, index=False, mode='w')
        else:
            df_row.to_csv(filename_csv, index=False, mode='a', header=False)