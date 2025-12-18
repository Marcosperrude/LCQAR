#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Módulo desenvolvido para processar e extrair séries horárias de temperatura
(T2 – temperatura a 2 metros) a partir de arquivos mensais do modelo WRF
(Weather Research and Forecasting), disponibilizados em formato NetCDF (.nc).

O objetivo principal deste módulo é gerar, para cada município brasileiro,
uma série temporal horária contendo a temperatura média dos pixels do WRF
que cruzam os limites municipais, permitindo sua integração com modelos e
inventários atmosféricos que dependem de temperatura local — como emissões
evaporativas de combustíveis.

Autor: Marcos Perrude
Data: 09 de outubro de 2025

"""
#%%

import geopandas as gpd
import pandas as pd
import os

import xarray as xr
from tqdm import tqdm   # barra de progresso
import glob
#%%

# Caminhos
tablePath = "/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto"
dataPath = tablePath + '/inputs'
saidaPath = tablePath + "/outputs"
os.makedirs(saidaPath, exist_ok=True)


# 1. Abrir shapefile de municípios (IBGE)
shp_mun = gpd.read_file(
    tablePath + '/inputs/BR_Municipios_2022/BR_Municipios_2022.shp').to_crs("EPSG:4326")
shp_mun['CD_MUN'] = shp_mun['CD_MUN'].astype(int)

#%%

# Pasta raiz onde estao os dados mensais do WRF
path_temp = dataPath + "/WRF/temp_month/"

# Lista dos arquivos
arquivos_temp = sorted(glob.glob(os.path.join(path_temp, "*_T2.nc")))

# Loop para cada arquivo
for arq in arquivos_temp:
    
    # Caminho para o arquivo/mes
    nome = os.path.basename(arq)
    
    # Abre o arquivo e transforma Kelvin -> Celsius
    temp = xr.open_dataset(arq)["T2"] - 273.15

    # Criar DataArray com coordenadas
    temp_xr = xr.DataArray(
        data=temp,
        dims=["time", "y", "x"],
        coords=dict(
            x=temp['XLONG'][0, 0, :].values,
            y=temp['XLAT'][0, :, 0].values,
            time=temp["XTIME"].values,
        ),
        name="T2"
    )

    # Define CRS
    temp_xr = temp_xr.rio.write_crs("epsg:4326", inplace=True)
    # Ajuste do timezone
    temp_xr['time'] = temp_xr['time'] - pd.Timedelta(hours=3)

    nome = os.path.basename(arq)
    print(f"\n>>> Lendo arquivo: {nome}")

    # Extrai ano e mês do nome do arquivo (AAAA MM)
    ano  = int(nome[0:4])
    mes  = int(nome[4:6])
    # Define o início e fim do mês automaticamente
    inicio_mes = f"{ano}-{mes:02d}-01"
    fim_mes    = (pd.Timestamp(inicio_mes) + pd.offsets.MonthEnd(1)).strftime("%Y-%m-%d")
    temp_xr = temp_xr.sel(time=slice(inicio_mes, fim_mes))
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
    
           # Salva os dados
            filename_csv = f"{saidaPath}/temperatura_csv/temp_media_cidade/temperatura_cidade_{dt.year}_{str(dt.month).zfill(2)}.csv"
            if not os.path.exists(filename_csv):
                df_row.to_csv(filename_csv, index=False, mode='w')
            else:
                df_row.to_csv(filename_csv, index=False, mode='a', header=False)