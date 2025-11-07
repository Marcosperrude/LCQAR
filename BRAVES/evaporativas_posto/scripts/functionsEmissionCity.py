#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  2 15:49:55 2025

@author: marcosperrude
"""
import geopandas as gpd
import re
import os
import pandas as pd
from scipy.optimize import curve_fit
import numpy as np
from functionsEmissionFactors import rvp


# Função para formatar CNPJ apenas se necessário
def format_cnpj(cnpj):
    cnpj_str = str(cnpj)
    # Regex para detectar CNPJ já formatado no padrão
    if re.fullmatch(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", cnpj_str):
        return cnpj_str  # já está formatado
    else:
        cnpj_str = re.sub(r"\D", "", cnpj_str)  # remove tudo que não for dígito
        cnpj_str = cnpj_str.zfill(14)  # garante 14 dígitos
        return f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:]}"

def filtragempostos (postos_anp , postos_ibama): 
    postos_anp_loc  = postos_anp.copy()
    # Formatar CNPJ
    postos_anp_loc['CNPJ'] = postos_anp_loc['CNPJ'].apply(format_cnpj)

    #  Atribuir localização aos postos ANP
    postos_anp_loc = postos_anp_loc.merge(
        postos_ibama[['CNPJ' , 'Latitude' , 'Longitude' ]],
        on =  'CNPJ',
        how = 'left'
        )
    linhas_nan_lat = postos_anp_loc[postos_anp_loc['Latitude'].isna()]
    postos_anp_loc = postos_anp_loc.drop(linhas_nan_lat.index)
    return postos_anp_loc

def filtragemcelulas (postos_ibama , postos_anp , desg_consumo_city , shp_cells):

    postos_anp_loc = filtragempostos(postos_anp, postos_ibama)
    
    # Criação de geodataframe com os pontos de lat e lon dos pontos
    gdf = gpd.GeoDataFrame(
        postos_anp_loc,
        geometry=gpd.points_from_xy(postos_anp_loc["Longitude"], postos_anp_loc["Latitude"]),
        crs=shp_cells.crs 
    )

    # Idenficação das celulas que caem cada posto
    gdf_com_cells = gpd.sjoin(
        gdf, 
        shp_cells[["fid", "geometry"]], 
        how="left", 
        predicate="within"
    )


    desg_consumo_city["cell_id"] = desg_consumo_city["cell_id"].astype(int)
    
    # Filtragem das celulas que apenas caem postos
    desg_filtrado = desg_consumo_city[desg_consumo_city["cell_id"].isin(
        gdf_com_cells["fid"].dropna().unique().astype(int))]


    desg_filtrado['vkt_fraction_corrigido'] = desg_filtrado['vkt_fraction']/ desg_filtrado['vkt_fraction'].sum()
    
    return desg_filtrado

def carregar_vkt_city(postos_ibama,postos_anp, desagPath, ano, mes_num, cidade, shp_cells):
    desagPath_mes = os.path.join(desagPath, f"{ano}_{str(mes_num).zfill(2)}")
    
    arquivos_parquet = [
        os.path.join(desagPath_mes, f)
        for f in os.listdir(desagPath_mes)
        if f.endswith(".parquet")
    ]

    desg_consumo = pd.concat((pd.read_parquet(f) for f in arquivos_parquet))
    desg_consumo_city = desg_consumo[
        (desg_consumo["city_id"] == cidade) &
        (desg_consumo.index.month == mes_num)
    ].sort_index()
    
            
    # Selecionar apenas as células com postos
    desg_consumo_city = filtragemcelulas(postos_ibama, postos_anp, 
                                         desg_consumo_city, shp_cells)
        
    return desg_consumo_city

def processar_combustivel(desg_consumo_city, temp_hour, cidade, mes, comb, props,
                          volume_mensal, ethanolPerc, EFCarRefueling_hour, 
                          rvpCurve , EFSubmergedFilling ,EFTankBreathing):

    # COnsumo de combustivel na hora de acordo com o VKT
    desg_consumo_city["cons_hour"] = float(volume_mensal) * desg_consumo_city["vkt_fraction_corrigido"]

    desg_consumo_city_m = desg_consumo_city.reset_index().rename(columns={"date_range": "datetime"})
    desg_consumo_city_m = desg_consumo_city_m.merge(temp_hour[["datetime", "TEMP_C"]], on="datetime", how="left")

    df_ef = pd.DataFrame({
        "datetime": pd.to_datetime(temp_hour["datetime"]),
        "EF reabastecimento": EFCarRefueling_hour,
    })
    
    # Merge com o fatores de emissao de reabastecimento
    desg_consumo_city_m = desg_consumo_city_m.merge(df_ef, on="datetime", how="left")
    desg_consumo_city_m["datetime"] =  pd.to_datetime(desg_consumo_city_m["datetime"])
    desg_consumo_city_m = desg_consumo_city_m.set_index('datetime')

    # Emissões respiradores
    desg_consumo_city_m["emis_total"] = desg_consumo_city_m["cons_hour"] * EFTankBreathing
    
    # Emissioes reabasticimento em horario comercial (5:00 - 22:00)
    mask_horas = (desg_consumo_city_m.index.hour >= 5) & (desg_consumo_city_m.index.hour <= 23)
    desg_consumo_city_m.loc[mask_horas, "emis_total"] += (
        desg_consumo_city_m.loc[mask_horas, "cons_hour"]
        * desg_consumo_city_m.loc[mask_horas, "EF reabastecimento"]
    )

    # Submerged Filling
    # Calculando consumo semanal de combustivel
    desg_consumo_city_m["semana"] = desg_consumo_city_m.index.to_period("W")
    desg_consumo_city_m["consumo_semanal"] = (
        desg_consumo_city_m.groupby(["cell_id", "semana"])["cons_hour"].transform("sum")
    )
    
    # gerando os dias de descarte de combustivel no tanque
    dias_semana_submerged = np.random.choice(range(7), size=3, replace=False)
    horas_submerged = [np.random.randint(6, 23) for _ in range(3)]
    eventos_submerged = list(zip(dias_semana_submerged, horas_submerged))
    mask_submerged = desg_consumo_city_m.index.to_series().apply(
        lambda dt: (dt.weekday(), dt.hour) in eventos_submerged
    )
    
    # Calculando as emissoes por descarte de combustivel no tanque
    desg_consumo_city_m["emis_submerged"] = 0.0
    desg_consumo_city_m.loc[mask_submerged, "emis_submerged"] = (
        desg_consumo_city_m.loc[mask_submerged, "consumo_semanal"] * EFSubmergedFilling / 3
    )

    desg_consumo_city_m["emis_total"] += desg_consumo_city_m["emis_submerged"]

    return desg_consumo_city_m.reset_index()[["datetime","city_id" ,"cell_id", "emis_total"]]

