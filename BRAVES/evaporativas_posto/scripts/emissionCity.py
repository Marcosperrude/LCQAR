#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  2 15:49:55 2025

@author: marcosperrude
"""
import geopandas as gpd
import re

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

