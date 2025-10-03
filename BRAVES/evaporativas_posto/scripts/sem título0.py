#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 15:31:29 2025

@author: marcosperrude
"""

import pandas as pd
import os
from scipy.optimize import curve_fit

# Caminhos
tablePath = "/home/marcosperrude/Documents/LCQAR/BRAVES/evaporativas_posto"
saidaPath = tablePath + "/outputs"
os.makedirs(saidaPath, exist_ok=True)

# Importação das planilhas
# Revisao de Censo e Contagem de 2007
# tendo entre 2006 e 2007 uma grande mudnaça nos codigos de cidades
sheet_volume = pd.read_excel(
    tablePath + "/SIC 48003009498202458.xlsx", skiprows=4, sheet_name="2006"
)


# última 13 colunas
sheet_volume.iloc[:, -13:] = sheet_volume.iloc[:, -13:] * 1000  # última 13 colunas


voc_density = pd.read_csv(tablePath + "/VOC_density.csv")
rvpCurve = pd.read_csv(tablePath + "/RVP.csv")

#Definir posteriormente
temperatura_mensal = [25, 25, 24, 22, 19, 17, 16, 17, 19, 21, 22, 24]

# -------------------------
# Funções auxiliares (mantidas)
# -------------------------
def func(x, a, b, c, d):
    return a*x**3 + b*x**2 + c*x + d

def carRefuelingEF(tamb_list, ethanolPercentage):
    # ethanolPercentage = 27
    popt, _ = curve_fit(func, rvpCurve['ETHANOL'], rvpCurve['RVP'])
    EF_list = []
    for tamb in tamb_list:

        # Converter temperatura de celsius para Fahrenheit
        tConv = tamb * (9/5) + 32
        
        # Extrai RVP para a % de etanol do combsutivel
        rvpVal = func(ethanolPercentage, *popt)
        
        # Calculo da temperatura de combustivel que sai da bomba (California study)
        # Fonte: https://www.epa.gov/sites/default/files/2020-11/documents/420r20012.pdf
        td = 20.30 + 0.81 * tConv
        
        # Diferença de temperatura entre o tanque e o dispenser
        deltaT = 0.418 * td - 16.6
        
        # Conversão automatica para mg/L (EPA)
        EF = 264.2 * (-5.909 - 0.0949*deltaT + 0.084*td + 0.485*rvpVal)
        
        EF_list.append(EF)
    return EF_list

def rvp(ethanolPercentage, gasolineEmissionServiceEF):
    popt, _ = curve_fit(func, rvpCurve['ETHANOL'], rvpCurve['RVP'])
    rvp_val = func(ethanolPercentage, *popt)
    rvpUsaGasoline = 9.965801227
    return gasolineEmissionServiceEF * (rvp_val / rvpUsaGasoline)


for comb in ('AEHC' , 'GASOLINA C'):
    
    # Extrair os dados do combustivel
    df_comb = sheet_volume[sheet_volume["NOM_GRUPO_PRODUTO"] == comb]
    
    # Identifica os codigos das cidades
    cidades = df_comb["COD_LOCALIDADE_IBGE_D"].unique()
    resultados = []

    # Definir % de etanol dependendo do combustível
    if comb == "GASOLINA C":
        ethanolPerc = 27
    elif comb == "AEHC":
        ethanolPerc = 93

    # Fator de emissão de VOC [mg/L]
    EFCarRefueling_list = carRefuelingEF(temperatura_mensal, ethanolPerc)
    
    # Calculo do dator de emissao de despejo de combustivel BRASILEIRO em tanques de armazenamento
    EFSubmergedFilling = rvp(ethanolPerc, 880)
    EFTankBreathing = rvp(ethanolPerc, 120)

    colunas_meses = [c for c in df_comb.columns if str(c).startswith("20")]
    
    # Loop para s cidades
    for cidade in cidades:
        
        linha = {"Cidade": cidade, "Combustivel": comb}
        volumes = df_comb[df_comb["COD_LOCALIDADE_IBGE_D"] == cidade][colunas_meses].sum().tolist()

        # CarRefueling V x EF
        emissoes_CR = [v * EF for v, EF in zip(volumes, EFCarRefueling_list)]

        # Submerged Filling V x EF
        emissoes_SF = [v * EFSubmergedFilling for v in volumes]

        # Tank Breathing V x EF
        emissoes_TB = [v * EFTankBreathing for v in volumes]

        # Salvar resultados em dicionário
        for i, mes in enumerate(colunas_meses):
            linha[f"{mes}"] = emissoes_CR[i] + emissoes_SF[i] + emissoes_TB[i]
             
        # Resultados em Mg de VOC
        resultados.append(linha)

    # Exporta CSV por combustível
    df_out = pd.DataFrame(resultados)
    df_out.to_csv(f"{saidaPath}/{comb}.csv", index=False, encoding="utf-8-sig")
    print(f"✔ CSV salvo: {saidaPath}/{comb}.csv")
#%%
import geopandas as gpd
br_mun  =   gpd.read_file(tablePath + '/inputs/BR_Municipios_2022/BR_Municipios_2022.shp')

df_out = df_out.rename(columns={"Cidade": "CD_MUN"})
df_out["CD_MUN"] = df_out["CD_MUN"].astype(float).astype(int).astype(str)
br_mun["CD_MUN"] = br_mun["CD_MUN"].astype(str)

gdf_out = df_out.merge(br_mun[["CD_MUN", "geometry"]], on="CD_MUN", how="left")
gdf_out = gpd.GeoDataFrame(gdf_out, geometry="geometry", crs=br_mun.crs)

gdf_out['geometry'].plot() 
    