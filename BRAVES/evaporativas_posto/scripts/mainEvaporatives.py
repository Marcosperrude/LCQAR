#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 15:31:29 2025

Módulo de Cálculo de Emissões Evaporativas em Postos de Combustível no Brasil
-----------------------------------------------------------------------------

Este módulo foi desenvolvido para realizar a estimativa completa das emissões 
evaporativas associadas ao consumo de combustíveis automotivos por município, 
incluindo os mecanismos de:

    • Emissões durante o abastecimento de veículos (car refueling)
    • Emissões por descarregamento/enchimento de tanques subterrâneos (submerged filling)
    • Emissões por respiração dos tanques de armazenamento (tank breathing)

Autor: Marcos Perrude  
Data: 09 de outubro de 2025
"""
#%%
import geopandas as gpd
import pandas as pd
import os
import xarray as xr
from tqdm import tqdm   # barra de progresso
import matplotlib.pyplot as plt
import numpy as np 
from functionsEmissionCity import carregar_vkt_city ,processar_combustivel
from functionsEmissionFactors import carRefuelingEF, rvp
#%% Caminhos 

tablePath = "/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto"
dataPath = tablePath + '/inputs'
outPath = tablePath + "/outputs"

# 1. Abrir shapefile de municípios (IBGE)
shp_mun = gpd.read_file(
    dataPath + '/BR_Municipios_2022/BR_Municipios_2022.shp').to_crs("EPSG:4326")
shp_mun['CD_MUN'] = shp_mun['CD_MUN'].astype(int)

# Densidade de VOC de acordo com a tempratura
voc_density = pd.read_csv(tablePath + "/VOC_density.csv")
# Conversão de Kg/L para g/L
voc_density[['VOC_gaso_dens', 'VOC_eth_dens']] = voc_density[
    ['VOC_gaso_dens', 'VOC_eth_dens']]  * 1000


# Curva de pressão de vapor da combustivel em relação a % de etanol
# Extrai os dados de RVP do gráfico deste artigo
# RVP = https://d35t1syewk4d42.cloudfront.net/file/1410/RVP-Effects-Memo_03_26_12_Final.pdf
rvpCurve = pd.read_csv(tablePath + "/RVP.csv")

# VKT horario por cada cidade para a desagregação de consumo de combsutivel
# desg_consumo = pd.read_parquet(dataPath + '/2023-01-01_00h-2023-01-31_23h.parquet')
# Lendo os arquivos de desagregaç
desagPath = os.path.join(dataPath, 'desagregacao_vkt')

# Celulas de localização (id_cell)
shp_cells = gpd.read_file(dataPath + '/2025-09-22_mesh_BR_GArBR.gpkg')
shp_cells['fid'] = shp_cells.index
# shp_cells['fid'] = shp_cells.index - 1

# Postos do brasil cadastrados no banco de dados do IBAMA
postos_ibama = pd.read_csv(dataPath + '/postos.csv')

# Postos do brasil cadastrados no banco de dados do Agencia Nacional de Petróleo (anp)
postos_anp = pd.read_csv(
    dataPath + '/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv', sep=';')

# Temperatura media horária de cada cidade (obtivo pelo codigo AnalysisTemp)
tempPath = os.path.join(outPath, 'temperatura_csv','temp_media_cidade')

# Desagregação de uso de combustivel para o uso em transportes (BEN)
ConBen = pd.read_excel(dataPath + '/ConsumoCombustiveTransporte_BEN.xlsx')

#%% Emissoes de evaporativas de combustivel por cidade


# Definir parãmetros dde analise
combustiveis = {
    "GASOLINA C": {"ethanolPerc": 27,
                   'voc_density': 'VOC_gaso_dens',
                   'desag' : "Porcentagem Gasolina"},
    "AEHC": {"ethanolPerc": 93,
             'voc_density': 'VOC_eth_dens',
             'desag' : "Porcentagem Etanol"}
}

cidades_sem_combustivel = []
cidades_sem_vkt = []

for ano in [2021]:
    # ano = 2021
    # Importação das planilhas
    # Revisao de Censo e Contagem de 2007
    # tendo entre 2006 e 2007 uma grande mudnaça nos codigos de cidades
    # Consumo de combustivel em Litros
    # sheet_volume = pd.read_excel(
    #     tablePath + "/SIC 48003009498202458.xlsx", skiprows=4, sheet_name=f'{ano}'
    # )
    
    # Consumo de combustivel em m³
    sheet_volume = pd.read_excel(
        dataPath + "/SIC 48003009498202458_2006-a-2023.xlsx", skiprows=4, sheet_name=f'{ano}'
    )
    
    # Mapear cidades que possuem consumo
    cidades = sheet_volume["COD_LOCALIDADE_IBGE_D"].unique().astype(int)
    
    # Mapear as colunas com consumo
    # colunas_meses = [c for c in sheet_volume.columns if str(c).startswith("20")]
    colunas_meses = [202101]

    #Loop para os meses
    for mes in colunas_meses:
        
        # Carregar e concatenar dados de vkt do Brasil para o mes
        mes_num = int(str(mes)[-2:])
        arquivos_parquet = [
            os.path.join(desagPath, f'{ano}_vkt_proportion',f)
            for f in os.listdir(os.path.join(desagPath, f'{ano}_vkt_proportion'))
            if f.startswith(f"{ano}-{mes_num:02d}") and f.endswith(".parquet")
            ]
        desg_consumo = pd.concat((pd.read_parquet(f) for f in arquivos_parquet))
        
        # Carregar dados de temperatura horárias do mes (Obtivo pelo 'analysisTemp')
        temp = pd.read_csv(tempPath + f"/temperatura_cidade_{ano}_{mes_num:02d}.csv")
        temp["datetime"] = pd.to_datetime(temp[["year", "month", "day", "hour"]])
        temp = temp.set_index("datetime")
        
        #Loop para as cidades
        for cidade in tqdm(cidades, desc="Processando cidades"):
            # Exemplo para teste
            # cidade = 5222203
            
            # Filtrar temperatura horaria para a cidade analisada
            df_temp_city = temp[temp["CD_MUN"] == cidade].sort_values("datetime")
           
            # Carregar arquivos de VKT ja corrigido (apenas celulas com postos)
            desg_consumo_city = carregar_vkt_city(postos_ibama, postos_anp, desg_consumo,
                                                      cidade, shp_cells)
            # Se o volume for vazio, pula para outro combustivel
             
            # FIltrar a temperatura média da cidade para o mes analisado
            temp_hour = df_temp_city[df_temp_city["month"] == mes_num].reset_index()
    
            resultados = []
            for comb, props in combustiveis.items():
                try :  
                    
                    # Obter o consumo do combustivel 
                    # consumo_combsutivel = sheet_volume[sheet_volume["NOM_GRUPO_PRODUTO"] == comb]
                    # comb = 'AEHC'

                    # Volume mensal consumido do combustível
                    volume_mensal = sheet_volume[
                        (sheet_volume["COD_LOCALIDADE_IBGE_D"] == cidade) &
                        (sheet_volume["NOM_GRUPO_PRODUTO"] == comb)
                    ][mes]* 1000
                    
                    # Se o volume for vazio, pula para outro combustivel
                    if volume_mensal.empty:
                        cidades_sem_combustivel.append((cidade, mes, comb))
                        continue
                        
                    # Multiplicar pela quantidade de combustivel1
                    volume_mensal = volume_mensal * ConBen.loc[ConBen['Ano'] == ano][props['desag']].values
                    
                            
                    voc_density_comb = voc_density[[props["voc_density"] ,'temp_C']]
                    
                    # Fatores de emissao de reabastecimento horário para cada temperatura (EF) em Mg/L
                    EFCarRefueling_hour = carRefuelingEF(temp_hour["TEMP_C"].values,
                                                         props["ethanolPerc"] , rvpCurve)
                   
                    # Fator de emissao para descarte de combustivel mg/L
                    EFSubmergedFilling = rvp(props["ethanolPerc"], 880, rvpCurve)
                    
                    # Fator de emissao para respiradores de tanque de armazenamento mg/L
                    EFTankBreathing = rvp(props["ethanolPerc"], 120, rvpCurve)
                    
                    # emissão em g/h e L/h
                    df_comb = processar_combustivel(desg_consumo_city, temp_hour, 
                                                    cidade, mes, comb, props, volume_mensal, 
                                                    props["ethanolPerc"], EFCarRefueling_hour, rvpCurve,
                                                    EFSubmergedFilling, EFTankBreathing , voc_density_comb,)
                   
                    
                    # Armazenar os resultados
                    resultados.append(df_comb)
                    
                except Exception as e:
                 print(f"Erro ao processar {comb} na cidade {cidade}: {e}")
            try:
                # Soma as emissões dos dois combustíveis e conversao para g/Hora
                emissoes = pd.concat(resultados, ignore_index=True).groupby(
                    ["city_id", "cell_id","datetime"], as_index=False).first()
                
                # Verificar se existe a pasta 
                   
                
                # Arquivo Final resultado final
                filename_parquet = (
                    f"{outPath}/emissoes_postos/emissoes_{ano}_{str(mes_num).zfill(2)}/"
                    f"emissoes_{ano}_{str(mes_num).zfill(2)}_{cidade}.parquet"
                )
                os.makedirs(os.path.dirname(filename_parquet), exist_ok=True)
                emissoes.to_parquet(filename_parquet, index=False)
            except Exception as e:
                print(f"Erro ao salvar acidade {cidade}: {e}")
    
#%%

florianopolis = pd.read_parquet(outPath + '/emissoes_postos/emissoes_2021_01/emissoes_2021_01_1200013.parquet')



# Garantir que os IDs são compatíveis
shp_cells['cell_id'] = shp_cells['fid'].astype(int)
florianopolis['cell_id'] = florianopolis['cell_id'].astype(int)

## Converter datetime
florianopolis['datetime'] = pd.to_datetime(florianopolis['datetime'])

# Definir resolução do grid (a partir das células)
# Aqui assumimos que as células são quadradas e têm a mesma dimensão
res = shp_cells.geometry.iloc[0].bounds  # xmin, ymin, xmax, ymax
cell_size = res[2] - res[0]

# Criar grid com base nos limites totais das células
xmin, ymin, xmax, ymax = shp_cells.total_bounds

# Criar coordenadas dos pixels (centros das células)
x_coords = np.arange(xmin + cell_size / 2, xmax, cell_size)
y_coords = np.arange(ymin + cell_size / 2, ymax, cell_size)

# Criar arrays vazios para o dataset
times = sorted(florianopolis['datetime'].unique())
emis_array = np.full((len(times), len(y_coords), len(x_coords)), np.nan)

# Criar um mapeamento rápido de cell_id -> índice da célula
cell_index = {cid: i for i, cid in enumerate(shp_cells['cell_id'])}

# Criar uma correspondência entre cada cell_id e suas coordenadas (x, y)
centroids = shp_cells.copy()
centroids['x'] = centroids.geometry.centroid.x
centroids['y'] = centroids.geometry.centroid.y

# Mapear as emissões para o pixel correspondente
for t_i, t in enumerate(times):
    df_t = florianopolis[florianopolis['datetime'] == t]
    for _, row in df_t.iterrows():
        cell = centroids.loc[centroids['cell_id'] == row['cell_id']]
        if not cell.empty:
            cx = cell['x'].values[0]
            cy = cell['y'].values[0]
            # Encontrar índices correspondentes no grid
            ix = np.argmin(np.abs(x_coords - cx))
            iy = np.argmin(np.abs(y_coords - cy))
            emis_array[t_i, iy, ix] = row['VOC_AEHC_93_Porc(g)']

# Criar Dataset xarray
ds = xr.Dataset(
    {
        "VOC_AEHC_93_Porc": (["time", "lat", "lon"], emis_array)
    },
    coords={
        "time": times,
        "lat": y_coords,
        "lon": x_coords
    },
)

ds["VOC_AEHC_93_Porc"].sel(time=ds.time.values[0])

# --- Plot simples ---
plt.figure(figsize=(8, 6))
ds["VOC_AEHC_93_Porc"].sel(time=ds.time.values[0]).plot(cmap="inferno",
    cbar_kwargs={'label': 'Emissão de VOC (kg/h)'}
)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(False)
plt.show()
#%%

# emissao_total = (florianopolis.groupby(['datetime', 'city_id'], group_keys=True)['VOC_AEHC_93_Porc(g)'].sum().reset_index())


# plt.figure(figsize=(12,6))
# plt.plot(emissao_total.datetime, emissao_total["VOC_AEHC_93_Porc(g)"])
# plt.legend()
# plt.show()


