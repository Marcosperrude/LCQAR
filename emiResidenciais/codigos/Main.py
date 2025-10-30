# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 14:43:17 2025

@author: Marcos.Perrude
"""
import pandas as pd 
import geopandas as gpd
import numpy as np
from EmissionsEstimateWoodCoal import emissionEstimateWoodCoal
import os
from EmissionsPixels import EmissionsPixelsWoodCoal,GridMat5D , EmissionsPixelsGLP , GridMat7D
from CreateGrid import CreateGrid
import matplotlib.pyplot as plt
import xarray as xr
from temporalDisagg import temporalDisagg
from EmissionsEstimateGLP import emissionEstimateGLP 
from local2UTC import local2UTC


#Pasta do repositório
DataDir = '/home/marcos/Documents/LCQAR/emiResidenciais'

#Pasta dados
DataPath = os.path.join(DataDir,'inputs')

#Pasta de Outputs
OutPath = os.path.join(DataDir, 'outputs')

BR_UF = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))

BR_MUN = gpd.read_file(os.path.join(DataPath, 'BR_Municipios_2022' , 'BR_Municipios_2022.shp'))

#Fonte dos dados : https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html?edicao=41826&t=downloads
setores = os.path.join(DataPath, 'Setores')

weekdis = pd.read_csv(os.path.join(DataPath,'desagregacao_dia_hora' ,'weekdis.csv'))

hourdis = pd.read_csv(os.path.join(DataPath,'desagregacao_dia_hora' , 'hourdis.csv'))
# Estimando as emissões para lenha e carvao por setor

#malha com atributos - setores - csv

#%% Definindo o grid
# Definições da grade

shp_cells = gpd.read_file(DataPath + '/2025-09-22_mesh_BR_GArBR.gpkg')
gridGerado = shp_cells
gridGerado = gridGerado.to_crs(crs='EPSG:4674')

# Tam_pixel = 1  # ~1 km
# minx = -53.9   # longitude mínima (oeste)
# maxx = -48.3   # longitude máxima (leste)
# miny = -29.4   # latitude mínima (sul)
# maxy = -25.9   # latitude máxima (norte)
# minx, miny, maxx, maxy = BR_UF.total_bounds
# minx, miny, maxx, maxy = shp_cells.total_bounds

# gridGerado, xx, yy = CreateGrid(Tam_pixel,minx,maxx,miny,maxy)


# fig, ax = plt.subplots(figsize=(10, 10))
# gridGerado.boundary.plot(ax=ax, color='gray')
# BR_UF.boundary.plot(ax=ax, color='black')

gridGerado['lon'] = gridGerado.geometry.centroid.x.round(6)
gridGerado['lat'] =  gridGerado.geometry.centroid.y.round(6)
gridGerado = gridGerado.drop_duplicates(subset=['lon', 'lat'])

xx, yy = np.meshgrid(np.sort(np.unique(gridGerado.lon)),
                      np.sort(np.unique(gridGerado.lat)))


estados_intersectados = BR_UF[BR_UF.intersects(gridGerado.unary_union)].copy()
ufs = list(estados_intersectados['SIGLA_UF'])


#%% Xarray de emissoes Lenha e Carvão
 

#Calcular as emissões de lenha e carvão (ton)
#Fonte dos dados : https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/csv/
WoodCoalDf = pd.read_csv(os.path.join(DataPath, 'BR_setores_CD2022.csv'))
woodEmission, coalEmission, poluentesWoodCoal = emissionEstimateWoodCoal(
    WoodCoalDf,DataPath,OutPath)

datasets = {}
anos = 3

# lc2utc, tag = local2UTC(xx, yy)
# np.savetxt(DataPath + "/lc2utc.csv", lc2utc, delimiter=",")

lc2utc = np.loadtxt(DataPath + "/lc2utc.csv", delimiter=",")

# lc2utc = lc2utc.T

# Criando loop para lenha e carvao
for Combustivel, dt in zip(['Lenha','Carvao'], [woodEmission, coalEmission ]):
    # uf='SC'
    # Combustivel = 'Lenha'
    # dt  = woodEmission
    # poluentes = poluentesWoodCoal
    
    # Loop para ler cada UF separadamente
    for ii, uf in enumerate(ufs):
        
        #Criando matriz vazia
        if ii == 0:
            # Transformando em uma matriz x por y

            # gridMat5D = np.zeros((len(poluentesWoodCoal),54, 12, np.shape(np.unique(gridGerado.lat))[0],
            #                       np.shape(np.unique(gridGerado.lon))[0]
            #                       ))
            gridMat5D = np.zeros((len(poluentesWoodCoal),anos, 12, np.shape(np.unique(gridGerado.lat))[0],
                                  np.shape(np.unique(gridGerado.lon))[0]
                                  ))
            
        # Colocando emissões de cada estado na grade
        emiGrid = EmissionsPixelsWoodCoal(Combustivel, dt
                               , gridGerado, DataPath, OutPath,uf, BR_UF ,
                               poluentesWoodCoal, setores)

        
        # transforma em matriz e soma as emissões de cada estado
        gridMat5D = GridMat5D(Combustivel, emiGrid, gridMat5D, poluentesWoodCoal, 
                              DataPath, uf, anos)
        
    #Fazendo a desagregação anual e mensal
    gridMat7D = GridMat7D(weekdis,hourdis,gridMat5D,poluentesWoodCoal,DataPath, Combustivel,
                   xx,yy , OutPath , lc2utc)
    
    # ds = temporalDisagg(gridMat7D, poluentesWoodCoal, Combustivel, xx, yy)
    # datasets[Combustivel] = ds

# emiCoal = datasets['Carvao'].copy()
# emiWood = datasets['Lenha'].copy()
#%% Tentando ajustar tempo


# # calcula deslocamento de fuso horário em horas (inteiro)
# offset_hours = np.round(emi.lon / 15).astype(int)
# # converte para timedelta (como array numpy, não como TimedeltaIndex)
# offset_timedelta = pd.to_timedelta(offset_hours.values, unit="h").to_numpy()
# # cria atriz 2D [time, lon] com broadcasting explícito
# local_time = (
#     emi.time.values[:, np.newaxis] + offset_timedelta[np.newaxis, :]
# )
# # adiciona ao dataset como coordenada auxiliar
# emi = emi.assign_coords(local_time=(("time", "lon"), local_time))


# import matplotlib.pyplot as plt

# # define 4 fusos e coordenadas aproximadas de cada pixel
# # escolha uma longitude e uma latitude dentro de cada fuso
# pixels = {
#     -5: {'lon': -75, 'lat': -10},  # Oeste Acre/AM
#     -4: {'lon': -60, 'lat': -10},  # MT/RO
#     -3: {'lon': -45, 'lat': -20},  # Sudeste
#     -2: {'lon': -33, 'lat': -15}   # Litoral
# }

# fig, axes = plt.subplots(2, 2, figsize=(16, 10))
# axes = axes.flatten()

# for i, (fuso, coords) in enumerate(pixels.items()):
#     # encontra índices de lat/lon mais próximos
#     lat_idx = np.argmin(np.abs(emi.lat.values - coords['lat']))
#     lon_idx = np.argmin(np.abs(emi.lon.values - coords['lon']))
    
#     # extrai histórico horário do pixel
#     co_pixel = emi.CO[:, lat_idx, lon_idx]
    
#     # extrai hora local do pixel
#     local_pixel = emi.local_time[:, lon_idx]
    
#     # plota histórico
#     axes[i].plot(local_pixel, co_pixel, marker='.', linestyle='-', label=f'Pixel {coords}')
#     axes[i].set_title(f'Fuso UTC{fuso:+d} - Pixel ({coords["lat"]}, {coords["lon"]})')
#     axes[i].set_xlabel('Hora local')
#     axes[i].set_ylabel('CO [unit]')
#     axes[i].grid(True)
#     axes[i].legend()

# plt.tight_layout()
# plt.show()

# #%%
# # co = ds['CO']
# # plt.figure(figsize=(20, 5))
# # co.mean(dim=["lat", "lon"]).plot()
# # plt.title("Emissões médias horárias de CO sobre todos os pixels")
# # plt.ylabel("Emissão média (kg/h)")
# # plt.xlabel("Tempo")
# # plt.grid(True)
# # plt.show()


# #%%

# import matplotlib.pyplot as plt
# from matplotlib.colors import LogNorm
# import numpy as np
# import rasterio
# from rasterio.plot import show
# import rioxarray
# from shapely.ops import unary_union


# BR_UF_4326 = BR_UF.to_crs("EPSG:4326")  # converte todo o GeoDataFrame
# br_uf_union = BR_UF_4326.unary_union
    
# for Combustivel, dt in zip(['Lenha','Carvao'], [emiWood, emiCoal]):
#     # Combustivel ='Lenha'
#     # dt = emiWood
#     # dt_co = dt['CO'].sel(time=slice('2000-01-01', None))
#     dt_co = co.rename({'lon':'x', 'lat':'y'})
#     dt_co = dt_co.rio.write_crs("EPSG:4326", inplace=True)
#     # co_clipped = dt_co.rio.clip([br_uf_union], dt_co.rio.crs, drop=True, invert=False)
#     fig, ax = plt.subplots(figsize=(8,6))
#     a = ax.pcolor(
#         dt_co['x'][:],
#         dt_co['y'][:],
#         np.sum(dt_co.values, axis=0),
#         cmap='turbo',
#         norm=LogNorm(vmin= 0.0001, vmax=5)
#     )

#     BR_UF.boundary.plot(ax=ax, linewidth=0.5, color="black")
#     ax.set_xticks([])
#     ax.set_yticks([])
#     for spine in ax.spines.values():
#         spine.set_visible(False)
#     fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias Mensais (Ton/mês)')
#     # Combustivel_safe = Combustivel.replace(' ', '_')
#     # fig.savefig(f"./outputs/figuras/figura_emissao_{Combustivel_safe}_01.png", dpi=1500, bbox_inches='tight')
#     plt.show()
# #%%
# emi = emi['PM'].groupby('time.day').mean(dim='time')

# import matplotlib.pyplot as plt
# import matplotlib.colors as colors
# import numpy as np

# vals = ds_hourly.values.flatten()
# vals = vals[np.isfinite(vals)]
# vals = vals[vals > 0]
# vmin = np.percentile(vals, 5)
# vmax = np.percentile(vals, 95)
# print(f"vmin = {vmin:.4f}, vmax = {vmax:.4f}")

# norm = colors.LogNorm(vmin=vmin, vmax=vmax)

# # -----------------------------
# # Cria figura base
# # -----------------------------
# fig, ax = plt.subplots(figsize=(8, 6))
# im = ax.pcolormesh(
#     ds['lon'], ds['lat'], ds_hourly.isel(hour=0),
#     cmap='turbo', norm=norm
# )
# BR_UF.boundary.plot(ax=ax, linewidth=0.5, color='black')
# ax.set_title("Emissões horárias (00h)", fontsize=12)
# ax.set_xticks([])
# ax.set_yticks([])

# cbar = fig.colorbar(im, ax=ax, orientation='vertical', fraction=0.046, pad=0.04)
# cbar.set_label('Emissões médias (Ton/hora)')

# # -----------------------------
# # Função de atualização
# # -----------------------------
# def update(frame):
#     im.set_array(ds_hourly.isel(hour=frame).values.ravel())
#     ax.set_title(f"Emissões horárias - {frame:02d}h", fontsize=12)
#     return [im]

# # -----------------------------
# # Criação da animação
# # -----------------------------
# ani = FuncAnimation(fig, update, frames=24, blit=False, repeat=True)

# #%% Analisar 
# import rioxarray
# import geopandas as gpd
# import rioxarray
# from rasterio import features
#%%

import xarray as xr
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import rioxarray

# --- 1. Dataset global EDGAR ---
ds_global = xr.open_dataset(
    '/home/marcos/Documents/LCQAR/emiResidenciais/inputs/bkl_BUILDINGS_emi_nc/v8.1_FT2022_AP_CO_2021_bkl_BUILDINGS_emi.nc'
)

# --- 2. Shapefile Brasil ---
# (supondo que BR_UF já está carregado)
BR_UF = BR_UF.to_crs("EPSG:4326")

# Escreve CRS no dataset se necessário
if not ds_global.rio.crs:
    ds_global = ds_global.rio.write_crs("EPSG:4326")

# Recorta o dataset ao território brasileiro
ds_global_BR = ds_global.rio.clip(BR_UF.geometry, BR_UF.crs, drop=True)

# --- 3. Soma das emissões residenciais ---
PM_total = None
combustiveis = ["Butano", "Propano", "Lenha", "Carvao"]

for i in combustiveis:
    path = f"/home/marcos/Documents/LCQAR/emiResidenciais/outputs/emissoes/{i}/2021/2021_1.nc"
    ds = xr.open_dataset(path)
    PM = ds['CO']
    PM_total = PM if PM_total is None else PM_total + PM

# --- 4. Prepara o campo total ---
PM_emi = PM_total.sum(dim='time') if 'time' in PM_total.dims else PM_total

# --- 5. Cria figura lado a lado ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
# --- (a) Mapa EDGAR recortado ---
a1 = ds_global_BR['emissions'].isel(time=0)
im1 = axes[0].pcolormesh(
    a1['lon'], a1['lat'], a1.values,
    cmap='turbo'
)
BR_UF.boundary.plot(ax=axes[0], color='black', linewidth=0.5)
axes[0].set_title("EDGAR CO 2021 (Recorte Brasil)")
cbar1 = fig.colorbar(im1, ax=axes[0], orientation='vertical', fraction=0.046, pad=0.04)
cbar1.set_label("Concentração / Emissão (ton/mês)")



# --- (b) Mapa das emissões residenciais ---
a2 = PM_emi
im2 = axes[1].pcolormesh(
    a2['lon'], a2['lat'], a2.values,
    cmap='turbo'
)
BR_UF.boundary.plot(ax=axes[1], color='black', linewidth=0.5)
axes[1].set_title("Emissões Residenciais CO 2021")
cbar2 = fig.colorbar(im2, ax=axes[1], orientation='vertical', fraction=0.046, pad=0.04)
cbar2.set_label("Concentração / Emissão (ton/mês)")

plt.tight_layout()
plt.show()
#%%
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

# --- 5. Cria figura lado a lado ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# --- (a) Mapa EDGAR recortado ---
a1 = ds_global_BR['emissions'].isel(time=0)
im1 = axes[0].pcolormesh(
    a1['lon'], a1['lat'], a1.values,
    cmap='turbo',
    norm=LogNorm(vmin=0.1, vmax=10)
)
BR_UF.boundary.plot(ax=axes[0], color='black', linewidth=0.5)
axes[0].set_title("EDGAR CO 2021 (Recorte Brasil)")
axes[0].set_xlabel("")
axes[0].set_ylabel("")
cbar1 = fig.colorbar(im1, ax=axes[0], orientation='vertical', fraction=0.046, pad=0.04)
cbar1.set_label("Concentração / Emissão (ton/mês)")

# --- (b) Mapa das emissões residenciais ---
a2 = PM_emi
im2 = axes[1].pcolormesh(
    a2['lon'], a2['lat'], a2.values,
    cmap='turbo',
    norm=LogNorm(vmin=0.1, vmax=10)
)
BR_UF.boundary.plot(ax=axes[1], color='black', linewidth=0.5)
axes[1].set_title("Emissões Residenciais CO 2021")
axes[1].set_xlabel("")
axes[1].set_ylabel("")
cbar2 = fig.colorbar(im2, ax=axes[1], orientation='vertical', fraction=0.046, pad=0.04)
cbar2.set_label("Concentração / Emissão (ton/mês)")

plt.tight_layout()
plt.show()
#%%
import numpy as np
import matplotlib.pyplot as plt

# Latitude e longitude de São Paulo
lat_sp, lon_sp = -23.55, -46.63

# Encontra o índice do pixel mais próximo
lat_idx = np.abs(PM_total.lat - lat_sp).argmin().item()
lon_idx = np.abs(PM_total.lon - lon_sp).argmin().item()

# Seleciona a série temporal do pixel
serie_sp = PM_total[:, lat_idx, lon_idx]

# Plota a série temporal em escala linear
plt.figure(figsize=(12, 5))
plt.plot(serie_sp.time, serie_sp.values)
plt.xlabel("Tempo")
plt.ylabel("Emissão de CO (ton/mês)")
plt.title("Série Temporal de Emissões de CO - São Paulo")
plt.grid(True)
plt.show()

#%%ANALISAR!!!!!!!

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis
glpDf = pd.read_csv(DataPath + '/vendas-anuais-de-glp-por-municipio_1.csv',encoding ='utf-8',  sep=';')
glpDf.rename(columns={'CÓDIGO IBGE': 'CODIGO IBGE','MUNICÍPIO': 'MUNICIPIO'}, inplace=True)

glpDf = glpDf[glpDf['ANO'] >= 2000]
glpDf =  glpDf[glpDf['P13'] >= 0 & glpDf['P13']]

# Emissoes propano e butano em ton
propEmiCid, butEmiCid, poluentesGLP = emissionEstimateGLP(DataPath, OutPath, glpDf)


# prop_joinvile =  propEmiCid[propEmiCid['MUNICIPIO'] == 'JOINVILLE']
datasetsglp = {}


for Combustivel, dt in zip(['Propano', 'Butano'], [propEmiCid, butEmiCid]):
    print(f"Processando {Combustivel}...")
    
    # Combustivel = 'Propano'
    # dt = propEmiCid
   
    # Adaptação para rodar apenas 2021,2022 e 2023
    dt = dt[dt['ANO'] >= 2021]
    
    gridMat5Dglp = EmissionsPixelsGLP(dt, BR_MUN, gridGerado, poluentesGLP, DataPath, Combustivel , ufs)


    # Desagregação temporal
    # ds = temporalDisagg(gridMat5Dglp, poluentesGLP, Combustivel, xx, yy)
    # datasetsglp[Combustivel] = ds
    
    # Filtrar para os anos 2021,2022,2023
    gridMat7D = GridMat7D(weekdis, hourdis, gridMat5Dglp, poluentesGLP, DataPath, 
                     Combustivel, xx, yy, OutPath , lc2utc)
    
    
# emiPro = datasetsglp['Propano'].copy()
# emiBut = datasetsglp['Butano'].copy()
# emiGLP = datasetsglp['Butano'].copy() + datasetsglp['Propano'].copy()

#Loop para todos os combustíveis

# datasetsglp = {}


# for Combustivel, dt in zip(['Propano', 'Butano'], [propEmiCid, butEmiCid]):
#     # Combustivel = 'Propano'
#     # dt = propEmiCid

#     # Loop pelos estados intersectados
#     for ii , uf in enumerate(ufs):
#         print(ii)
#         if ii == 0:
#             gridMat5Dglp = np.zeros((len(poluentesGLP),len(glpDf['ANO'].unique()),12,
#                                      np.shape(np.unique(gridGerado.lat))[0],
#                                      np.shape(np.unique(gridGerado.lon))[0]))
#         print(f"Processando UF {uf}")
#         # Para cada ano dentro do dataframe glpDf
#         for jj, ano in enumerate(glpDf['ANO'].unique()):
#             # ano = 2000
#             # Filtra emissões para o combustível, ano e estado (municípios dentro do estado)
            
            
#             emiGridGLP = EmissionsPixelsGLP(
#                 dt[(dt['ANO'] == ano) & (dt['UF'] == uf)],
#                 BR_MUN, gridGerado, poluentesGLP
#             )

#             # 6. Atualiza a matriz 5D desagregando mensalmente
#             gridMat5Dglp = GridMat5D(Combustivel, emiGridGLP, gridMat5Dglp, poluentesGLP, DataPath,
#                                      uf)
                        
#     #Tranformar em dataset
#     ds = temporalDisagg(gridMat5Dglp, poluentesGLP, Combustivel, xx, yy)
#     datasetsglp[Combustivel] = ds



# co_slice = GLPEmission['CO'].isel(time=150)

# # Filtra apenas valores maiores que 0
# co_positive = co_slice.where(co_slice > 0)

# # Plot
# plt.figure(figsize=(8,6))
# co_positive.plot()
# plt.title("CO > 0 no time=150")
# plt.show()
   
            















