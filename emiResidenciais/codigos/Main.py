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
import dask
from dask import delayed, compute
from tqdm import tqdm


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

shp_cells = gpd.read_file('/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto/inputs/2025-09-22_mesh_BR_GArBR.gpkg')


# Tam_pixel = 1  # ~1 km
# minx = -53.9   # longitude mínima (oeste)
# maxx = -48.3   # longitude máxima (leste)
# miny = -29.4   # latitude mínima (sul)
# maxy = -25.9   # latitude máxima (norte)
# minx, miny, maxx, maxy = BR_UF.total_bounds
# minx, miny, maxx, maxy = shp_cells.total_bounds

# gridGerado, xx, yy = CreateGrid(Tam_pixel,minx,maxx,miny,maxy)

gridGerado = shp_cells
gridGerado = gridGerado.to_crs(crs='EPSG:4674')

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
                   xx,yy , OutPath)
    
    # ds = temporalDisagg(gridMat7D, poluentesWoodCoal, Combustivel, xx, yy)
    # datasets[Combustivel] = ds

emiCoal = datasets['Carvao'].copy()
emiWood = datasets['Lenha'].copy()
#%%

# Escolha o poluente, ano e mês
import matplotlib.pyplot as plt
import numpy as np

# Escolha o poluente, ano e mês
i_pol = 0     # índice do poluente (0 a 4)
i_ano = 0     # 0=2021, 1=2022, 2=2023
i_mes = 0     # 0=janeiro, 11=dezembro

# Extrai o campo 2D (lat x lon)
emis_2d = gridMat5D[i_pol, i_ano, i_mes, :, :]

# Cria arrays de coordenadas com base em shp_cells
# (certifique-se de que yy e xx estão em meshgrid no mesmo formato)
lats = np.sort(gridGerado['lat'].unique())[::-1]
lons = np.sort(gridGerado['lon'].unique())

# Plot simples
plt.figure(figsize=(8, 6))
plt.pcolormesh(lons, lats, emis_2d, shading='auto')
plt.colorbar(label='Emissão (ton/mês)')
plt.title(f'Poluente {i_pol} | Ano {2021 + i_ano} | Mês {i_mes + 1}')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.show()


#%%
# co = ds['CO']
# plt.figure(figsize=(20, 5))
# co.mean(dim=["lat", "lon"]).plot()
# plt.title("Emissões médias horárias de CO sobre todos os pixels")
# plt.ylabel("Emissão média (kg/h)")
# plt.xlabel("Tempo")
# plt.grid(True)
# plt.show()


#%%

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import numpy as np
import rasterio
from rasterio.plot import show
import rioxarray
from shapely.ops import unary_union

BR_UF_4326 = BR_UF.to_crs("EPSG:4326")  # converte todo o GeoDataFrame
br_uf_union = BR_UF_4326.unary_union
    
for Combustivel, dt in zip(['Lenha','Carvao'], [emiWood, emiCoal]):
    # Combustivel ='Lenha'
    # dt = emiWood
    # dt_co = dt['CO'].sel(time=slice('2000-01-01', None))
    dt_co = co.rename({'lon':'x', 'lat':'y'})
    dt_co = dt_co.rio.write_crs("EPSG:4326", inplace=True)
    # co_clipped = dt_co.rio.clip([br_uf_union], dt_co.rio.crs, drop=True, invert=False)
    fig, ax = plt.subplots(figsize=(8,6))
    a = ax.pcolor(
        dt_co['x'][:],
        dt_co['y'][:],
        np.sum(dt_co.values, axis=0),
        cmap='turbo',
        norm=LogNorm(vmin= 0.0001, vmax=5)
    )

    BR_UF.boundary.plot(ax=ax, linewidth=0.5, color="black")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias Mensais (Ton/mês)')
    # Combustivel_safe = Combustivel.replace(' ', '_')
    # fig.savefig(f"./outputs/figuras/figura_emissao_{Combustivel_safe}_01.png", dpi=1500, bbox_inches='tight')
    plt.show()
#%%
ds_hourly = ds['PM'].groupby('time.hour').mean(dim='time')

import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np

vals = ds_hourly.values.flatten()
vals = vals[np.isfinite(vals)]
vals = vals[vals > 0]
vmin = np.percentile(vals, 5)
vmax = np.percentile(vals, 95)
print(f"vmin = {vmin:.4f}, vmax = {vmax:.4f}")

norm = colors.LogNorm(vmin=vmin, vmax=vmax)

# -----------------------------
# Cria figura base
# -----------------------------
fig, ax = plt.subplots(figsize=(8, 6))
im = ax.pcolormesh(
    ds['lon'], ds['lat'], ds_hourly.isel(hour=0),
    cmap='turbo', norm=norm
)
BR_UF.boundary.plot(ax=ax, linewidth=0.5, color='black')
ax.set_title("Emissões horárias (00h)", fontsize=12)
ax.set_xticks([])
ax.set_yticks([])

cbar = fig.colorbar(im, ax=ax, orientation='vertical', fraction=0.046, pad=0.04)
cbar.set_label('Emissões médias (Ton/hora)')

# -----------------------------
# Função de atualização
# -----------------------------
def update(frame):
    im.set_array(ds_hourly.isel(hour=frame).values.ravel())
    ax.set_title(f"Emissões horárias - {frame:02d}h", fontsize=12)
    return [im]

# -----------------------------
# Criação da animação
# -----------------------------
ani = FuncAnimation(fig, update, frames=24, blit=False, repeat=True)

#%% Analisar 
import rioxarray
import geopandas as gpd
import rioxarray
from rasterio import features



co = ds['CO']
fig, ax = plt.subplots()
a =ax.pcolor(co['lon'][:], co['lat'], np.sum(co, axis=0))  #Média 
BR_UF.boundary.plot(ax=ax)
fig.colorbar(a, ax=ax, orientation='vertical', label='Emissoes Médias (Ton/mês)')



from matplotlib.colors import LogNorm
vmin = max(a[a>0].min(), 1)  # mínimo >0
vmax = np.percentile(a, 99) 

fig, ax = plt.subplots(figsize=(8,6))
# vmin = 0.1   # mínimo para escala log (>0)
# vmax = 2000  # máximo que queremos destacar

a= np.sum(co, axis=0)
# Plot das emissões
a = ax.pcolor(
    co['lon'][:],
    co['lat'][:],
    a.values,
    cmap='turbo',
)

BR_UF.boundary.plot(ax=ax, linewidth=0.5, color="black")

# Desativa ticks e labels dos eixos (escala de graus)
ax.set_xticks([])
ax.set_yticks([])

fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias (Ton/mês)')

plt.show()



#%%

import xarray as xr
import matplotlib.pyplot as plt

# Exemplo: abrindo o arquivo NetCDF (se ainda não estiver carregado)
# co_sc = xr.open_dataset("./outputs/co_sc_santa_catarina.nc")['emissao']  # substitua 'emissao' pelo nome da variável

# Coordenadas de Joinville
lat_joinville = -26.3044
lon_joinville = -48.8485

# Seleciona o pixel mais próximo
pixel_joinville = co.sel(
    lat=lat_joinville, 
    lon=lon_joinville, 
    method='nearest'
)

# Plotar emissões ao longo do tempo
pixel_joinville.plot(marker='o')
plt.title("Emissões em Joinville ao longo do tempo")
plt.ylabel("Emissão")
plt.xlabel("Tempo")
plt.grid(True)
plt.show()



#%%ANALISAR!!!!!!!

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis
glpDf = pd.read_csv(DataPath + '/vendas-anuais-de-glp-por-municipio.csv',encoding ='utf-8')
glpDf = glpDf[glpDf['ANO'] >= 2000]
propEmiCid, butEmiCid, poluentesGLP = emissionEstimateGLP(DataPath, OutPath, glpDf)


# prop_joinvile =  propEmiCid[propEmiCid['MUNICIPIO'] == 'JOINVILLE']
datasetsglp = {}


for Combustivel, dt in zip(['Propano', 'Butano'], [propEmiCid, butEmiCid]):
    print(f"Processando {Combustivel}...")
    
    # Combustivel = 'Propano'
    # dt = propEmiCid
    #O PRPBLEMA ESTA AQUI!!!!
    
    
    gridMat5Dglp = EmissionsPixelsGLP(dt, BR_MUN, gridGerado, poluentesGLP, DataPath, Combustivel , ufs)


    # Desagregação temporal
    # ds = temporalDisagg(gridMat5Dglp, poluentesGLP, Combustivel, xx, yy)
    # datasetsglp[Combustivel] = ds
    
    # Filtrar para os anos 2021,2022,2023
    gridMat7D = GridMat7D(weekdis, hourdis, gridMat5Dglp, poluentesGLP, DataPath, 
                     Combustivel, xx, yy, OutPath)
    
    
emiPro = datasetsglp['Propano'].copy()
emiBut = datasetsglp['Butano'].copy()
emiGLP = datasetsglp['Butano'].copy() + datasetsglp['Propano'].copy()

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
   
            















