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
from EmissionsPixels import EmissionsPixelsWoodCoal,GridMat5D , EmissionsPixelsGLP 
from CreateGrid import CreateGrid
import matplotlib.pyplot as plt
import xarray as xr
from temporalDisagg import temporalDisagg
from EmissionsEstimateGLP import emissionEstimateGLP 
import dask
from dask import delayed, compute
from dask.diagnostics import ProgressBar

#Pasta do repositório
DataDir = '/home/marcosperrude/Documents/LCQAR/emiResidenciais'

#Pasta dados
DataPath = os.path.join(DataDir,'inputs')

#Pasta de Outputs
OutPath = os.path.join(DataDir, 'outputs')

BR_UF = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))

BR_MUN = gpd.read_file(os.path.join(DataPath, 'BR_Municipios_2022' , 'BR_Municipios_2022.shp'))

#Fonte dos dados : https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html?edicao=41826&t=downloads
setores = os.path.join(DataPath, 'Setores')

# Estimando as emissões para lenha e carvao por setor

#malha com atributos - setores - csv

#%% Definindo o grid
# Definições da grade
Tam_pixel = 0.1  # ~1 km

# minx = -53.9   # longitude mínima (oeste)
# maxx = -48.3   # longitude máxima (leste)
# miny = -29.4   # latitude mínima (sul)
# maxy = -25.9   # latitude máxima (norte)
minx, miny, maxx, maxy = BR_UF.total_bounds

gridGerado, xx, yy = CreateGrid(Tam_pixel,minx,maxx,miny,maxy)

# fig, ax = plt.subplots(figsize=(10, 10))
# gridGerado.boundary.plot(ax=ax, color='gray')
# BR_UF.boundary.plot(ax=ax, color='black')

estados_intersectados = BR_UF[BR_UF.intersects(gridGerado.unary_union)].copy()
ufs = list(estados_intersectados['SIGLA_UF'])


#%% Xarray de emissoes Lenha e Carvão
 

#Calcular as emissões de lenha e carvão

#Fonte dos dados : https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/csv/
WoodCoalDf = pd.read_csv(os.path.join(DataPath, 'BR_setores_CD2022.csv'))
woodEmission, coalEmission, poluentesWoodCoal = emissionEstimateWoodCoal(
    WoodCoalDf,DataPath,OutPath)

datasets = {}

# Criando loop para lenha e carvao
for Combustivel, dt in zip(['Lenha','Carvao'], [woodEmission, coalEmission ]):
    
    # uf='SC'
    # Combustivel = 'Lenha'
    # dt  = woodEmission
    
    # Loop para ler cada UF separadamente
    for ii, uf in enumerate(ufs):
        
        #Criando matriz vazia
        if ii == 0:
            # Transformando em uma matriz x por y

            gridMat5D = np.zeros((len(poluentesWoodCoal),54, 12, np.shape(np.unique(gridGerado.lat))[0],
                                  np.shape(np.unique(gridGerado.lon))[0]
                                  ))

        # Colocando emissões de cada estado na grade
        emiGrid = EmissionsPixelsWoodCoal(Tam_pixel, Combustivel, dt
                               , gridGerado, DataPath, OutPath,uf, BR_UF ,
                               poluentesWoodCoal, setores)

        
        # transforma em matriz e soma as emissões de cada estado
        gridMat5D = GridMat5D(Combustivel, emiGrid, gridMat5D, poluentesWoodCoal, 
                              DataPath, uf)

    #Fazendo a desagregação anual e mensal
    ds = temporalDisagg(gridMat5D, poluentesWoodCoal, Combustivel, xx, yy)
    datasets[Combustivel] = ds

emiCoal = datasets['Carvao'].copy()
emiWood = datasets['Lenha'].copy()

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
    dt_co = dt['CO'].sel(time=slice('2000-01-01', None))
    dt_co = dt_co.rename({'lon':'x', 'lat':'y'})
    dt_co = dt_co.rio.write_crs("EPSG:4326", inplace=True)
    co_clipped = dt_co.rio.clip([br_uf_union], dt_co.rio.crs, drop=True, invert=False)
    fig, ax = plt.subplots(figsize=(8,6))
    a = ax.pcolor(
        co_clipped['x'][:],
        co_clipped['y'][:],
        np.mean(co_clipped.values, axis=0),
        cmap='turbo',
        norm=LogNorm(vmin=.1, vmax=10)
    )

    BR_UF.boundary.plot(ax=ax, linewidth=0.5, color="black")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias Mensais (Ton/mês)')
    plt.title(f"Emissões de Monóxido de Carbono na Queima de {Combustivel} 0.1")
    
    Combustivel_safe = Combustivel.replace(' ', '_')
    fig.savefig(f"./outputs/figuras/figura_emissao_{Combustivel_safe}_01.png", dpi=1500, bbox_inches='tight')
    plt.show()

#%% Analisar 
import rioxarray
import geopandas as gpd
import rioxarray
from rasterio import features

emiTotal = emiCoal + emiWood
co = emiWood['CO']
GLPEmission = emiPro + emiBut
fig, ax = plt.subplots()
a =ax.pcolor(co['lon'][:], co['lat'], np.mean(co, axis=0))  #Média 
BR_UF.boundary.plot(ax=ax)
fig.colorbar(a, ax=ax, orientation='vertical', label='Emissoes Médias (Ton/mês)')


from matplotlib.colors import LogNorm
vmin = max(media_co_np[media_co_np>0].min(), 1)  # mínimo >0
vmax = np.percentile(media_co_np, 99) 

fig, ax = plt.subplots(figsize=(8,6))
# vmin = 0.1   # mínimo para escala log (>0)
# vmax = 2000  # máximo que queremos destacar

media_co = np.mean(co, axis=0)
media_co_np = media_co.values 

# Plot das emissões
a = ax.pcolor(
    co['lon'][:],
    co['lat'][:],
    media_co_np,
    cmap='turbo',
    norm=LogNorm(vmin=vmin, vmax=vmax)
)

BR_UF.boundary.plot(ax=ax, linewidth=0.5, color="black")

# Desativa ticks e labels dos eixos (escala de graus)
ax.set_xticks([])
ax.set_yticks([])

fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias (Ton/mês)')

plt.show()


# 1. Escrever o CRS
co = co.rio.write_crs("EPSG:4326")

# 2. Definir quais dimensões são espaciais
co = co.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
co_sc = co.rio.clip(a.geometry.values, a.crs)
# Certifique-se de que todos os atributos estão em UTF-8


# Salvar o arquivo
co_sc.to_netcdf("./outputs/co_sc_santa_catarina.nc")



# # Seleciona CO
# co = emiTotal['CO']  # shape: (648, 99, 95)

# Índices de tempo para 2000 a 2020 (inclusive)
start_idx = (2000 - 1970) * 12  # 360
end_idx = (2021 - 1970) * 12    # 612

# Média do período 2000–2020
co_media_2000_2020 = np.mean(co[:, :, :], axis=0)

# Plot
fig, ax = plt.subplots()
a = ax.pcolor(co['lon'], co['lat'], co_media_2000_2020)
BR_UF.boundary.plot(ax=ax)
fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias (Ton/mês) de 2000 a 2020')


# # Soma total de emissões
# emiTotal = emiCoal + emiWood

# # Seleciona o poluente CO
# co = emiTotal['CO']  # shape: (648, lat, lon)

# # Índices para 2000–2020
# start_idx = (2000 - 1970) * 12  # 360
# end_idx = (2021 - 1970) * 12    # 612

# # Recorte do período desejado
# co_2000_2020 = co[start_idx:end_idx, :, :]  # shape: (252, lat, lon)

# # Criar vetor de tempo correspondente (mensal de jan/2000 a dez/2020)
# time = pd.date_range("2000-01-01", periods=252, freq="MS")

# # Atribuir coordenadas de tempo
# co_2000_2020 = co_2000_2020.assign_coords(time=time)

# # Renomear variáveis e adicionar metadados
# co_2000_2020.name = "CO"
# co_2000_2020.attrs = {
#     "units": "g/km²/mês",
#     "description": "Emissões mensais de CO de 2000 a 2020 (lenha + carvão)"
# }

# # Criar Dataset
# ds = xr.Dataset({"CO": co_2000_2020})

# # Salvar como NetCDF
# ds.to_netcdf("emissoes_CO_mensal_2000_2020.nc")

#%%

import xarray as xr
import matplotlib.pyplot as plt

# Exemplo: abrindo o arquivo NetCDF (se ainda não estiver carregado)
# co_sc = xr.open_dataset("./outputs/co_sc_santa_catarina.nc")['emissao']  # substitua 'emissao' pelo nome da variável

# Coordenadas de Joinville
lat_joinville = -26.3044
lon_joinville = -48.8485

# Seleciona o pixel mais próximo
pixel_joinville = dt_co.sel(
    y=lat_joinville, 
    x=lon_joinville, 
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
    ds = temporalDisagg(gridMat5Dglp, poluentesGLP, Combustivel, xx, yy)
    datasetsglp[Combustivel] = ds

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



co_slice = GLPEmission['CO'].isel(time=150)

# Filtra apenas valores maiores que 0
co_positive = co_slice.where(co_slice > 0)

# Plot
plt.figure(figsize=(8,6))
co_positive.plot()
plt.title("CO > 0 no time=150")
plt.show()
   
            















