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

minx = -53.9   # longitude mínima (oeste)
maxx = -48.3   # longitude máxima (leste)
miny = -29.4   # latitude mínima (sul)
maxy = -25.9   # latitude máxima (norte)
# minx, miny, maxx,  rinmaxy = br_uf.total_bounds

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

#%% Analisar 

emiTotal = emiCoal + emiWood
co = emiTotal['CO']


fig, ax = plt.subplots()
a =ax.pcolor(co['lon'][:], co['lat'], np.mean(co, axis=0))  #Média 
BR_UF.boundary.plot(ax=ax)
fig.colorbar(a, ax=ax, orientation='vertical', label='Emissoes Médias (Ton/mês)')

# emiTotal = emiCoal + emiWood

# # Seleciona CO
# co = emiTotal['CO']  # shape: (648, 99, 95)

# # Índices de tempo para 2000 a 2020 (inclusive)
# start_idx = (2000 - 1970) * 12  # 360
# end_idx = (2021 - 1970) * 12    # 612

# # Média do período 2000–2020
# co_media_2000_2020 = np.mean(co[start_idx:end_idx, :, :], axis=0)

# # Plot
# fig, ax = plt.subplots()
# a = ax.pcolor(co['lon'], co['lat'], co_media_2000_2020)
# BR_UF.boundary.plot(ax=ax)
# fig.colorbar(a, ax=ax, orientation='vertical', label='Emissões Médias (Ton/mês) de 2000 a 2020')


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




#%%ANALISAR!!!!!!!

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis
glpDf = pd.read_csv(DataPath + '/vendas-anuais-de-glp-por-municipio.csv',encoding ='utf-8')
glpDf = glpDf[glpDf['ANO'] >= 2000]
propEmiCid, butEmiCid, poluentesGLP = emissionEstimateGLP(DataPath, OutPath, glpDf)

# prop_joinvile =  propEmiCid[propEmiCid['MUNICIPIO'] == 'JOINVILLE']
dt = propEmiCid
datasetsglp = {}

for Combustivel, dt in zip(['Propano', 'Butano'], [propEmiCid, butEmiCid]):
    print(f"Processando {Combustivel}...")

    gridMat5Dglp = EmissionsPixelsGLP(dt, BR_MUN, gridGerado, poluentesGLP, DataPath, Combustivel , ufs)

    # Desagregação temporal
    ds = temporalDisagg(gridMat5Dglp, poluentesGLP, Combustivel, xx, yy)
    datasetsglp[Combustivel] = ds

emiPro = datasetsglp['Propano'].copy()
emiBut = datasetsglp['Butano'].copy()


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



emiPro['CO'].isel(time=4).plot()
   
            

# gridMat4D = np.zeros((len(poluentesWoodCoal),12, np.shape(np.unique(gridGerado.lat))[0],
#                       np.shape(np.unique(gridGerado.lon))[0]))

# for ii in (propEmiCid, butEmiCid):
#     emiGrid = EmissionsPixelsGLP(ii, br_mun, gridGerado)
#     for ii, pol in enumerate(poluentesGLP):
#         gridMat = np.reshape(emiGrid[pol].fillna(0),
#                              (np.shape(np.unique(emiGrid.lon))[0],
#                               np.shape(np.unique(emiGrid.lat))[0])).transpose()
















