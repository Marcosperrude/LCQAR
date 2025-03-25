# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 09:57:39 2025

@author: marcos perrude
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib
#%%
#Fonte dos dados: https://www.gov.br/mme/pt-br/assuntos/secretarias/sntep/publicacoes/balanco-energetico-nacional/anteriores/1-sobre-o-ben/nota-tecnica-consumo-de-lenhacv-residencial-final-2021.pdf/view
consumo = {
    'Regiao': ['Norte', 'Centro-Oeste', 'Sul', 'Nordeste', 'Sudeste'],
    'Lenha (Kg/dia/pessoa)': [4.07, 3.83, 3.20, 2.80, 2.65],
    'Carvão (Kg/dia/pessoa)': [1.11, 0.67, 0.97, 1.14, 0.83]
   
}

consumo = pd.DataFrame(consumo)
#%% Criar um data com apenas os dados da população
pop = pd.read_csv(r"C:\Users\marcos perrude\Documents\LCQAR\dados\quantificacao_lenha_carvao\tabela4714\tabela4714.csv", sep=",")
populacao = pop.iloc[3:8, :3]
populacao.reset_index(inplace = True)
populacao = populacao.drop(columns=['index'])
populacao.columns = ['CD_REGIAO', 'Regiao', 'populacao']
#%%
populacao = pd.merge(populacao, consumo, on='Regiao')

#%% Estimar consumo do lenha e carvao no Brasil
populacao['populacao'] = pd.to_numeric(populacao['populacao'], errors='coerce')
populacao["consumo_lenha"] = populacao["Lenha (Kg/dia/pessoa)"] * populacao['populacao']
populacao["consumo_carvao"] = populacao["Carvão (Kg/dia/pessoa)"] * populacao['populacao']
#%% Uilizando o poder calorifico disponibilizado pelo documento da fonte de dados, estimar os fatores de emissao de acordo com o poder calorífico
#Fonte: https://www.epa.gov/system/files/documents/2022-03/c1s6_final_0.pdf
#poder calorífico (lenha == 8 MMBtu/lb, carvao == 14 MMBtu/lb)

fator_emissao = {
    'Poluentes': ['PM', 'PM10', 'NOx', 'SO2', 'CO'],
    'Lenha': [0.40, 0.36, 0.49, 0.025, 0.60],
    'Carvao': [0.40, 0.36, 0.49, 0.025, 0.60]
}
fator_emissao = pd.DataFrame(fator_emissao)
#%%%
fator_emissao['Lenha'] = fator_emissao['Lenha'] * (0.08*2000*0.5)
fator_emissao['Carvao'] = fator_emissao['Carvao'] * (0.14*2000*0.5)
#%%
emissoes_lenha = pd.DataFrame()
emissoes_carvao = pd.DataFrame()
for i,pol in enumerate(fator_emissao['Poluentes']):
    emissoes_lenha[pol] = populacao['consumo_lenha'] * fator_emissao['Lenha'][i]
    emissoes_carvao[pol] = populacao['consumo_carvao'] * fator_emissao['Carvao'][i]
#%%
emissoes_lenha = pd.concat([populacao[['CD_REGIAO', 'Regiao']], emissoes_lenha], axis=1)
emissoes_carvao = pd.concat([populacao[['CD_REGIAO', 'Regiao']], emissoes_carvao], axis=1)
#%%
regioes = gpd.read_file(r"C:\Users\marcos perrude\Downloads\BR_Regioes_2023\BR_Regioes_2023.dbf")
#%%

#fig, axes = plt.subplots(3,2, figsize=(14, 20), sharex=True, sharey=True)
#def emissoes (Combustivel):
  
#    if Combustivel == 'Lenha':
#        for i, pol in enumerate(fator_emissao['Poluentes']):
#             emissoes_lenha['CD_REGIAO'] = emissoes_lenha['CD_REGIAO'].astype(object)
 #            emissoes_lenha_geometry = pd.merge(emissoes_lenha, regioes, on='CD_REGIAO', how='inner')
#             
#             vmin = emissoes_lenha[pol].min()
 #            vmax = emissoes_lenha[pol].max()
   #          emissoes_lenha_geometry.plot(column=pol, cmap='jet', legend=True, alpha=1, vmin=vmin, vmax=vmax)

         
                
          
#%%
emissoes_lenha['CD_REGIAO'] = emissoes_lenha['CD_REGIAO'].astype(object)
emissoes_lenha_geometry = pd.merge(emissoes_lenha, regioes, on='CD_REGIAO', how='inner')     
emissoes_lenha_geometry = gpd.GeoDataFrame(emissoes_lenha_geometry, geometry='geometry') 
emissoes_lenha_geometry.plot(column = 'PM10',cmap = 'jet')












