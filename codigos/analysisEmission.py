# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 10:56:08 2025

Este script sera utilizado para o tratamento dos dados e quantifcação das emissoes atmosféricas de Fontes Residenciais,
do qual sera utilizado para integrar o inventário nacional de fontes fixas.


@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import os

#Pasta do repositório
DataDir = os.path.dirname(os.getcwd())

#Pasta dados
DataPath = DataDir +'\dados'

#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
dff = pd.read_csv(DataPath + '\BR_setores_CD2022.csv')

#Caminho para os setores censitários do Brasil: https://www.ibge.gov.br/geociencias/downloads-geociencias.html?caminho=organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/shp/UF
setores = DataPath + '\Setores'

#Shape Brasil https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html
br_uf = gpd.read_file(DataPath + '\BR_UF_2023\BR_UF_2023.shp')

#Dados EDGAR https://edgar.jrc.ec.europa.eu/gallery?release=v81_AP&substance=CO&sector=RCO
EDGARPath = DataPath + '\v8.1_FT2022_AP_CO_2022_RCO_emi_nc'
#%%Tratamento dos dados
dff = dff[dff["CD_SIT"] != 9]  #Massas de agua
dff = dff[dff['v0002'] !=0]  #Áreas com 0 domicílios
# Definir a classificação de acordo com o tipo do setor censitário
classificacao = {
    1: "URBANA - Cidade ou vila, área urbanizada",
    2: "URBANA - Cidade ou vila, área não urbanizada",
    3: "URBANA - Área urbana isolada",
    5: "RURAL - Aglomerado rural, isolado, povoado",
    6: "RURAL - Aglomerado rural, isolado, povoado",
    7: "RURAL - Aglomerado rural, isolado, outros aglomerados",
    8: "RURAL - Zona rural exclusive aglomerado rural"
}

dff["Classificacao"] = dff["CD_SIT"].map(classificacao)
 #%% Definindo a quantidade de residencias que utilizam  de acordo com o tipo de combustível e classificação do setor censitário

data_nacional = pd.read_csv(DataPath + '\data_nacional.csv')

#Consumo de lenha (ton/dia) por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
consumo_lenha = {
    'Sul': 0.093,
    'Norte': 0.0913,
    'Centro-Oeste': 0.085,
    'Sudeste': 0.0766,
    'Nordeste': 0.0781,
}

consumo_carvao = {
    'Sul': 0.0351,
    'Norte': 0.028,
    'Centro-Oeste': 0.0167,
    'Sudeste': 0.0415,
    'Nordeste': 0.0354,
}



# Mapear a quantidade de residencias que utilizam lenha
fatores_lenha = data_nacional.set_index("Categoria").loc["Lenha"].to_dict()
fatores_carvao = data_nacional.set_index("Categoria").loc["Carvão"].to_dict()
# Mapear a classificação ao fator correspondente
dff["Fator_Lenha"] = dff["Classificacao"].map(fatores_lenha)
dff["Fator_Carvao"] = dff["Classificacao"].map(fatores_carvao)
#Definindo a quantidade de residenciais que utilizam lenha
dff["Residencias_Ajustadas_lenha"] = dff["v0002"] * dff["Fator_Lenha"]
dff["Residencias_Ajustadas_carvao"] = dff["v0002"] * dff["Fator_Carvao"]

#Consumo total de lenha por setor censitário, de acordo com a classificação

dff['Consumo_lenha[ton/ano]'] = (dff['Residencias_Ajustadas_lenha'] * dff['NM_REGIAO'].map(consumo_lenha)) * 365 #Conversão de ton/dia --> ton/ano
dff['Consumo_Carvao[ton/ano]'] = (dff['Residencias_Ajustadas_carvao'] * dff['NM_REGIAO'].map(consumo_carvao)) * 365 #Conversão de ton/dia --> ton/ano
#%% Estimando Fator de emissao
#Uilizando o poder calorifico disponibilizado pelo documento da fonte de dados, estimar os fatores de emissao de acordo com o poder calorífico
#Fonte: https://www.epa.gov/system/files/documents/2022-03/c1s6_final_0.pdf
#poder calorífico (lenha == 8 Btu/lb (0.08 MMBtu/lb), carvao == 14 Btu/lb (0.14 MMBtu/lb))

#lb/MMBtu
fator_emissao = {
    'Poluentes': ['PM', 'PM10', 'NOx', 'SO2', 'CO'],
    'Lenha': [0.40, 0.36, 0.49, 0.025, 0.60],
    'Carvao': [0.40, 0.36, 0.49, 0.025, 0.60]
}

fator_emissao = pd.DataFrame(fator_emissao)

# Conversao para kg/ton
fator_emissao['Lenha'] = fator_emissao['Lenha'] * (0.08 * 2000 * 0.5) 
fator_emissao['Carvao'] = fator_emissao['Carvao'] * (0.14 * 2000 * 0.5)

#Estimar o fator de emissao, usando o consumo de lenha por setor censitário e salvando em um novo data 
emissoes_lenha = pd.DataFrame()
emissoes_carvao = pd.DataFrame()
emissoes_lenha['CD_SETOR'] = dff['CD_SETOR']
emissoes_carvao['CD_SETOR'] = dff['CD_SETOR']
 # Mantendo a coluna 'CD_SETOR'
for i, pol in enumerate(fator_emissao['Poluentes']):
    
    #emissoes em Kg
    emissoes_lenha[pol] = dff['Consumo_lenha[ton/ano]'] * fator_emissao['Lenha'][i]
    emissoes_carvao[pol] = dff['Consumo_Carvao[ton/ano]'] * fator_emissao['Carvao'][i]
emissoes_total = pd.concat([emissoes_carvao,emissoes_lenha])
emissoes_total = emissoes_total.groupby(['CD_SETOR']).sum().reset_index(False) #Kg/ano