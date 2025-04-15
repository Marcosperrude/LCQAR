# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 14:43:17 2025

@author: Marcos.Perrude
"""
import pandas as pd 
from analysisEmission import analysisEmission
from PlotEmission import gerar_mapa
#Pasta do repositório
DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
#Pasta dados
DataPath = DataDir +'\dados'

#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
dff = pd.read_csv(DataPath + '\BR_setores_CD2022.csv')
emissoes_lenha, emissoes_carvao, emissoes_total = analysisEmission(dff)
#Dados EDGAR https://edgar.jrc.ec.europa.eu/gallery?release=v81_A
#EDGARPath = DataPath + '\v8.1_FT2022_AP_CO_2022_RCO_emi_nc'
#%%
Combustivel = "Lenha"
Tam_pixel = 0.1  # 0.1 Equivale a ~1km se o CRS for metros
Poluente = 'CO'
Estado = 'SC'
gerar_mapa(Poluente, Estado, Tam_pixel, Combustivel, emissoes_lenha, emissoes_carvao, emissoes_total)