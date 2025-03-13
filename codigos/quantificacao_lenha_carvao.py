# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 09:57:39 2025

@author: marcos perrude
"""

import pandas as pd
import numpy as np
#%%

#Fonte dos dados: https://www.gov.br/mme/pt-br/assuntos/secretarias/sntep/publicacoes/balanco-energetico-nacional/anteriores/1-sobre-o-ben/nota-tecnica-consumo-de-lenhacv-residencial-final-2021.pdf/view
consumo = {
    'Lenha (Kg/dia/pessoa)' : [4.07,3.83,3.20,2.80,2.65],
    'Carvao (Kg/dia/pessoa)' : [1.11, 0.67, 0.97,1.14,0.83],
           }

df = pd.DataFrame(consumo,index = ['Norte','Centro_Oeste', 'Sul', 'Nordeste', 'Sudeste'])

populacao = (17354884,16289538,29937706,54658515,84840113)
df_total = df.multiply(populacao, axis=0)

#%%
fator_lenha = (np.array([0.7, 0.010, 13, 0.9, 12500, 7.5, 0.2]) * 0.5)























