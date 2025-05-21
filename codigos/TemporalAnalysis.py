# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:58:24 2025

@author: Marcos.Perrude
"""
import pandas as pd
import seaborn as sbn
import matplotlib.pyplot as plt
import numpy as np
import os

DataDir = r"C:\Users\marcos perrude\Documents\LCQAR"
#Pasta dados
DataPath = os.path.join(DataDir,'Inputs')
OutPath = os.path.join(DataDir, 'Outputs')


#%% Ànalise mensal
df = pd.read_csv(DataPath + '\GLP_Vendas_Historico.csv', encoding='latin1')

glp = df[
    (df['Mercado Destinatário'] == 'CONSUMIDOR FINAL') &
    (~df["Código de Embalagem GLP"].isin(["P 190", "A Granel"]))
    ]

glp['Quantidade de Produto(mil ton)'] = glp['Quantidade de Produto(mil ton)'].astype(int)

glp_mes_uf = glp.groupby(['UF Destino', 'Mês'])['Quantidade de Produto(mil ton)'].mean().reset_index()

def calcula_peso(x):
    return x / x.sum()

glp_mes_uf['Peso'] = glp_mes_uf.groupby('UF Destino')['Quantidade de Produto(mil ton)'].transform(calcula_peso)

fatdes = glp_mes_uf.drop("Quantidade de Produto(mil ton)", axis='columns')

uf_codigos = {
    "RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17,
    "MA": 21, "PI": 22, "CE": 23, "RN": 24, "PB": 25, "PE": 26, "AL": 27, "SE": 28, "BA": 29,
    "MG": 31, "ES": 32, "RJ": 33, "SP": 35,
    "PR": 41, "SC": 42, "RS": 43,
    "MS": 50, "MT": 51, "GO": 52, "DF": 53
}
fatdes['CD_UF'] = fatdes["UF Destino"].map(uf_codigos)
fatdes.to_csv(os.path.join(DataPath, 'fatdes.csv'), index = False)

#%% Análise Anual

#consumo em tep
df= pd.read_csv(DataPath + '\\EPE_Consumo_Historico.csv',index_col = [0],  encoding='latin1', )
df= df.replace(',', '', regex=True).astype(float)


df2023 = df["2023"]

fatdesEPE = df.div(df['2023'], axis=0)

fatdesEPE.to_csv(os.path.join(OutPath, 'fatdesEPE.csv'), index = True)






