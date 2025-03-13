# -*- coding: utf-8 -*-
"""
Created on Thu Mar  6 11:00:27 2025

@author: marcos perrude
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors
# %%

#Fonte dos dados: https://dados.gov.br/dados/conjuntos-dados/vendas-de-derivados-de-petroleo-e-biocombustiveis

df = pd.read_csv(r"C:\Users\marcos perrude\Documents\LCQAR\dados\vendas-anuais-de-glp-por-municipio.csv",encoding ='utf-8')
brasil_municipios = gpd.read_file(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_Municipios_2022\BR_Municipios_2022.dbf")
brasil = gpd.read_file(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_Pais_2022 (1)\BR_Pais_2022.dbf")
#Agrupar colunas iguais caso tenha
df_grouped = df.groupby(['CODIGO IBGE', 'ANO']).agg({
    'GRANDE REGIAO': 'first',
    'UF': 'first',
    'PRODUTO': 'first',
    'MUNICIPIO': 'first',
    'P13': 'sum',
    'OUTROS': 'sum'
}).reset_index()



#Somar as colunas p12 e outros
df_grouped['TOTAL'] = df_grouped['P13'] + df_grouped['OUTROS']


# %%
#Atransformar de kg para m³ e dividir pela proporção de butano e propano (50%/50%)
TOTAL_50 = (df_grouped['TOTAL']/(2 *2.3))

# Definir os fatores de emissão (AP42 em kg/m³) para propano e butano como arrays e aplicar fato de correção
tep = (np.array([0.7, 0.010, 13, 0.9, 12500, 7.5, 0.2]) * 0.12)
teb = (np.array([0.8, 0.09, 15, 0.9, 14300, 8.4, 0.2]) * 0.12)

# Obter as taxas de emissões para propano e butano
Taxa_propano = TOTAL_50.values[:, np.newaxis] * tep
Taxa_butano = TOTAL_50.values[:, np.newaxis] * teb

# Definir taxa total de emissão
taxa_total = Taxa_propano + Taxa_butano

colunas = ['PMtotal(g/s)', 'SO2(g/s)', 'NOx(g/s)', 'N20(g/s)', 'CO2(g/s)', 'CO(g/s)', 'CH4(g/s)']
taxa_total_df = pd.DataFrame(taxa_total, columns=colunas)
# Concatenar os valores
df_quantificado = pd.concat([df_grouped, taxa_total_df], axis=1)

# Converter de kg/ano para g/s
df_quantificado[colunas] = (df_quantificado[colunas] * 1000) / 31536000

#%%

def emissoes(POLUENTE, ANO):
    
    if 2000 <= ANO <= 2022:
        
        # Filtra os dados para o ano especificado
        df_filtrado = df_quantificado.query('ANO == @ANO')
        df_filtrado = df_filtrado[df_filtrado[POLUENTE] != 0]
        
        brasil_municipios['CD_MUN'] = brasil_municipios['CD_MUN'].astype(int)  #Shape com geometrias
        df_filtrado['CODIGO IBGE'] = df_filtrado['CODIGO IBGE'].astype(int)
        df_filtrado.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace = True)
        #Fazer um merged para conseguir as geometrias 
        df_filtrado_geometrias = pd.merge(brasil_municipios, df_filtrado, on='CD_MUN', how='inner')
        df_filtrado_geometrias['CD_MUN'] = df_filtrado_geometrias['CD_MUN'].astype(object)
        
       
        fig, ax = plt.subplots(1, 1, figsize=(20, 12))
        
        brasil.plot(ax=ax, color='lightgrey', edgecolor='black', alpha=0.8)
        
        vmin = df_filtrado_geometrias[POLUENTE].min()
        vmax = df_filtrado_geometrias[POLUENTE].max()

        # Usando LogNorm para representar a escala logarítmica
        norm = matplotlib.colors.LogNorm(vmin=vmin, vmax=vmax)
        df_filtrado_geometrias.plot(column=POLUENTE, cmap='jet', legend=True, ax=ax, alpha=1, norm = norm)

        plt.title(f'Emissão [g/s] de {POLUENTE} por Município em {ANO}')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.show()
    else:
        print('Ano não consta na base de dados')
#%%
# Exemplo de uso
POLUENTE ='CH4(g/s)'
ANO = 2010
emissoes(POLUENTE, ANO)

#%%
anos = [2019, 2020, 2021, 2022]

fig, axes = plt.subplots(2, 2, figsize=(20, 14), sharex=True, sharey=True)

# Convertendo apenas uma vez antes do loop
brasil_municipios['CD_MUN'] = brasil_municipios['CD_MUN'].astype(int)

for i, ANO in enumerate(anos):
    df_filtrado = df_quantificado.query('ANO == @ANO')
    df_filtrado = df_filtrado[df_filtrado['CH4(g/s)'] != 0]
        
    df_filtrado['CODIGO IBGE'] = df_filtrado['CODIGO IBGE'].astype(int)
    df_filtrado.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace=True)
        
    df_filtrado_geometrias = pd.merge(brasil_municipios, df_filtrado, on='CD_MUN', how='inner')
    df_filtrado_geometrias['CD_MUN'] = df_filtrado_geometrias['CD_MUN'].astype(object)
        
    ax = axes[i // 2, i % 2]  # Correção para acessar os subplots corretamente
    brasil.plot(ax=ax, color='darkgray', edgecolor='black', alpha=0.8)
        
    vmin = df_filtrado_geometrias['CH4(g/s)'].min()
    vmax = df_filtrado_geometrias['CH4(g/s)'].max()
    norm = matplotlib.colors.LogNorm(vmin=vmin, vmax=vmax)
        
    df_filtrado_geometrias.plot(column='CH4(g/s)', cmap='jet', legend=True, ax=ax, alpha=1, norm=norm)
        
    ax.set_title(f'Emissão [g/s] de CH4 em {ANO}')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
plt.tight_layout()
plt.savefig(r"C:\Users\marcos perrude\Documents\LCQAR\imagens\quantificacao_glp")
plt.show()






