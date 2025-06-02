# -*- coding: utf-8 -*-
"""
Created on Tue May 27 15:36:03 2025

@author: Marcos.Perrude
"""

import pandas as pd
import os

metSuperficie = pd.read_csv(r"C:\Users\marcos perrude\Downloads\60665000_Vazoes(in).csv", encoding='latin1')
for col in metSuperficie.columns[4:]:
    metSuperficie[col] = pd.to_numeric(metSuperficie[col], errors='coerce')
    
metSuperficie['datetime'] = pd.to_datetime(metSuperficie[['year', 'month', 'day']])
metSuperficie.set_index('datetime', inplace=True)

#%%
import matplotlib.pyplot as plt

colunas_vazoes = [f'Vazao{str(i).zfill(2)}' for i in range(1, 32)]
for coluna in colunas_vazoes:
    metSuperficie[coluna] = pd.to_numeric(metSuperficie[coluna], errors='coerce')

metSuperficie_filtrado = metSuperficie[(metSuperficie.index.year >= 1980) & (metSuperficie.index.year <= 2015)]

plt.figure(figsize=(35, 5))
for mes in range(1, 13):
    dados_mes = metSuperficie_filtrado[metSuperficie_filtrado.index.month == mes]
    for _, row in dados_mes.iterrows():
        datas_mes = pd.date_range(start=row.name.replace(day=1), periods=len(colunas_vazoes), freq='D')
        vazoes_diarias = row[colunas_vazoes].dropna()
        plt.plot(datas_mes[:len(vazoes_diarias)], vazoes_diarias, color='blue', alpha=0.7)

plt.xlim(pd.Timestamp('1980-01-01'), pd.Timestamp('2015-12-31'))
plt.xlabel('Data')
plt.ylabel('Vazão Diária (m³/s)')
plt.title('Vazões Diárias por Mês (1980-2015)')
plt.grid(True)
plt.savefig('vazoes_diarias.png', dpi=300, bbox_inches='tight')
plt.show()
#%%
import numpy as np

vazoes_consolidadas = metSuperficie[colunas_vazoes].values.ravel()
vazoes_consolidadas = vazoes_consolidadas[~np.isnan(vazoes_consolidadas)]

num_bins = 15
hist, bins = np.histogram(vazoes_consolidadas, bins=num_bins)

plt.figure(figsize=(25, 6))
plt.hist(vazoes_consolidadas, bins=num_bins, edgecolor='black', color='blue', alpha=0.7)
plt.xlabel('Vazão Diária (m³/s)')
plt.ylabel('Frequência')
plt.title('Histograma das Vazões Diárias com 15 Classes')
plt.grid(True)
plt.savefig('histograma_vazoes_15_classes.png', dpi=300, bbox_inches='tight')
plt.show()

for i in range(len(hist)):
    print(f"Classe {bins[i]:.2f}-{bins[i+1]:.2f}: {hist[i]} observações")

#%%
metSuperficie['Media'] = pd.to_numeric(metSuperficie['Media'], errors='coerce')
metSuperficie_filtrado = metSuperficie[(metSuperficie.index.year >= 1980) & (metSuperficie.index.year <= 2015)]

metSuperficie_filtrado['year_month'] = metSuperficie_filtrado.index.to_period('M')
media_mensal = metSuperficie_filtrado.groupby('year_month')['Media'].mean().reset_index()
media_mensal['year_month'] = media_mensal['year_month'].dt.to_timestamp()

media_geral = metSuperficie_filtrado['Media'].mean()
meses_abaixo_media = media_mensal[media_mensal['Media'] < media_geral]['year_month'].count()

plt.figure(figsize=(20, 5))
plt.plot(media_mensal['year_month'], media_mensal['Media'], color='blue', label='Média Mensal')
plt.axhline(y=media_geral, color='red', linestyle='--', label=f'Média Geral: {media_geral:.2f} m³/s')
plt.xlabel('Ano')
plt.ylabel('Vazão (m³/s)')
plt.title('Média Mensal de Vazão (1980-2015)')
plt.grid(True)
plt.legend()
plt.savefig('hidrograma_media_mensal_com_contagem.png', dpi=300, bbox_inches='tight')
plt.show()

print(f"Número de meses com vazão abaixo da média geral: {meses_abaixo_media}")
