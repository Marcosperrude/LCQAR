# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:58:24 2025

@author: Marcos.Perrude
"""
import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
import xarray as xr
import geopandas as gpd
import re 
import os
import rioxarray
from os import listdir

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

#%%


Cams = os.path.join(DataPath, 'CAMS')

arquivos = [os.path.join(Cams, f) for f in listdir(Cams) if f.endswith('.nc')]
ds = xr.open_mfdataset(arquivos, combine='by_coords')

br = gpd.read_file(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_Pais_2022 (1)\BR_Pais_2022.shp")
br = br.to_crs(epsg=4326)
minx, miny, maxx, maxy = br.total_bounds
ds_br = ds.sel(
    latitude=slice(miny, maxy),
    longitude=slice(minx, maxx)
)

# Primeiro, atribuir o CRS corretamente
ds_br = ds_br.rio.write_crs("EPSG:4326")

# Mascara com base no polígono
masked = ds_br.rio.clip(
                    br.geometry,
                    crs="EPSG:4326",
                    all_touched=True
                )

miny = masked.latitude.min().item()
maxy = masked.latitude.max().item()
minx = masked.longitude.min().item()
maxx = masked.longitude.max().item()

FD_res = masked['FD_res']

FH_res_others = masked['FH_res_others']
FH_res_pm10_pm25 = masked['FH_res_pm10_pm25']

FD = FD_res.assign_coords(day=FD_res['time'].dt.day, 
                              month=FD_res['time'].dt.month) 

dayDisagg = FD.groupby(['month', 'day']).mean('time')
dayDisagg = dayDisagg.rename({'latitude':'lat', 'longitude':'lon'})

# fig, ax = plt.subplots(figsize=(10, 8))
# masked.plot(ax=ax)
# br.boundary.plot(ax=ax, color='red', linewidth=1.5)
#%%

soma_mes = dayDisagg.groupby('month').sum('day')
padrao_diario_norm = dayDisagg / soma_mes
sns.set(style="whitegrid")

# Coordenadas das cidades
cidades = {
    "Joinville":  (-26.30, -48.85),
    "São Paulo":  (-23.55, -46.63),
    "Salvador":   (-12.98, -38.48)
}

for nome, (lat, lon) in cidades.items():
    pixel = padrao_diario_norm.sel({'lat': lat, 'lon': lon}, method='nearest').compute()
    df = pixel.to_dataframe(name='valor').reset_index()

    fig, axes = plt.subplots(3, 4, figsize=(16, 10), sharey=True)
    axes = axes.flatten()

    for mes in range(1, 13):
        ax = axes[mes - 1]
        dados_mes = df[df['month'] == mes]
        sns.lineplot(data=dados_mes, x='day', y='valor', ax=ax, color='royalblue')
        ax.set_title(f"Mês {mes:02d}")
        ax.set_xlabel("Dia do mês")
        ax.set_ylabel("Fração diária")

    fig.suptitle(f"Padrão diário normalizado por mês - {nome}", fontsize=16)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

# Coordenadas das cidades
cidades = {
    "Joinville":  (-26.30, -48.85),
    "São Paulo":  (-23.55, -46.63),
    "Salvador":   (-12.98, -38.48)
}

for nome, (lat, lon) in cidades.items():
    pixel = padrao_diario_norm.sel({'lat': lat, 'lon': lon}, method='nearest').compute()
    
    # Soma dos valores diários por mês
    soma_por_mes = pixel.sum(dim='day')
    
    print(f"\n📍 Soma dos valores diários por mês - {nome}")
    for mes in range(1, 13):
        valor = soma_por_mes.sel(month=mes).item()
        print(f"  Mês {mes:02d} → soma = {valor:.6f}")
