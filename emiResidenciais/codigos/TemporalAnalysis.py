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

DataDir = "home\Documents\LCQAR"
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

# Uso da lenha no domcílio
uso_lenha = pd.read_csv(DataPath + '/uso_lenha_domicilio.csv', encoding = 'utf-8')

uso_lenha = uso_lenha.set_index('Utilizacao')


uso_lenha_norm = uso_lenha.iloc[:, :] / uso_lenha.iloc[:, :].sum(axis=0)

# Categorias
lareiras = [
    'Preparar alimentos para animais',
    'Aquecer a casa'
]

fogoes = [
    'Cozinhar ou preparar alimentos para a familia',
    'Aquecer a agua para o banho',
    'Ferver a agua para beber',
    'Ferver roupas'
]

regioes = uso_lenha_norm.columns[:]

# Soma por categoria com todas as regiões
uso_lenha_agrupado = pd.DataFrame({
    'Lareiras': uso_lenha_norm[uso_lenha_norm.index.isin(lareiras)][regioes].sum(),
    'Fogoes': uso_lenha_norm[uso_lenha_norm.index.isin(fogoes)][regioes].sum()
}).T

uso_lenha_agrupado.to_csv(DataPath + 'uso_lenha_agrupado.csv', index=True)  
#%%
import numpy as np

DataDir = "/home/marcos/Documents/LCQAR/emiResidenciais"
#Pasta dados
DataPath = os.path.join(DataDir,'inputs')
OutPath = os.path.join(DataDir, 'outputs')


ds_global = xr.open_dataset(
    '/home/marcos/Documents/LCQAR/emiResidenciais/inputs/bkl_BUILDINGS_emi_nc/v8.1_FT2022_AP_CO_2021_bkl_BUILDINGS_emi.nc')
ds_global = ds_global.rio.write_crs("EPSG:4326")

BR_UF = gpd.read_file(os.path.join(DataPath, 'BR_UF_2023', 'BR_UF_2023.shp'))
BR_UF = BR_UF.to_crs("EPSG:4326")

ds_global = ds_global.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)
ds_global_BR = ds_global.rio.clip(BR_UF.geometry, BR_UF.crs, drop=True, all_touched = True)

CO_list = []
for i in ["Butano", "Propano", "Lenha", "Carvao"]:
    path = f"/home/marcos/Documents/LCQAR/emiResidenciais/outputs/edgar/emissoes/{i}/2023/2023_1.nc"
    ds = xr.open_dataset(path)
    CO = ds['CO']
    CO_list.append(CO)

emiCO = sum(CO_list)
emiCO = emiCO.rio.write_crs("EPSG:4326", inplace=False)
if emiCO.lat[0] < emiCO.lat[-1]:
    emiCO = emiCO.reindex(lat=list(reversed(emiCO.lat)))


BR_UF = BR_UF.to_crs("EPSG:4326")

emiCO = emiCO.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)

# Recortar
emiCO_br = emiCO.rio.clip(BR_UF.geometry, BR_UF.crs, drop=True, all_touched=True)


emiCO_month = emiCO_br.resample(time="M").sum()

#emi= (emiCO_month - ds_global_BR['emissions'][0])/ds_global_BR['emissions'][0]
emi= emiCO_month - ds_global_BR['emissions'][0]

emi2d = emi.squeeze()

plt.figure(figsize=(10, 6))
im = plt.pcolormesh(
    emi2d.lon, emi2d.lat, emi2d,
    shading='auto',
    vmin= -0.5 , vmax=50, cmap='managua'
)

plt.title("Diferença de emissões (emiCO_month - ds_global_BR)")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.colorbar(im, label="Δ emissões")
plt.tight_layout()
plt.show()
#%%

import numpy as np
import matplotlib.pyplot as plt

lat_min = float(ds_global_BR.lat.min())
lat_max = float(ds_global_BR.lat.max())
lon_min = float(ds_global_BR.lon.min())
lon_max = float(ds_global_BR.lon.max())

emiCO_common = emiCO_month.sel(lat=slice(lat_max, lat_min), lon=slice(lon_min, lon_max))

# Flatten remove dimensões espaciais
x = ds_global_BR['emissions'][0].values.flatten()
y = emiCO_month.values.flatten()

mask = np.isfinite(x) & np.isfinite(y)
x, y = x[mask], y[mask]

plt.hexbin(x, y, gridsize=200, bins='log', cmap='viridis')
plt.xlabel('Global reference (ds_global_BR)')
plt.ylabel('Estimated (emiCO_month)')
plt.title('Relação pixel a pixel entre bases')
plt.colorbar(label='N° de pixels')
plt.show()
#%%
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from scipy import stats


emi_aligned, ref_aligned = xr.align(emiCO_common, ds_global_BR['emissions'][0], join="inner")
emi_flat = emi_aligned.values.flatten()
ref_flat = ref_aligned.values.flatten()
mask = np.isfinite(emi_flat) & np.isfinite(ref_flat)
emi_flat = emi_flat[mask]
ref_flat = ref_flat[mask]



reg = LinearRegression().fit(ref_flat.reshape(-1, 1), emi_flat)
r2 = reg.score(ref_flat.reshape(-1, 1), emi_flat)

plt.figure(figsize=(7, 7))
plt.scatter(ref_flat, emi_flat, alpha=0.2, s=6, color='steelblue', label="Pixels")
x_line = np.linspace(ref_flat.min(), ref_flat.max())
plt.plot(x_line, reg.predict(x_line.reshape(-1, 1)), color='red', lw=2,
         label=f"Regressão linear (R² = {r2:.2f})")

plt.legend()
plt.show()

#%%
stats.spearmanr(ref_flat, emi_flat)



#%%
plt.figure(figsize=(8,4))
plt.hist(diff_rel, bins=100, color='gray', edgecolor='black')
plt.title("Distribuição das diferenças relativas (%)")
plt.xlabel("Diferença percentual")
plt.ylabel("Número de pixels")
plt.grid(True)
plt.tight_layout()
plt.show()
#%%

