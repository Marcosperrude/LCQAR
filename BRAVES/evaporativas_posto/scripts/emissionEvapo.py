#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 15:31:29 2025

@author: marcosperrude
"""



import geopandas as gpd
import pandas as pd
import os
import xarray as xr
from tqdm import tqdm   # barra de progresso
import matplotlib.pyplot as plt
import numpy as np 
from emissionCity import carregar_vkt_city ,processar_combustivel
from emissionFactors import carRefuelingEF, rvp

# Caminhos
tablePath = "/home/marcos/Documents/LCQAR/BRAVES/evaporativas_posto"
dataPath = tablePath + '/inputs'
outPath = tablePath + "/outputs"


# 1. Abrir shapefile de municípios (IBGE)
shp_mun = gpd.read_file(
    dataPath + '/BR_Municipios_2022/BR_Municipios_2022.shp').to_crs("EPSG:4326")
shp_mun['CD_MUN'] = shp_mun['CD_MUN'].astype(int)

# NetCDF de temperatura de 2023 (escala) do brasil
# (usado para obter a temp horaria de cada cidade)
# temp =  xr.open_mfdataset(dataPath  +'/WRF/T2.nc')["T2"] - 273.15 
# temp_xr = xr.DataArray(
#     data=temp,
#     dims=["time", "y", "x"],
#     coords=dict(
#         x= temp['XLONG'][0,0,:].to_numpy(),
#         y= temp['XLAT'][0,:,0].to_numpy(),
#         time =temp["XTIME"].values
#     ),
#     name="T2"
# )
# temp_xr = temp_xr.rio.write_crs("epsg:4326", inplace=True)
# temp_xr['time'] = temp_xr['time'] - pd.Timedelta(hours=3)
# temp_xr = temp_xr.sel(time=slice("2023-01-01", "2023-12-31"))


# Importação das planilhas
# Revisao de Censo e Contagem de 2007
# tendo entre 2006 e 2007 uma grande mudnaça nos codigos de cidades
# Consumo de combustivel em Litros
sheet_volume = pd.read_excel(
    tablePath + "/SIC 48003009498202458.xlsx", skiprows=4, sheet_name="2023"
)

# Densidade de VOC de acordo com a tempratura
voc_density = pd.read_csv(tablePath + "/VOC_density.csv")
# Conversão de Kg/L para g/L
voc_density[['VOC_gaso_dens', 'VOC_eth_dens']] = voc_density[
    ['VOC_gaso_dens', 'VOC_eth_dens']]  * 1000


# Curva de pressão de vapor da combustivel em relação a % de etanol
# Extrai os dados de RVP do gráfico deste artigo
# RVP = https://d35t1syewk4d42.cloudfront.net/file/1410/RVP-Effects-Memo_03_26_12_Final.pdf
rvpCurve = pd.read_csv(tablePath + "/RVP.csv")

# VKT horario por cada cidade para a desagregação de consumo de combsutivel
desg_consumo = pd.read_parquet(dataPath + '/2023-01-01_00h-2023-01-31_23h.parquet')
# Lendo os arquivos de desagregaç
desagPath = os.path.join(dataPath, 'desagregacao_vkt')

# Celulas de localização (id_cell)
shp_cells = gpd.read_file(dataPath + '/2025-09-22_mesh_BR_GArBR.gpkg')
shp_cells['fid'] = shp_cells.index
# shp_cells['fid'] = shp_cells.index - 1

# Postos do brasil cadastrados no banco de dados do IBAMA
postos_ibama = pd.read_csv(dataPath + '/postos.csv')

# Postos do brasil cadastrados no banco de dados do Agencia Nacional de Petróleo (anp)
postos_anp = pd.read_csv(
    dataPath + '/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv', sep=';')

# Temperatura media horária de cada cidade (obtivo pelo codigo AnalysisTemp)
temp = pd.read_csv(f"{outPath}/temperatura_csv/temperatura_cidade_2023_01.csv")
temp["datetime"] = pd.to_datetime(temp[["year", "month", "day", "hour"]])
temp = temp.set_index("datetime")
#%%


# resultados = []
# for comb in ("AEHC", "GASOLINA C"):(
#     print(f'Rodando {comb} ')
#     # comb = "GASOLINA C"
#     # Extrair os dados do combustivel
#     df_comb = sheet_volume[sheet_volume["NOM_GRUPO_PRODUTO"] == comb]

#     # Identifica os codigos das cidades
#     cidades = df_comb["COD_LOCALIDADE_IBGE_D"].unique()
#     resultados = []

#     # Definir % de etanol dependendo do combustível
#     if comb == "GASOLINA C":
#         ethanolPerc = 27
        
#     elif comb == "AEHC":
#         ethanolPerc = 93

#     # tabela Table 5.2-7, usa-se os fatores de gasolina para o etanol tambem (880,120)
#     EFSubmergedFilling = rvp(ethanolPerc, 880)
#     EFTankBreathing = rvp(ethanolPerc, 120)

#     colunas_meses = [c for c in df_comb.columns if str(c).startswith("20")]

#     for cidade in tqdm(cidades, total=len(cidades), desc="Processando municípios"):
        
#         # cidades = [1100015]
#         linha = {"CD_MUN": cidade, "Combustivel": comb}
#         volumes = df_comb[df_comb["COD_LOCALIDADE_IBGE_D"]
#                           == cidade][colunas_meses].sum().tolist()
        
#         # procurar município no shapefile
#         mun = shp_mun[shp_mun["CD_MUN"] == int(cidade)]
        
        
#         # Usa o DataFrame de temperatura já calculad
#         temp_monthly_mun = df_clima[df_clima[
#            "CD_MUN"] == int(cidade)].sort_values("date")["TEMP_C"].values

        
#        # Calcula EF dependente da temperatura
#         EFCarRefueling_list = carRefuelingEF(temp_monthly_mun, ethanolPerc)
       
#         # extrair temp/pressão mensais
#         # temp_monthly_mun, press_monthly_mun = get_monthly_temp_press(
#         #     temp_monthly, press_monthly, mun)

#         # fator emissão dependente da temperatura

#         # emissões
#         # Reabastecimento
#         emissoes_CR = [v * EF for v, EF in zip(volumes, EFCarRefueling_list)]
        
#         # Despejo de combustivel
#         emissoes_SF = [v * EFSubmergedFilling for v in volumes]
#         emissoes_TB = [v * EFTankBreathing for v in volumes]

#         for i, mes in enumerate(colunas_meses):
#             volumes = volumes[i]/(30*24)
#             if comb == "GASOLINA C":
#                 densidade_VOC = voc_density.loc[voc_density[
#                     'temp_C'] == round(temp_monthly_mun[i]), 'VOC_gaso_dens'].values[0]
#             elif comb == "AEHC":
#                 densidade_VOC = voc_density.loc[voc_density[
#                     'temp_C'] == round(temp_monthly_mun[i]), 'VOC_eth_dens'].values[0]

#             linha[mes] = (emissoes_CR[i] + emissoes_SF[i] + \
#                           emissoes_TB[i])/densidade_VOC

#         resultados.append(linha)

#     df_out = pd.DataFrame(resultados)
    

#     df_out.to_csv(f"{outpath}/{comb}.csv", index=False, encoding="utf-8-sig")
#     print(f"✔ CSV salvo: {outpath}/{comb}.csv")


#%% Emissoes de evaporativas de combustivel por cidade

# Mapear cidades que possuem consumo
cidades = sheet_volume["COD_LOCALIDADE_IBGE_D"].unique().astype(int)

# Definir parãmetros dde analise
combustiveis = {
    "GASOLINA C": {"ethanolPerc": 27},
    "AEHC": {"ethanolPerc": 93}
}


for cidade in tqdm(cidades, desc="Processando cidades"):
    # Exemplo para teste
    # cidade = 3550308

    # FIltrar temperatura horaria para a cidade analisada
    df_temp_city = temp[temp["CD_MUN"] == cidade].sort_values("datetime")
    
    # colunas_meses = [c for c in df_comb.columns if str(c).startswith("20")]
    colunas_meses = [202301]
    
    # Loop para todos os meses com consumo
    for mes in colunas_meses:
        ano = int(str(mes)[:4])
        mes_num = int(str(mes)[-2:])
        
        # Carregar arquivos de VKT ja corrigido (apenas celulas com postos)
        desagPath_mes = os.path.join(desagPath, f'{ano}_{str(mes_num).zfill(2)}')
        desg_consumo_city = carregar_vkt_city(postos_ibama, postos_anp, desagPath,
                                                  ano, mes_num, cidade, shp_cells)

        # FIltrar a temperatura média da cidade para o mes analisado
        temp_hour = df_temp_city[df_temp_city["month"] == mes_num].reset_index()

        resultados = []
        for comb, props in combustiveis.items():
            try :  
                
                # Obter o consumo do combustivel 
                consumo_combsutivel = sheet_volume[sheet_volume["NOM_GRUPO_PRODUTO"] == comb]
                
                # Volume mensal consumido do combustível
                volume_mensal = consumo_combsutivel[(consumo_combsutivel[
                    "COD_LOCALIDADE_IBGE_D"] == cidade) & (consumo_combsutivel[
                        "NOM_GRUPO_PRODUTO"] == comb)][mes].iloc[0]
                
                # Definir a % de ethanol no combustivel
                ethanolPerc = props["ethanolPerc"]
                
                # Fatores de emissao de reabastecimento horário para cada temperatura (EF)
                EFCarRefueling_hour = carRefuelingEF(temp_hour["TEMP_C"].values,
                                                     ethanolPerc , rvpCurve)
               
                # Fator de emissao para descarte de combustivel
                EFSubmergedFilling = rvp(ethanolPerc, 880, rvpCurve)
                
                # Fator de emissao para respiradores de tanque de armazenamento
                EFTankBreathing = rvp(ethanolPerc, 120, rvpCurve)
                
                #
                df_comb = processar_combustivel(desg_consumo_city, temp_hour, 
                                                cidade, mes, comb, props, volume_mensal, 
                                                ethanolPerc, EFCarRefueling_hour, rvpCurve,
                                                EFSubmergedFilling, EFTankBreathing)
                
                # Armazenar os resultados
                resultados.append(df_comb)
                
                # Soma as emissões dos dois combustíveis e conversao para g/Hora
                emissoes = pd.concat(resultados, ignore_index=True).groupby(
                    ["city_id", "cell_id","datetime"], as_index=False)["emis_total"].sum()
                emissoes["emis_total"] = emissoes["emis_total"] / 1000 
                    
                # Salva resultado final
                filename_parquet = (
                    f"{outPath}/emissoes_postos/emissoes_{ano}_{str(mes_num).zfill(2)}/"
                    f"emissoes_{ano}_{str(mes_num).zfill(2)}_{cidade}.parquet"
                )
                os.makedirs(os.path.dirname(filename_parquet), exist_ok=True)
                emissoes.to_parquet(filename_parquet, index=False)
                
            except Exception as e:
             print(f"Erro ao processar {comb} na cidade {cidade}: {e}")


#%%

saoPaulo = pd.read_parquet(outPath + '/emissoes_postos/emissoes_2023_01/emissoes_2023_01_3550308.parquet')



# Garantir que os IDs são compatíveis
shp_cells['cell_id'] = shp_cells['fid'].astype(int)
saoPaulo['cell_id'] = saoPaulo['cell_id'].astype(int)

## Converter datetime
saoPaulo['datetime'] = pd.to_datetime(saoPaulo['datetime'])

# Definir resolução do grid (a partir das células)
# Aqui assumimos que as células são quadradas e têm a mesma dimensão
res = shp_cells.geometry.iloc[0].bounds  # xmin, ymin, xmax, ymax
cell_size = res[2] - res[0]

# Criar grid com base nos limites totais das células
xmin, ymin, xmax, ymax = shp_cells.total_bounds

# Criar coordenadas dos pixels (centros das células)
x_coords = np.arange(xmin + cell_size / 2, xmax, cell_size)
y_coords = np.arange(ymin + cell_size / 2, ymax, cell_size)

# Criar arrays vazios para o dataset
times = sorted(saoPaulo['datetime'].unique())
emis_array = np.full((len(times), len(y_coords), len(x_coords)), np.nan)

# Criar um mapeamento rápido de cell_id -> índice da célula
cell_index = {cid: i for i, cid in enumerate(shp_cells['cell_id'])}

# Criar uma correspondência entre cada cell_id e suas coordenadas (x, y)
centroids = shp_cells.copy()
centroids['x'] = centroids.geometry.centroid.x
centroids['y'] = centroids.geometry.centroid.y

# Mapear as emissões para o pixel correspondente
for t_i, t in enumerate(times):
    df_t = saoPaulo[saoPaulo['datetime'] == t]
    for _, row in df_t.iterrows():
        cell = centroids.loc[centroids['cell_id'] == row['cell_id']]
        if not cell.empty:
            cx = cell['x'].values[0]
            cy = cell['y'].values[0]
            # Encontrar índices correspondentes no grid
            ix = np.argmin(np.abs(x_coords - cx))
            iy = np.argmin(np.abs(y_coords - cy))
            emis_array[t_i, iy, ix] = row['emis_total']

# Criar Dataset xarray
ds = xr.Dataset(
    {
        "emis_total": (["time", "lat", "lon"], emis_array)
    },
    coords={
        "time": times,
        "lat": y_coords,
        "lon": x_coords
    },
)

ds["emis_total"].sel(time=ds.time.values[0])

# --- Plot simples ---
plt.figure(figsize=(8, 6))
ds["emis_total"].sel(time=ds.time.values[0]).plot(cmap="inferno",
    cbar_kwargs={'label': 'Emissão de VOC (kg/h)'}
)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(False)
plt.show()
#%%

emissao_total = (
    df_grouped.groupby(['datetime', 'CD_MUN'], group_keys=True)['emissao']
    .sum()
    .reset_index()
)

plt.figure(figsize=(12,6))
plt.plot(emissao_total.datetime, emissao_total["emissao"])
plt.legend()
plt.show()
#%%

postos_anp_loc = filtragempostos(postos_anp, postos_ibama)
gdf_postos = gpd.GeoDataFrame(
    postos_anp_loc,
    geometry=gpd.points_from_xy(postos_anp_loc["Longitude"], postos_anp_loc["Latitude"]),
    crs=shp_cells.crs
)

# 2. Identificar a célula de cada posto
gdf_postos = gpd.sjoin(
    gdf_postos,
    shp_cells[['fid', 'geometry']],
    how='left',
    predicate='within'
)

# 3. Contar postos por célula
postos_count = gdf_postos.groupby('fid', as_index=False).size().rename(columns={'size':'n_postos'})

# 4. Agregar emissões por célula
df_out_agg = df_out.groupby(['datetime','CD_MUN','cell_id'], as_index=False)['emissao'].sum()
df_out_agg["cell_id"] = df_out_agg["cell_id"].astype(int)
postos_count["fid"] = postos_count["fid"].astype(int)

# 5. Juntar emissões com número de postos e calcular emissão por posto
df_emissoes = df_out_agg.merge(
    postos_count[['fid','n_postos']],
    left_on='cell_id',
    right_on='fid',
    how='left'
)
df_emissoes['emission_por_posto'] = df_emissoes['emissao'] / df_emissoes['n_postos']

# 6. Adicionar emissão por posto de volta ao GeoDataFrame
postos_emissoes = gdf_postos.merge(
    df_emissoes[['datetime','fid','emission_por_posto']],
    on='fid',
    how='left'
).dropna(subset=['datetime'])

#%% Calculo emissao total de VOC em cada cidade

emissao_combustivelC = pd.read_csv(outpath + '/GASOLINA C.csv')
emissao_AEHC = pd.read_csv(outpath + '/AEHC.csv')

# Calcular emissão total de VOC em cada cidade (Litros)
def calcular_total(emissao_combustivelC, emissao_AEHC):
    # Preparar cada DataFrame
    for name, df in zip(['emissao_combustivelC', 'emissao_AEHC'], [emissao_combustivelC, emissao_AEHC]):
        df['CD_MUN'] = df['CD_MUN'].astype(int)
        df.drop(columns=['Combustivel'], inplace=True)
        df.set_index('CD_MUN', inplace=True)
        df[:] = df.apply(pd.to_numeric, errors='coerce')

    # Soma direta usando add, preenchendo NaN com 0
    emissao_total = emissao_combustivelC.add(emissao_AEHC, fill_value=0)
    return emissao_total


# Soma direta usando add (preenchendo NaN com 0)
emissao_total = calcular_total(emissao_combustivelC, emissao_AEHC)
emissao_total = emissao_total.reset_index()

# # Plot por cidade
# emissao_total["CD_MUN"] = emissao_total["CD_MUN"].astype(str)
# emissao_total_geometry = emissao_total.merge(shp_mun[["CD_MUN", "geometry"]],
#                                              on="CD_MUN", how="left")
# emissao_total_geometry = gpd.GeoDataFrame(emissao_total_geometry,
#                                           geometry="geometry", crs=shp_mun.crs)
# # Plotando em log
# emissao_total_geometry.plot(
#     column='202312',
#     cmap='plasma',
#     legend=True,
#     norm=LogNorm()  # escala logarítmica
# )
#%% Desagregando em cada posto ANALISARR

postos_brasil = pd.read_csv(dataPath + '/postos.csv')
postos_brasil_ativos = postos_brasil[postos_brasil[
    'Situação cadastral'] == 'Ativa'].drop_duplicates(subset=[
        'Latitude', 'Longitude'])
        
df = pd.read_csv(
    dataPath + '/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv', sep=';')

# Supondo que seu DataFrame seja df
def format_cnpj(cnpj):
    cnpj_str = str(cnpj).zfill(14)  # garante 14 dígitos
    return f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:]}"

df['CNPJ'] = df['CNPJ'].apply(format_cnpj)


df_merge = df.merge(
    postos_brasil[['CNPJ' , 'Latitude' , 'Longitude' ]],
    on =  'CNPJ',
    how = 'left'
    )
linhas_nan_lat = df_merge[df_merge['Latitude'].isna()]
#%%

# Tratamento de dados

# Selecionar com situação CNPJ ativo
postos_brasil_ativos = postos_brasil[postos_brasil[
    'Situação cadastral'] == 'Ativa'].drop_duplicates(subset=[
        'Latitude', 'Longitude'])
# Selecionar apenas sem data de termino
# df_p_ativo_d = df_p_ativo_d[df_p_ativo_d['Data de término da atividade'].isna()]


# Retiradas de linhas que provavelmente nao sao postos
palavras_excluir = [
    "gas", "gás", "autopeças", "autopecas", "bebidas", "peças", "pecas",
    "incorporadora", "industria", "industrial", "revenda de gas", "revenda de gás",
    "distribuidora de gas", "distribuidora de gás", "glp","auto pecas"
    "auto peças","mercado", "mercadinho", "supermercado","super mercado" "atacado", "varejo",
    "alimentos", "material de construção","industria", "industrial", "cerâmica",
    "ceramica", "usina","construcao", "construção", "telecomunicações",
    "associação", "depósito", "deposito", "consultoria", "projetos",
    "logística", "logistica", "taxi", "táxi","ceramica", "cerâmica"
]

for palavra in palavras_excluir:
    postos_brasil_ativos = postos_brasil_ativos[
        ~postos_brasil_ativos["Razão Social"].str.contains(palavra, case=False, na=False)
    ]

    

# Contar quantos postos existem em cada município
postos_count = postos_brasil_ativos.groupby(
     "CD_MUN").size().reset_index(name="n_postos")


postos_flori = 4205407

# Juntar postos com a contagem de postos
emissao_total["CD_MUN"] = emissao_total["CD_MUN"].astype(int)
postos_count["CD_MUN"] = postos_count["CD_MUN"].astype(int)

df_emissoes = emissao_total.merge(
    postos_count[['CD_MUN','n_postos']],
    on="CD_MUN",
    how="left"
)

df_emissoes = df_emissoes.set_index('datetime')

df_emissoes['Emissoes_postos'] = df_emissoes['emissao']/df_emissoes['n_postos']

postos_flori = postos_brasil_ativos[postos_brasil_ativos['CD_MUN'] == 4205407 ]

postos_emissoes = postos_brasil_ativos.merge(
    df_emissoes[['CD_MUN'] + list(df_emissoes.columns[-12:])],
    on="CD_MUN",
    how="left"
)


# dividir as emissões igualmente entre os postos
for c in [str(c) for c in colunas_meses]:
    df_emissoes['Emissoes_postos_' + c] = df_emissoes[c] / df_emissoes["n_postos"]

# cada posto recebe a emissão do município correspondente
postos_emissoes = postos_brasil_ativos.merge(
    df_emissoes[['CD_MUN'] + list(df_emissoes.columns[-12:])],
    on="CD_MUN",
    how="left"
)

# Plot dos postos por cidade
# gdf = gpd.GeoDataFrame(
#     postos_emissoes,
#     geometry=gpd.points_from_xy(postos_emissoes['Longitude'], postos_emissoes['Latitude']),
#     crs="EPSG:4326"  
# )

# gdf = gdf.to_crs(shp_mun.crs)
# fig, ax = plt.subplots(figsize=(10, 10))
# shp_mun.plot(ax=ax, facecolor="none", edgecolor="gray",
#             linewidth=0.5)  # municípios
# gdf.plot(
#     column="Emissoes_postos_202311",  # coluna usada para escala
#     ax=ax,
#     cmap="viridis",                      
#     markersize=10)
# plt.show()

#%%

shp_mun_m = shp_mun.merge(
    df_emissoes[['CD_MUN', '202301']],  # escolher o mês desejado
    left_on='CD_MUN',
    right_on='CD_MUN',
    how='left'
)

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

# Criar figura e eixo
fig, ax = plt.subplots(figsize=(12, 12))

# Plotar usando normalização logarítmica
shp_mun_m.plot(
    column='202301', 
    ax=ax,
    cmap='OrRd',
    norm=LogNorm(vmin=shp_mun_m['202301'].replace(0, 1).min(),  # substituir 0 por 1 para evitar log(0)
                 vmax=shp_mun_m['202301'].max()),
    legend=True,
    edgecolor='black',
    linewidth=0.3
)

ax.set_title('Emissões de VOC por município - Janeiro 2023 (log scale)', fontsize=16)
ax.set_axis_off()

plt.show()


#%%
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt

# --- supondo que seu dataframe seja df ---
# converter para GeoDataFrame
gdf = gpd.GeoDataFrame(
    postos_emissoes,
    geometry=gpd.points_from_xy(postos_emissoes["Longitude"], postos_emissoes["Latitude"]),
    crs="EPSG:4326"
)

# criar buffers proporcionais às emissões
# fator de escala (pode ajustar conforme necessário para visualização)
scale = 0.1  
gdf["buffer"] = gdf.geometry.buffer(gdf["Emissoes_postos_202301"] * scale)

# criar GeoDataFrame só com os buffers
gdf_buf = gpd.GeoDataFrame(gdf, geometry="buffer", crs=gdf.crs)

# --- plotagem ---
fig, ax = plt.subplots(figsize=(10,10))
shp_mun.boundary.plot(ax=ax, edgecolor='black', linewidth=0.2,
                     alpha=0.5)  
gdf_buf.plot(ax=ax, column="Emissoes_postos_202301", cmap="Reds", alpha=1, legend=True)
gdf.plot(ax=ax, color="black", markersize=0.2)  # postos
ax.set_title("Áreas de risco de VOC em postos de combustíveis", fontsize=14)
plt.show()
#%%
import netCDF4 as nc

filePath = os.path.join(dataPath, "dataset_jan_2023.nc")
# desWeek = nc.Dataset(filePath)

# desWeek =  xr.open_mfdataset(dataPath  +'/dataset_jan_2023.nc')

desWeek =  xr.open_mfdataset(dataPath  +'/dataset_jan_2023.nc')

# Plotar para uma data específica
ds = desWeek['vkt_fraction'].assign_coords(
    lat=(('y', 'x'), desWeek.lat[::-1, :].data))
ds.isel(time=0).plot(x='lon', y='lat')

a = shp_mun[shp_mun['CD_MUN'] == 3550308]
ds = ds.rio.write_crs(a.crs)

temp_clip = ds.rio.clip(a.geometry.values, a.crs, drop=True, all_touched=True)


fig, ax = plt.subplots(figsize=(12, 12))
ds.isel(time=0).plot(x='lon', y='lat', ax=ax)
a.boundary.plot(ax=ax, edgecolor='black')


# %% Atribuir as emissoes as localizações dos postos que estão no geofabrik

# br_mun = gpd.read_file(
#     tablePath + '/inputs/BR_Municipios_2022/BR_Municipios_2022.shp')
# # postos = gpd.read_file(tablePath + '/inputs/centro-oeste-250908-free.shp/gis_osm_traffic_a_free_1.shp')
# # postos_clss = postos[postos['fclass'] == 'fuel']

# postos_lista = []
# for pasta in os.listdir(os.path.join(dataPath, "geofabrik")):
#     # procura arquivos .shp dentro da pasta
#     arquivo = os.path.join(dataPath, 'geofabrik', pasta,
#                            "gis_osm_traffic_a_free_1.shp")
#     gdf = gpd.read_file(arquivo)
#     gdf_fuel = gdf[gdf['fclass'] == 'fuel']
#     postos_lista.append(gdf_fuel)

# # concatena todos em um único GeoDataFrame
# postos_clss = gpd.GeoDataFrame(
#     pd.concat(postos_lista, ignore_index=True), crs=gdf.crs)

# # Separa os postos por cidade
# postos_clss = postos_clss.to_crs(br_mun.crs)
# postos_com_mun = gpd.sjoin(
#     postos_clss,
#     # 'CD_MUN' ou outra coluna com o código IBGE
#     br_mun[['CD_MUN', 'geometry']],
#     how="left",
#     predicate="within"
# )


# # 1. Contar quantos postos existem em cada município
# postos_count = postos_com_mun.groupby(
#     "CD_MUN").size().reset_index(name="n_postos")

# # 2. Carregar as emissões mensais que você já calculou (df_out)
# #    df_out tem colunas: ["Cidade", "Combustivel", 202101, 202102, ...]
# #    "Cidade" == código IBGE do município

# # 3. Juntar df_out com a contagem de postos
# df_emissoes = df_out.merge(
#     postos_count, left_on="CD_MUN", right_on="CD_MUN", how="left")

# # 4. Dividir as emissões igualmente entre os postos
# colunas_meses_str = [str(c) for c in colunas_meses]

# for c in colunas_meses_str:
#     df_emissoes['Emissoes_postos_' + c] = df_emissoes[c] / \
#         df_emissoes["n_postos"]

# # 5. Expandir: cada posto recebe a emissão do município correspondente
# postos_emissoes = postos_com_mun.merge(
#     df_emissoes[['CD_MUN'] + list(df_emissoes.columns[-12:])],
#     on="CD_MUN",
#     how="left"
# )
# %%
import  numpy as np

# df_postos_pessoas_jurudicas = pd.read_csv(
#     dataPath + '/pessoasJuridicas.csv', sep=';')
# df_postos = df_postos_pessoas_jurudicas[df_postos_pessoas_jurudicas['Código da atividade'] == 6]
# df_ceps = pd.read_csv(dataPath + '/qualocep_geo.csv', sep='|')

# df_join = df.merge(
#     df_ceps,
#     left_on="CEP",   # coluna do DataFrame da esquerda
#     right_on="cep",  # coluna do DataFrame da direita
#     how="left"       # tipos: 'inner', 'left', 'right', 'outer'
# )
# df_join = df_join.replace('-', np.nan)

# # Contar quantas vezes cada par lat/lon aparece
# contagem = (
#     df_join.groupby(['latitude', 'longitude'])
#     .size()
#     .reset_index(name='ocorrencias')
#     .sort_values(by='ocorrencias', ascending=False)
#     .reset_index()
# )

# contagem = contagem.replace('-', np.nan)
# contagem = contagem.dropna(subset=['latitude', 'longitude'])
# # forçar conversão para float
# df_join = df_join.dropna(subset=['latitude', 'longitude'])
# gdf = gpd.GeoDataFrame(
#     contagem,
#     geometry=gpd.points_from_xy(contagem['longitude'], contagem['latitude']),
#     crs="EPSG:4326"  # WGS84
# )

# # plot
# fig, ax = plt.subplots(figsize=(10, 10))
# shp_mun.plot(ax=ax, facecolor="none", edgecolor="gray",
#             linewidth=0.5)  # municípios
# gdf.plot(ax=ax, color="red", markersize=5)  # pontos

# plt.show()


# gdf['buffer'] = gdf.apply(lambda row: row.geometry.buffer(
#     0.01 * row['ocorrencias']), axis=1)

# # Plot
# fig, ax = plt.subplots(figsize=(12, 12))
# shp_mun.boundary.plot(ax=ax, edgecolor='black', linewidth=0.2,
#                      alpha=0.5)  # contorno dos municípios
# gdf['buffer'].plot(ax=ax, color='red')  # buffers dos pontos

# ax.set_title('Buffers ponderados pela contagem sobre municípios')
# ax.set_xlabel('Longitude')
# ax.set_ylabel('Latitude')
# plt.show()

# postos_mesmo_ponto = df_join[(df_join['latitude'] == contagem['latitude'][0]) & (
#     df_join['longitude'] == contagem['longitude'][0])]

