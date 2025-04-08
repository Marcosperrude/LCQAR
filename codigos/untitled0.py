# -*- coding: utf-8 -*-
"""
Created on Mon Mar 31 14:08:27 2025

Código desenvolvido para o inventário nacional de fontes fixas, emissoes residênciais

@author: marcos perrude
"""

import pandas as pd
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import os
from shapely.geometry import box
#%%
#Dados : https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?edicao=41851&t=downloads
#malha com atributos - setores - csv
dff = pd.read_csv(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_setores_CD2022.csv")
#Caminho para os setores censitários do Brasil: https://www.ibge.gov.br/geociencias/downloads-geociencias.html?caminho=organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/shp/UF
setores = r"C:\Users\marcos perrude\Documents\LCQAR\dados\Setores"
br_uf = gpd.read_file(r"C:\Users\marcos perrude\Documents\LCQAR\dados\BR_UF_2023\BR_UF_2023.shp")   

#%%Tratamento dos dados
dff = dff[dff["CD_SIT"] != 9]  #Massas de agua
dff = dff[dff['v0002'] !=0]  #areas com 0 domicílios
#%% Definir a classificação de acordo com o tipo do setor censitário
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
data = {
    "Categoria": [
        "Gás de botijão/canalizado", "Lenha", "Carvão", "Energia elétrica/outros combustíveis", "Sem declaração"
    ],
    "URBANA - Cidade ou vila, área urbanizada": [0.972, 0.019, 0.008, 0.0, 0.0],
    "URBANA - Cidade ou vila, área não urbanizada": [0.944, 0.051, 0.005, 0.0, 0.0],
    "URBANA - Área urbana isolada": [0.982, 0.016, 0.0, 0.002, 0.0],
    "RURAL - Aglomerado rural de extensão urbana": [0.977, 0.021, 0.002, 0.0, 0.0],
    "RURAL - Aglomerado rural, isolado, povoado": [0.73, 0.219, 0.051, 0.0, 0.0],
    "RURAL - Aglomerado rural, isolado, outros aglomerados": [0.728, 0.245, 0.028, 0.0, 0.0],
    "RURAL - Zona rural exclusive aglomerado rural": [0.542, 0.407, 0.052, 0.0, 0.0],
}

data = pd.DataFrame(data)

# Mapear a quantidade de residencias que utilizam lenha
fatores = data.set_index("Categoria").loc["Lenha"].to_dict()
    
# Mapear a classificação ao fator correspondente
dff["Fator_Lenha"] = dff["Classificacao"].map(fatores)

#Definindo a quantidade de residenciais que utilizam lenha
dff["Residencias_Ajustadas"] = dff["v0002"] * dff["Fator_Lenha"]

#Consumo de lenha por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
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

#Consumo total de lenha por setor censitário, de acordo com a classificação

dff['Consumo_lenha[g/s]'] = (dff['Residencias_Ajustadas'] * dff['NM_REGIAO'].map(consumo_lenha)) * (1000/86400) #Conversão de kg/dia --> g/s
dff['Consumo_Carvao[g/s]'] = (dff['Residencias_Ajustadas'] * dff['NM_REGIAO'].map(consumo_carvao)) * (1000/86400) #Conversão de kg/dia --> g/s
#%% Estimando Fator de emissao
#Uilizando o poder calorifico disponibilizado pelo documento da fonte de dados, estimar os fatores de emissao de acordo com o poder calorífico
#Fonte: https://www.epa.gov/system/files/documents/2022-03/c1s6_final_0.pdf
#poder calorífico (lenha == 8 Btu/lb (0.08 MMBtu/lb), carvao == 14 Btu/lb (0.14 MMBtu/lb))

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
emissoes_carvao['CD_SETOR'] = dff['CD_SETOR']  # Mantendo a coluna 'CD_SETOR'
for i, pol in enumerate(fator_emissao['Poluentes']):
    emissoes_lenha[pol] = dff['Consumo_lenha[g/s]'] * fator_emissao['Lenha'][i]
    emissoes_carvao[pol] = dff['Consumo_Carvao[g/s]'] * fator_emissao['Carvao'][i]

#%% Função para encontrar o arquivo shapefile
def encontrar_arquivo(Estado):
    pastas = [filename for filename in os.listdir(setores) if filename.startswith(Estado)]
    dirpasta = os.path.join(setores, pastas[0])
    arquivos = os.listdir(dirpasta)
    shp_files = [f for f in arquivos if f.endswith(".shp")]
    dirarquivo = os.path.join(dirpasta, shp_files[0])
    gdf = gpd.read_file(dirarquivo)
    return gdf

#%% Vinculando a geometria do shape com os fatores de emissões
def vinculargeometria(Combustivel, Estado):
    gdf = encontrar_arquivo(Estado)
    if Combustivel == "Lenha":
        emissoes_lenha['CD_SETOR'] = emissoes_lenha['CD_SETOR'].astype(str)
        estado_filtrado = gdf[['CD_SETOR', 'geometry']].copy()
        estado_filtrado['CD_SETOR'] = estado_filtrado['CD_SETOR'].astype(str)
        emissoes = pd.merge(emissoes_lenha, estado_filtrado, on='CD_SETOR', how='right')
    elif Combustivel == "Carvao":
        emissoes_carvao['CD_SETOR'] = emissoes_carvao['CD_SETOR'].astype(str)
        estado_filtrado = gdf[['CD_SETOR', 'geometry']].copy()
        estado_filtrado['CD_SETOR'] = estado_filtrado['CD_SETOR'].astype(str)
        emissoes = pd.merge(emissoes_carvao, estado_filtrado, on='CD_SETOR', how='right')
    emissoes = gpd.GeoDataFrame(emissoes, geometry='geometry', crs=gdf.crs)
    return emissoes

#%% Função para calcular emissões ponderadas por área
def calcular_emissoes_por_pixel(cell, Poluente, emissoes):
    setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
    if setores_intersectados.empty:
        return 0
    setores_intersectados["area_total"] = setores_intersectados.geometry.area
    setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
    setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
    return (setores_intersectados[Poluente] * setores_intersectados["peso"]).sum()

#%% Função para plotar emissões
def plotaremissoes(Poluente, Estado, Tam_pixel, Combustivel):
    emissoes = vinculargeometria(Combustivel, Estado)
    uf = br_uf.query('SIGLA_UF == @Estado')
    minx, miny, maxx, maxy = uf.total_bounds
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    grid = gpd.GeoDataFrame(geometry=grid_cells, crs=uf.crs)

    # Aqui está a lista onde os valores serão armazenados
    valores = []

    # Loop pelas células do grid
    for i in grid.index:
        cell = grid.geometry[i]
        valor = calcular_emissoes_por_pixel(cell, Poluente, emissoes)
        valores.append(valor)

    grid[Poluente] = valores
    
    return grid
#%% função do plot
def plotar(Poluente, Estado, Tam_pixel, Combustivel):
    #Se for brasi
    if Estado == 'BR':
        ufs = [filename[:2] for filename in os.listdir(setores)]
        brasil = []
        
        for i in ufs:
            print(f"Processando {i}")
            cidades = plotaremissoes(Poluente, i, Tam_pixel, Combustivel)
            brasil.append(cidades)

        mapa = gpd.GeoDataFrame(pd.concat(brasil, ignore_index=True), crs=brasil[0].crs)
        mapa_fundo = br_uf
    
    else : 
        mapa = plotaremissoes(Poluente, Estado, Tam_pixel, Combustivel)
        mapa_fundo = encontrar_arquivo(Estado)

    #Plotagem
    fig, ax = plt.subplots(figsize=(15, 10))
    lognorm = mcolors.LogNorm(vmin=mapa[Poluente].replace(0, np.nan).min(), vmax=mapa[Poluente].max())
    mapa_fundo.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1)
    mapa.plot(ax=ax, column=Poluente, cmap="jet", alpha=0.75, edgecolor=None, legend=True, norm=lognorm)
    plt.savefig(r"C:\Users\marcos perrude\Documents\LCQAR\imagens\quantificação_lenha\lenha.png", dpi=300)
    plt.show()
#%% Execução
Combustivel = "Lenha"
Tam_pixel = 1  # 0.1 Equivale a ~1km se o CRS for metros
Poluente = 'PM'
Estado = 'PR'

plot = plotar(Poluente, Estado, Tam_pixel, Combustivel)


#%% Relacionamento das emissoes co as geometrias
#BRfiltrado = SC[['CD_SETOR', 'geometry']]
#BRfiltrado['CD_SETOR'] = BRfiltrado['CD_SETOR'].astype(str)


#emissoes_lenha = pd.merge(emissoes_lenha, SCfiltrado[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right')
#emissoes_lenha = gpd.GeoDataFrame(emissoes_lenha)

#lognorm = mcolors.LogNorm(vmin=emissoes_lenha['PM'].min(), vmax=emissoes_lenha['PM'].max())

#fig, ax = plt.subplots(figsize=(20, 10))
#SC_estado.plot(ax=ax, color='black', edgecolor='black', alpha=0.7)
#emissoes_lenha.plot(ax=ax, column='PM', cmap='jet', legend=True, norm=lognorm)


#%%
uf = br_uf.query('SIGLA_UF == @Estado')
minx, miny, maxx, maxy = uf.total_bounds
x_coords = np.arange(minx, maxx, Tam_pixel)
y_coords = np.arange(miny, maxy, Tam_pixel)
grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]

grid_cells = gpd.GeoDataFrame(geometry=grid_cells)

fig,ax =  plt.subplots(figsize=(15, 10))
uf.boundary.plot(ax=ax)
grid_cells.boundary.plot(ax=ax)









