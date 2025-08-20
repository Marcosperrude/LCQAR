# main_pesados_exaustivas.py

import os
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import geopandas as gpd
from unidecode import unidecode
from glob import glob
import polars as pl
import re
import gc
import pyarrow

from funcoes_pesados_exaustivas import (
    calcular_matriz_probabilidade_pesados,
    carregar_fator_emissao,
    adicionar_fatores_emissao_pesados,
    padronizar_combustivel,
    adicionando_prob_ano_modelo,
    atribuir_consumo_combustivel,
    aplicar_deterioracao_veiculos,
    adicionar_probabilidade_subcategoria_pesados,
    calcular_emissoes_veiculares_exaustivas_pesados,
    processamento_arquivos_frota_categoria,
    adicionando_dados_ibge_frota,
    adicionando_codigo_ibge_mun_especiais_sem_espaco,
    identificando_cod_ibge,
    normalizar_nome_municipio,
    carregar_temperatura_media,
    processamento_arquivos_frota_ano,
    curva_sucateamento,
    processamento_arquivos_frota_combustivel,
    probabilidade_ano_modelo,
    consumos_flex_fuel,
    probabilidade_comb_leves,
    probabilidade_comb_comleves,
    probabilidade_comb_motos,
    processamento_arquivos_consumo_comb,
    adicionando_dados_ibge_consumo_comb,
    adicionando_codigo_ibge_mun_especiais_sem_espaco,
    combustivel_transportes_ben,
    segregacao_consumos_comb,
    fator_deterioracao,
    processar_matrizes_etanol,
    substituir_combustivel_fe_municipios_sem_etanol,
    substituir_combustivel_fe_municipios_sem_etanol_exaustivas,
    processar_probabilidades_combustivel_leves,
    processar_probabilidades_combustivel_comleves,
    processar_probabilidades_combustivel_motos,
    adicionar_probabilidade_motorizacao,
    adicionar_temp_uso_dias,
    calculo_emissao_diurnal,
    calculo_emissao_hotsoak,
    calculo_emissao_running_losses,
    manter_colunas_exaustivas,
    calcular_emissoes_veiculares_exaustivas,
    numero_motos,
    prob_motorizacao_motos,
    unindo_matrizes,
    carregar_autonomia,
    adicionar_autonomia
)


## Definição dos caminhos dos arquivos

#Insira o caminho para a pasta no seu computador:
caminho_diretorio = "/home/marcosperrude/Documents/LCQAR/BRAVES/Inputs"

#Caminhos padrão se o diretório completo com os dados de entrada forem baixados
caminho_arquivos_frota_categoria = r"2.FrotaPorMunicipio"

caminho_arquivos_frota_ano = r"3.FrotaPorMunicipioEAnoDeFabricacao"

caminho_arquivos_frota_comb = "4.FrotaPorMunicipioECombustivel"

caminho_arquivos_consumo_comb = r"Consumo_combustivel_mensal/1_janeiro_2019"

temperatura_media = carregar_temperatura_media(caminho_diretorio)

## Definição de variáveis globais
# Dicionário de correspondência entre estados e siglas
estados_brasileiros = {
    "ACRE": "AC", "ALAGOAS": "AL","AMAPA": "AP","AMAZONAS": "AM","BAHIA": "BA",
    "CEARA": "CE","DISTRITO FEDERAL": "DF","ESPIRITO SANTO": "ES",
    "GOIAS": "GO","MARANHAO": "MA","MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS","MINAS GERAIS": "MG","PARA": "PA",
    "PARAIBA": "PB","PARANA": "PR","PERNAMBUCO": "PE","PIAUI": "PI",
    "RIO DE JANEIRO": "RJ","RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS","RONDONIA": "RO","RORAIMA": "RR",
    "SANTA CATARINA": "SC","SAO PAULO": "SP","SERGIPE": "SE","TOCANTINS": "TO"}

# Dicionário para mapear nomes dos meses para números
meses_para_numeros = {'janeiro': 1, 'fevereiro': 2,'marco': 3,'abril': 4,
                      'maio': 5,'junho': 6,'julho': 7,'agosto': 8,
                      'setembro': 9,'outubro': 10,'novembro': 11,'dezembro': 12}

meses_para_numeros2 = {'jan': 1, 'fev': 2,'mar': 3,'abr': 4,'mai': 5,'jun': 6,
                       'jul': 7,'ago': 8,'set': 9,'out': 10,'nov': 11,'dez': 12}

# Definição do mapeamento de combustíveis
mapa_combustivel = {
        'GASOLINA': 'Gasolina C',
        'GASOLINA/ELETRICO': 'Gasolina C',
        'ALCOOL': 'Etanol',
        'ALCOOL/GASOLINA': 'Flex',
        'GASOLINA/ALCOOL': 'Flex',
        'DIESEL': 'Diesel',
        'GASOLINA/ALCOOL/GAS NATURAL': 'GNV',
        'GAS NATURAL VEICULAR': 'GNV',
        'GAS METANO': 'GNV',
        'GASOL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'GASOLINA/GAS NATURAL VEICULAR': 'GNV',
        'ALCOOL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'ALCOOL/GAS NATURAL VEICULAR': 'GNV',
        'GASOGENIO': 'GNV',
        'DIESEL/GAS NATURAL VEICULAR': 'GNV',
        'DIESEL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'GAS/NATURAL10/LIQUEFEITO': 'GNV',
        'GASOLINA/ALCOOL/ELETRICO': 'Não considerado',
        'DIESEL/ELETRICO': 'Não considerado',
        'ETANOL/ELETRICO': 'Não considerado',
        'VIDE/CAMPO/OBSERVACAO': 'Não considerado',
        'HIBRIDO PLUG-IN': 'Não considerado',
        'ELETRICO/FONTE EXTERNA': 'Não considerado',
        'ELETRICO/FONTE INTERNA': 'Não considerado',
        'Sem Informação': 'Não considerado',
        'Não Identificado': 'Não considerado',
        'Não se Aplica': 'Não considerado',
        'CELULA COMBUSTIVEL': 'Não considerado'}

# Mapeamento dos tipos de combustível
codigos_combustivel_mai = {'ETANOL HIDRATADO': 1, 'DIESEL': 2,
                           'FLEX-ETANOL HIDRATADO': 3, 'FLEX-GASOLINA C': 4,
                           'GASOLINA C': 5}

codigos_combustivel_min = {'Etanol': 1,'Diesel': 2,'Flex Etanol': 3,
                           'Flex Gasolina': 4, 'Gasolina C': 5}

# Mapeamento dos tipos de combustível de acordo com strings do df de autonomia
codigos_combustivel_autonomia = {'Etanol': 1,'Diesel': 2,'Flex Etanol': 3,
                                 'Flex Gasolina': 4, 'Gasolina': 5}


#%%
ibge_dados_cidades, ibge_estados, ibge_uf, codigos_uf = identificando_cod_ibge(caminho_diretorio)
# Aplicar normalização aos nomes dos municípios do IBGE
ibge_dados_cidades_normalizados = [
    (normalizar_nome_municipio(nome), codigo) 
    for nome, codigo in ibge_dados_cidades]

frota_categoria_processada = processamento_arquivos_frota_categoria(caminho_diretorio,
                                                                    caminho_arquivos_frota_categoria,
                                                                    estados_brasileiros,
                                                                    meses_para_numeros)
frota_categoria_processada = adicionando_dados_ibge_frota(frota_categoria_processada, 
                                                          ibge_dados_cidades,
                                                          ibge_uf, 
                                                          codigos_uf)
frota_categoria_processada = adicionando_codigo_ibge_mun_especiais_sem_espaco(frota_categoria_processada)
#%% Processamento dos dados

#Processando frota ano modelo
frota_ano_processada, anos_dados = processamento_arquivos_frota_ano(caminho_diretorio,
                                                                    caminho_arquivos_frota_ano, 
                                                                    estados_brasileiros,
                                                                    meses_para_numeros)

frota_ano_processada = adicionando_dados_ibge_frota(frota_ano_processada, ibge_dados_cidades, ibge_uf, codigos_uf)
frota_ano_processada = adicionando_codigo_ibge_mun_especiais_sem_espaco(frota_ano_processada)
valores_suc = curva_sucateamento(anos_dados, frota_ano_processada)
frota_processada_prob_ano_modelo = probabilidade_ano_modelo(frota_ano_processada, valores_suc)

#Processando frota combustivel
frota_combustivel_processada = processamento_arquivos_frota_combustivel(caminho_diretorio, 
                                                                        caminho_arquivos_frota_comb,
                                                                        meses_para_numeros,
                                                                        mapa_combustivel)
frota_combustivel_processada = adicionando_dados_ibge_frota(frota_combustivel_processada, 
                                                            ibge_dados_cidades, 
                                                            ibge_uf, 
                                                            codigos_uf)
frota_combustivel_processada = adicionando_codigo_ibge_mun_especiais_sem_espaco(frota_combustivel_processada)
frota_combustivel_processada_flexfuel = consumos_flex_fuel(caminho_diretorio,frota_combustivel_processada)
frota_proporcao_leves_82, frota_proporcao_leves_2003, frota_proporcao_leves_2007 = probabilidade_comb_leves(frota_combustivel_processada_flexfuel)
frota_proporcao_comleves_83, frota_proporcao_comleves_2003, frota_proporcao_comleves_2006, frota_proporcao_comleves_2007 = probabilidade_comb_comleves(frota_combustivel_processada_flexfuel)
frota_proporcao_motos_2003, frota_proporcao_motos_2010 = probabilidade_comb_motos(frota_combustivel_processada_flexfuel)

#Processando consumo combustível
dfs_combustiveis = processamento_arquivos_consumo_comb(caminho_diretorio,
                                                       caminho_arquivos_consumo_comb,
                                                       meses_para_numeros)

# Acessando DataFrames individuais
consumo_gasolina = dfs_combustiveis['G']
consumo_etanol = dfs_combustiveis['E']
consumo_oleo = dfs_combustiveis['D']

##Adicionando código IBGE aos dfs
consumo_gasolina = adicionando_dados_ibge_consumo_comb(consumo_gasolina, 
                                                       ibge_dados_cidades,
                                                       ibge_uf,
                                                       codigos_uf)
consumo_etanol = adicionando_dados_ibge_consumo_comb(consumo_etanol,
                                                     ibge_dados_cidades,
                                                     ibge_uf, codigos_uf)
consumo_oleo = adicionando_dados_ibge_consumo_comb(consumo_oleo, 
                                                   ibge_dados_cidades,
                                                   ibge_uf, 
                                                   codigos_uf)

consumo_gasolina = adicionando_codigo_ibge_mun_especiais_sem_espaco(consumo_gasolina)
consumo_etanol = adicionando_codigo_ibge_mun_especiais_sem_espaco(consumo_etanol)
consumo_oleo = adicionando_codigo_ibge_mun_especiais_sem_espaco(consumo_oleo)

consumo_oleo, consumo_gasolina, consumo_etanol = combustivel_transportes_ben(caminho_diretorio, 
                                                                             consumo_oleo,
                                                                             consumo_gasolina,
                                                                             consumo_etanol)
consumo_gasolina, consumo_etanol, consumo_oleo = segregacao_consumos_comb(caminho_diretorio, 
                                                                          consumo_gasolina, 
                                                                          consumo_etanol,
                                                                          consumo_oleo)

# Deterioração
deter_leves_otto, deter_leves_diesel, deter_motos_otto, deter_pesados = fator_deterioracao(caminho_diretorio,
                                                                                           valores_suc,
                                                                                           anos_dados)

#%% Procesamento matrizes evaporativas pras diferentes categorias
#%%
### Processando matrizes pras diferentes categorias
##LEVES
fator_emissao_leves= carregar_fator_emissao(caminho_diretorio, "EF_Evaporative_LightDuty.xlsx")
matriz_leves_com_etanol, matriz_leves_sem_etanol = processar_matrizes_etanol(frota_categoria_processada, 
                                                                             consumo_etanol,
                                                                             fator_emissao_leves)

matriz_leves_sem_etanol= substituir_combustivel_fe_municipios_sem_etanol(matriz_leves_sem_etanol,
                                                                         fator_emissao_leves)
matriz_leves_sem_etanol = padronizar_combustivel(matriz_leves_sem_etanol)
matriz_leves_com_etanol = padronizar_combustivel(matriz_leves_com_etanol)
# display(matriz_leves_com_etanol)
#Adicionando variáveis de cálculo
matriz_leves_com_etanol = adicionando_prob_ano_modelo(matriz_leves_com_etanol,
                                                      frota_processada_prob_ano_modelo, 
                                                      'PROBABILIDADE LEVES')
matriz_leves_sem_etanol = adicionando_prob_ano_modelo(matriz_leves_sem_etanol,
                                                      frota_processada_prob_ano_modelo, 
                                                      'PROBABILIDADE LEVES')
# display(matriz_leves_com_etanol)
matriz_leves_com_etanol, matriz_leves_sem_etanol = processar_probabilidades_combustivel_leves(matriz_leves_com_etanol, matriz_leves_sem_etanol, 
                                                                                                frota_proporcao_leves_82, frota_proporcao_leves_2003, 
                                                                                                frota_proporcao_leves_2007)

matriz_leves_com_etanol = atribuir_consumo_combustivel(matriz_leves_com_etanol, 
                                                       consumo_gasolina, 
                                                       consumo_etanol,
                                                       consumo_oleo, 
                                                       'PROPORCAO LEVES')
matriz_leves_sem_etanol = atribuir_consumo_combustivel(matriz_leves_sem_etanol,
                                                       consumo_gasolina, 
                                                       consumo_etanol, 
                                                       consumo_oleo, 
                                                       'PROPORCAO LEVES')
autonomia_leves = carregar_autonomia(caminho_diretorio,
                                     codigos_combustivel_autonomia,
                                     "EF_LightDuty.xlsx")
matriz_leves_com_etanol = adicionar_autonomia(matriz_leves_com_etanol, 
                                              autonomia_leves)
matriz_leves_sem_etanol = adicionar_autonomia(matriz_leves_sem_etanol, 
                                              autonomia_leves)
matriz_leves = unindo_matrizes(matriz_leves_com_etanol, 
                               matriz_leves_sem_etanol)

#%%

#COMERCIAIS LEVES
fator_emissao_comleves= carregar_fator_emissao(caminho_diretorio, "EF_Evaporative_LightCommercial.xlsx")
matriz_comleves_com_etanol, matriz_comleves_sem_etanol = processar_matrizes_etanol(frota_categoria_processada,
                                                                                   consumo_etanol,
                                                                                   fator_emissao_comleves)
matriz_comleves_sem_etanol= substituir_combustivel_fe_municipios_sem_etanol(matriz_comleves_sem_etanol,
                                                                            fator_emissao_comleves)
matriz_comleves_sem_etanol = padronizar_combustivel(matriz_comleves_sem_etanol)
matriz_comleves_com_etanol = padronizar_combustivel(matriz_comleves_com_etanol)
#Adicionando variáveis de cálculo
matriz_comleves_com_etanol = adicionando_prob_ano_modelo(matriz_comleves_com_etanol,
                                                         frota_processada_prob_ano_modelo,
                                                         ['PROBABILIDADE COMLEVES',
                                                          'PROBABILIDADE COMLEVES DIESEL'])
matriz_comleves_sem_etanol = adicionando_prob_ano_modelo(matriz_comleves_sem_etanol, frota_processada_prob_ano_modelo,
                                                         ['PROBABILIDADE COMLEVES',
                                                          'PROBABILIDADE COMLEVES DIESEL'])
                                                                                                                        
matriz_comleves_com_etanol, matriz_comleves_sem_etanol= processar_probabilidades_combustivel_comleves(matriz_comleves_com_etanol,
                                                                                                      matriz_comleves_sem_etanol, 
                                                                                                      frota_proporcao_comleves_83, 
                                                                                                      frota_proporcao_comleves_2003, 
                                                                                                      frota_proporcao_comleves_2006, 
                                                                                                      frota_proporcao_comleves_2007)

matriz_comleves_com_etanol = atribuir_consumo_combustivel(matriz_comleves_com_etanol, consumo_gasolina,
                                                          consumo_etanol, consumo_oleo,
                                                          'PROPORCAO COMERCIAIS LEVES')
matriz_comleves_sem_etanol = atribuir_consumo_combustivel(matriz_comleves_sem_etanol, 
                                                          consumo_gasolina,
                                                          consumo_etanol, consumo_oleo, 
                                                          'PROPORCAO COMERCIAIS LEVES')
autonomia_comleves = carregar_autonomia(caminho_diretorio,
                                        codigos_combustivel_autonomia,
                                        "EF_LightCommercial.xlsx")
matriz_comleves_com_etanol = adicionar_autonomia(matriz_comleves_com_etanol
                                                 , autonomia_comleves)
matriz_comleves_sem_etanol = adicionar_autonomia(matriz_comleves_sem_etanol, 
                                                 autonomia_comleves)
matriz_comleves = unindo_matrizes(matriz_comleves_com_etanol,
                                  matriz_comleves_sem_etanol)

#%%
fator_emissao_motos= carregar_fator_emissao(caminho_diretorio, "EF_Evaporative_MotorCycle -eea.xlsx")
matriz_motos_com_etanol, matriz_motos_sem_etanol = processar_matrizes_etanol(frota_categoria_processada,
                                                                             consumo_etanol,
                                                                             fator_emissao_motos)
matriz_motos_sem_etanol= substituir_combustivel_fe_municipios_sem_etanol(matriz_motos_sem_etanol
                                                                         , fator_emissao_motos)
matriz_motos_sem_etanol = padronizar_combustivel(matriz_motos_sem_etanol)
matriz_motos_com_etanol = padronizar_combustivel(matriz_motos_com_etanol)

#Adicionando variáveis de cálculo 
matriz_motos_com_etanol = adicionando_prob_ano_modelo(matriz_motos_com_etanol, frota_processada_prob_ano_modelo, 'PROBABILIDADE MOTOS')
matriz_motos_sem_etanol = adicionando_prob_ano_modelo(matriz_motos_sem_etanol, frota_processada_prob_ano_modelo, 'PROBABILIDADE MOTOS')
matriz_motos_com_etanol, matriz_motos_sem_etanol = processar_probabilidades_combustivel_motos(matriz_motos_com_etanol, matriz_motos_sem_etanol, 
                                                                                                    frota_proporcao_motos_2003, frota_proporcao_motos_2010)

matriz_motos_com_etanol = atribuir_consumo_combustivel(matriz_motos_com_etanol, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO MOTOS')
matriz_motos_sem_etanol = atribuir_consumo_combustivel(matriz_motos_sem_etanol, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO MOTOS')
autonomia_motos = carregar_autonomia(caminho_diretorio, codigos_combustivel_autonomia, "EF_MotorCycle.xlsx")

matriz_motos_com_etanol = adicionar_autonomia(matriz_motos_com_etanol, autonomia_motos)
matriz_motos_sem_etanol = adicionar_autonomia(matriz_motos_sem_etanol, autonomia_motos)
matriz_motos = unindo_matrizes(matriz_motos_com_etanol, matriz_motos_sem_etanol)
matriz_motos = numero_motos(matriz_motos)

#%%
## Cálculo Final
temperatura_media = carregar_temperatura_media(caminho_diretorio)

matriz_leves_completa = adicionar_temp_uso_dias(matriz_leves, temperatura_media, 'AUTOMOVEIS')
matriz_comleves_completa = adicionar_temp_uso_dias(matriz_comleves, temperatura_media, 'COMERCIAIS LEVES')
matriz_motos_completa = adicionar_temp_uso_dias(matriz_motos, temperatura_media, 'MOTOS')

matriz_leves_completa = calculo_emissao_diurnal(matriz_leves_completa, 'AUTOMOVEIS')
matriz_comleves_completa = calculo_emissao_diurnal(matriz_comleves_completa, 'COMERCIAIS LEVES')
matriz_motos_completa = calculo_emissao_diurnal(matriz_motos_completa, 'MOTOS')

matriz_leves_completa = calculo_emissao_hotsoak(matriz_leves_completa, 'AUTOMOVEIS')
matriz_comleves_completa = calculo_emissao_hotsoak(matriz_comleves_completa, 'COMERCIAIS LEVES')
matriz_motos_completa = calculo_emissao_hotsoak(matriz_motos_completa, 'MOTOS')

matriz_leves_completa = calculo_emissao_running_losses(matriz_leves_completa, 'AUTOMOVEIS')
matriz_comleves_completa = calculo_emissao_running_losses(matriz_comleves_completa, 'COMERCIAIS LEVES')
matriz_motos_completa = calculo_emissao_running_losses(matriz_motos_completa, 'MOTOS')

#%% leves exaustivas

##LEVES
fator_emissao_leves_exaustivas = carregar_fator_emissao(caminho_diretorio, "EF_LightDuty.xlsx")
matriz_leves_com_etanol_exaustivas, matriz_leves_sem_etanol_exaustivas = processar_matrizes_etanol(frota_categoria_processada,
                                                                                                    consumo_etanol, fator_emissao_leves_exaustivas)
matriz_leves_sem_etanol_exaustivas= substituir_combustivel_fe_municipios_sem_etanol_exaustivas(matriz_leves_sem_etanol_exaustivas, 
                                                                                               fator_emissao_leves_exaustivas)
matriz_leves_sem_etanol_exaustivas = padronizar_combustivel(matriz_leves_sem_etanol_exaustivas)
matriz_leves_com_etanol_exaustivas = padronizar_combustivel(matriz_leves_com_etanol_exaustivas)
matriz_leves_com_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_leves_com_etanol_exaustivas,
                                                                  frota_processada_prob_ano_modelo,
                                                                    'PROBABILIDADE LEVES') #A partir daqui, as probabilidades são adicionadas com 'PROBABILIDADE ANO MODELO ...'
matriz_leves_sem_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_leves_sem_etanol_exaustivas, frota_processada_prob_ano_modelo, 'PROBABILIDADE LEVES')
matriz_leves_com_etanol_exaustivas, matriz_leves_sem_etanol_exaustivas = processar_probabilidades_combustivel_leves(matriz_leves_com_etanol_exaustivas, matriz_leves_sem_etanol_exaustivas, 
                                                                                                frota_proporcao_leves_82, frota_proporcao_leves_2003, 
                                                                                                frota_proporcao_leves_2007)
matriz_leves_com_etanol_exaustivas = atribuir_consumo_combustivel(matriz_leves_com_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO LEVES')
matriz_leves_sem_etanol_exaustivas = atribuir_consumo_combustivel(matriz_leves_sem_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO LEVES')
matriz_leves_exaustivas = unindo_matrizes(matriz_leves_com_etanol_exaustivas, matriz_leves_sem_etanol_exaustivas)
matriz_leves_exaustivas= manter_colunas_exaustivas(matriz_leves_exaustivas, 'AUTOMOVEIS') ##ajustar p manter prob ano modelo conforme cat 
matriz_leves_exaustivas = aplicar_deterioracao_veiculos(matriz_leves_exaustivas, deter_leves_otto, 'leves')
matriz_leves_exaustivas = calcular_emissoes_veiculares_exaustivas(matriz_leves_exaustivas, 'AUTOMOVEIS')

#%% Comercial Leve

#COMERCIAIS LEVES
fator_emissao_comleves_exaustivas = carregar_fator_emissao(caminho_diretorio, "EF_LightCommercial.xlsx")
matriz_comleves_com_etanol_exaustivas, matriz_comleves_sem_etanol_exaustivas = processar_matrizes_etanol(frota_categoria_processada, consumo_etanol, fator_emissao_comleves_exaustivas)
matriz_comleves_sem_etanol_exaustivas= substituir_combustivel_fe_municipios_sem_etanol_exaustivas(matriz_comleves_sem_etanol_exaustivas, fator_emissao_comleves_exaustivas)
matriz_comleves_sem_etanol_exaustivas = padronizar_combustivel(matriz_comleves_sem_etanol_exaustivas)
matriz_comleves_com_etanol_exaustivas = padronizar_combustivel(matriz_comleves_com_etanol_exaustivas)
# display(matriz_comleves_com_etanol_exaustivas)
matriz_comleves_com_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_comleves_com_etanol_exaustivas, frota_processada_prob_ano_modelo, ['PROBABILIDADE COMLEVES', 'PROBABILIDADE COMLEVES DIESEL'])
# display(matriz_comleves_com_etanol_exaustivas)
matriz_comleves_sem_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_comleves_sem_etanol_exaustivas, frota_processada_prob_ano_modelo, ['PROBABILIDADE COMLEVES', 'PROBABILIDADE COMLEVES DIESEL'])
matriz_comleves_com_etanol_exaustivas, matriz_comleves_sem_etanol_exaustivas= processar_probabilidades_combustivel_comleves(matriz_comleves_com_etanol_exaustivas, matriz_comleves_sem_etanol_exaustivas, 
                                                                                                      frota_proporcao_comleves_83, frota_proporcao_comleves_2003, 
                                                                                                      frota_proporcao_comleves_2006, frota_proporcao_comleves_2007)
matriz_comleves_com_etanol_exaustivas = atribuir_consumo_combustivel(matriz_comleves_com_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO COMERCIAIS LEVES')
matriz_comleves_sem_etanol_exaustivas = atribuir_consumo_combustivel(matriz_comleves_sem_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO COMERCIAIS LEVES')
matriz_comleves_exaustivas = unindo_matrizes(matriz_comleves_com_etanol_exaustivas, matriz_comleves_sem_etanol_exaustivas)

matriz_comleves_exaustivas= manter_colunas_exaustivas(matriz_comleves_exaustivas, 'COMERCIAIS LEVES')
matriz_comleves_exaustivas = aplicar_deterioracao_veiculos(matriz_comleves_exaustivas, [deter_leves_otto, deter_leves_diesel], 'comerciais_leves') 
matriz_comleves_exaustivas = calcular_emissoes_veiculares_exaustivas(matriz_comleves_exaustivas, 'COMERCIAIS LEVES') ##Aparentemente usa sempre prob com leves (coluna prob com leves diesel nao aparece na func original)


#%%
##motos
probabilidade_motorizacao_motos = prob_motorizacao_motos(frota_categoria_processada)

fator_emissao_motos_exaustivas = carregar_fator_emissao(caminho_diretorio, "EF_MotorCycle.xlsx")
matriz_motos_com_etanol_exaustivas, matriz_motos_sem_etanol_exaustivas = processar_matrizes_etanol(frota_categoria_processada, consumo_etanol, fator_emissao_motos_exaustivas)
matriz_motos_sem_etanol_exaustivas= substituir_combustivel_fe_municipios_sem_etanol_exaustivas(matriz_motos_sem_etanol_exaustivas, fator_emissao_motos_exaustivas)
matriz_motos_sem_etanol_exaustivas = padronizar_combustivel(matriz_motos_sem_etanol_exaustivas)
matriz_motos_com_etanol_exaustivas = padronizar_combustivel(matriz_motos_com_etanol_exaustivas)
matriz_motos_com_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_motos_com_etanol_exaustivas, frota_processada_prob_ano_modelo, 'PROBABILIDADE MOTOS')
matriz_motos_sem_etanol_exaustivas = adicionando_prob_ano_modelo(matriz_motos_sem_etanol_exaustivas, frota_processada_prob_ano_modelo, 'PROBABILIDADE MOTOS')
matriz_motos_com_etanol_exaustivas, matriz_motos_sem_etanol_exaustivas = processar_probabilidades_combustivel_motos(matriz_motos_com_etanol_exaustivas, matriz_motos_sem_etanol_exaustivas, 
                                                                                                frota_proporcao_motos_2003, frota_proporcao_motos_2010)

matriz_motos_com_etanol_exaustivas = atribuir_consumo_combustivel(matriz_motos_com_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO MOTOS')
matriz_motos_sem_etanol_exaustivas = atribuir_consumo_combustivel(matriz_motos_sem_etanol_exaustivas, consumo_gasolina, consumo_etanol, consumo_oleo, 'PROPORCAO MOTOS')
matriz_motos_exaustivas = unindo_matrizes(matriz_motos_com_etanol_exaustivas, matriz_motos_sem_etanol_exaustivas)
matriz_motos_exaustivas = adicionar_probabilidade_motorizacao(matriz_motos_exaustivas, probabilidade_motorizacao_motos)
matriz_motos_exaustivas = aplicar_deterioracao_veiculos(matriz_motos_exaustivas, deter_motos_otto, 'motos')
    
#%% Pesados Emily

# PESADOS EXAUSTIVAS ##FUNÇÕES NAO IDENTIFICADAS AQUI ESTAO NOS ARQUIVOS PY RESPECTIVOS
#CORRETO
probabilidade_subcategoria_pesados = calcular_matriz_probabilidade_pesados(frota_categoria_processada)

#Carregando os fatores de emissão
fator_emissao_pesados_exaustivas = carregar_fator_emissao(caminho_diretorio,
                                                          "EF_HeavyDuty.xlsx")

matriz_pesados_exaustivas = adicionar_fatores_emissao_pesados(frota_categoria_processada,
                                                              fator_emissao_pesados_exaustivas)

matriz_pesados_exaustivas = padronizar_combustivel(matriz_pesados_exaustivas)

matriz_pesados_exaustivas = adicionando_prob_ano_modelo(matriz_pesados_exaustivas,
                                                        frota_processada_prob_ano_modelo,
                                                        ['PROBABILIDADE CAMINHOES DIESEL',
                                                         'PROBABILIDADE ONIBUS DIESEL'])

matriz_pesados_exaustivas = atribuir_consumo_combustivel(matriz_pesados_exaustivas,
                                                         consumo_gasolina,
                                                         consumo_etanol,
                                                         consumo_oleo,
                                                         'PROPORCAO PESADOS')


deter_pesados.iloc[:, 3:] = (deter_pesados.iloc[:, 3:] > 0).astype(int)



matriz_pesados_exaustivas = aplicar_deterioracao_veiculos(matriz_pesados_exaustivas,
                                                          deter_pesados,
                                                          'pesados')

(
matriz_pesados_exaustivas
) = adicionar_probabilidade_subcategoria_pesados(matriz_pesados_exaustivas,
                                                 probabilidade_subcategoria_pesados)

(
matriz_pesados_exaustivas
) = calcular_emissoes_veiculares_exaustivas_pesados(matriz_pesados_exaustivas)


resultados_pesados_exaustivas = matriz_pesados_exaustivas[['ANO','MES','UF','MUNICIPIO','CODIGO IBGE',
                                                           'EMISSAO CO', 'EMISSAO HCTOT','EMISSAO CH4',
                                                           'EMISSAO NOX', 'EMISSAO MP', 'EMISSAO N2O']]

resultados_pesados_exaustivas = (resultados_pesados_exaustivas
                                 .groupby(['ANO','MES','UF',
                                           'MUNICIPIO','CODIGO IBGE'])
                                 [['EMISSAO CO', 'EMISSAO HCTOT','EMISSAO CH4',
                                   'EMISSAO NOX', 'EMISSAO MP', 'EMISSAO N2O']]
                                 .sum()
                                 .reset_index())


resultados_pesados_exaustivas['EMISSAO NMHC'] = (resultados_pesados_exaustivas['EMISSAO HCTOT'] -
                                                 resultados_pesados_exaustivas['EMISSAO CH4'])


florinaopolis = resultados_pesados_exaustivas[resultados_pesados_exaustivas['MUNICIPIO'] == 'FLORIANOPOLIS']
# florinaopolis.to_csv('florianopolis.csv')
colunas_emissoes = ['EMISSAO CO', 'EMISSAO HCTOT', 'EMISSAO CH4', 'EMISSAO NOX',
                    'EMISSAO NMHC', 'EMISSAO MP', 'EMISSAO N2O']
#%% CALCULO VKT


# Calcular VKT Pesados 
# Calcular VKT para cade categoria e cada ano modelo
matriz_pesados_exaustivas['VKT_PESADOS'] = (
    matriz_pesados_exaustivas['PROB_ANO_MODELO'] * # Probabilidade de ocorrencia do ano e modelo
    matriz_pesados_exaustivas['PROB_SUBCATEGORIA'] * # Probabilidade de ocorrencia da subcatgoria
    matriz_pesados_exaustivas['CONSUMO DIESEL CATEGORIA'] * # Consumo de combustivel da categoria (L)
    matriz_pesados_exaustivas['AUTONOMIA'] # Autonomia (Km/L)
)


# Agrupar por municípios
VKT_PESADOS = (matriz_pesados_exaustivas
                                 .groupby(['ANO','MES','UF',
                                           'MUNICIPIO','CODIGO IBGE'])
                                 [['VKT_PESADOS']]
                                 .sum()
                                 .reset_index())



# Calculuar VKT leves
# Calcular VKT para cade categoria e cada ano modelo
matriz_leves_exaustivas['VKT_LEVES'] = (
    matriz_leves_exaustivas['PROBABILIDADE ANO MODELO'] * # Probabilidade de ocorrencia do ano e modelo
    matriz_leves_exaustivas['PROBABILIDADE USO COMBUSTIVEL']* # Probabilidade de ocorrencia do combustivel
    matriz_leves_exaustivas['AUTONOMIA'] * # Autonomia (Km/L)
    matriz_leves_exaustivas['CONSUMO UTILIZADO'] # Consumo respectivo do combustivel utiliza
)

# Agrupar por municípios
VKT_LEVES = (matriz_leves_exaustivas
                                 .groupby(['ANO','MES','UF',
                                           'MUNICIPIO','CODIGO IBGE'])
                                 [['VKT_LEVES']]
                                 .sum()
                                 .reset_index())
#Matriz com leves
matriz_comleves_exaustivas['VKT_COMLEVES'] = (
    matriz_comleves_exaustivas['PROBABILIDADE ANO MODELO'] *
    matriz_comleves_exaustivas['PROBABILIDADE USO COMBUSTIVEL']*
    matriz_comleves_exaustivas['AUTONOMIA'] *
    matriz_comleves_exaustivas['CONSUMO UTILIZADO']
)

VKT_COMLEVES = (matriz_comleves_exaustivas
                                 .groupby(['ANO','MES','UF',
                                           'MUNICIPIO','CODIGO IBGE'])
                                 [['VKT_COMLEVES']]
                                 .sum()
                                 .reset_index())

VKT_TOTAL = (
    VKT_PESADOS
    .merge(VKT_LEVES, on=['ANO','MES','UF','MUNICIPIO','CODIGO IBGE'], how='outer')
    .merge(VKT_COMLEVES, on=['ANO','MES','UF','MUNICIPIO','CODIGO IBGE'], how='outer')
)

VKT_TOTAL['TOTAL'] = VKT_TOTAL['VKT_PESADOS'] +VKT_TOTAL['VKT_LEVES'] + VKT_TOTAL ['VKT_COMLEVES']
FLP = VKT_TOTAL[VKT_TOTAL['MUNICIPIO'] == 'FLORIANOPOLIS']
print((VKT_TOTAL['TOTAL'].sum()*12).astype(int))
#%%
import geopandas as gpd
import shapely
mun = gpd.read_file('/home/marcosperrude/Downloads/SC_Municipios_2024/SC_Municipios_2024.shp')

grande_floripa = [
    'Florianópolis', 'São José', 'Palhoça', 'Biguaçu', 'Governador Celso Ramos',
    'Santo Amaro da Imperatriz', 'Águas Mornas', 'Canelinha', 'Nova Trento'
]

# Filtrar apenas esses municípios
grande_flori = mun[mun['NM_RGINT'] == 'Florianópolis']

popula = pd.read_csv('/home/marcosperrude/Downloads/1.csv', thousands='.')
popula.rename(columns={"NOME DO MUNICÍPIO": "NM_MUN"}, inplace=True)


grande_flori.drop(4, inplace=True)

grande_flori = grande_flori.merge(popula, on='NM_MUN', how='left')

from shapely.ops import unary_union
import geopandas as gpd

# Une todas as geometrias
grade_flori_union_geom = unary_union(grande_flori.geometry)

# Cria um GeoDataFrame com a geometria unida e o CRS original
grade_flori_union = gpd.GeoDataFrame({'geometry': [grade_flori_union_geom]}, crs=grande_flori.crs)


grade_flori_union.to_file('GrandeFlorianopolis_unido.gpkg')

#%%
# import geopandas as gpd

# vh = pd.read_parquet(caminho_diretorio + '/vehicle_count_daily-2025-07-09 00_00_00_to_2025-07-10 00_00_00_rev1.parquet')

# vh.plot()


# sp = gpd.read_file('/home/marcosperrude/Documents/LCQAR/emiResidenciais/inputs/BR_Municipios_2022/BR_Municipios_2022.shp')
# sp = sp[sp['NM_MUN'] =='São Paulo']
# sp.to_file('sp.shp', driver='ESRI Shapefile')
#%% Exibindo os resultados evaporativas

print("Exibindo emissões leves")
## Verificando emissão DIURNAL por município e valor total
matriz_leves_consolidado = matriz_leves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO DIURNAL'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_leves_consolidado['EMISSAO DIURNAL'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões diurnal é: {soma} ton/mês")
# display(matriz_leves_consolidado[matriz_leves_consolidado['CODIGO IBGE']==4205407])

## Verificando emissão HOT SOAK por município e valor total
matriz_leves_consolidado = matriz_leves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO HOT SOAK'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_leves_consolidado['EMISSAO HOT SOAK'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões hot soak é: {soma} ton/mês") #VERIFICAR MUNIC NULOS
# display(matriz_leves_consolidado[matriz_leves_consolidado['CODIGO IBGE']==4205407])

## Verificando emissão RUNNING LOSSES por município e valor total
matriz_leves_consolidado = matriz_leves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO RUNNING LOSSES'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_leves_consolidado['EMISSAO RUNNING LOSSES'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões hot soak é: {soma} ton/mês") #VERIFICAR MUNIC NULOS
# display(matriz_leves_consolidado[matriz_leves_consolidado['CODIGO IBGE']==4205407])


print("Exibindo emissões comerciais leves")
## Verificando emissão DIURNAL por município e valor total
matriz_comleves_consolidado = matriz_comleves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO DIURNAL'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_comleves_consolidado['EMISSAO DIURNAL'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões diurnal é: {soma} ton/mês")
# display(matriz_comleves_consolidado[matriz_comleves_consolidado['CODIGO IBGE']==4205407])


## Verificando emissão HOT SOAK por município e valor total
matriz_comleves_consolidado = matriz_comleves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO HOT SOAK'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_comleves_consolidado['EMISSAO HOT SOAK'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões hot soak é: {soma} ton/mês") 
# display(matriz_comleves_consolidado[matriz_comleves_consolidado['CODIGO IBGE']==4205407])


## Verificando emissão running losses por município e valor total
matriz_comleves_consolidado = matriz_comleves_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO RUNNING LOSSES'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_comleves_consolidado['EMISSAO RUNNING LOSSES'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões running losses é: {soma} ton/mês")
# display(matriz_comleves_consolidado[matriz_comleves_consolidado['CODIGO IBGE']==4205407])

print("Exibindo emissões motos")
## Verificando emissão DIURNAL por município e valor total
matriz_motos_consolidado = matriz_motos_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO DIURNAL'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_motos_consolidado['EMISSAO DIURNAL'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões diurnal é: {soma} ton/mês")


## Verificando emissão HOT SOAK por município ': 'Diesel',
        'GASOLINA/ALCOOL/GAS NATURAL': 'GNV',
        'GAS NATURAL VEICULAR': 'GNV',
        'GAS METANO': 'GNV',
        'GASOL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'GASOLINA/GAS NATURAL VEICULAR': 'GNV',
        'ALCOOL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'ALCOOL/GAS NATURAL VEICULAR': 'GNV',
        'GASOGENIO': 'GNV',
        'DIESEL/GAS NATURAL VEICULAR': 'GNV',
        'DIESEL/GAS NATURAL COMBUSTIVEL': 'GNV',
        'GAS/NATURAL/LIQUEFEITO': 'GNV',
        'GASOLINA/ALCOOL/ELETRICO': 'Não considerado',
        'DIESEL/ELETRICO': 'Não considerado',
        'ETANOL/ELETRICO': 'Não considerado',
        'VIDE/CAMPO/OBSERVACAO': 'Não considerado',
        'HIBRIDO PLUG-IN': 'Não considerado',
        'ELETRICO/FONTE EXTERNA': 'Não considerado',
        'ELETRICO/FONTE INTERNA': 'Não considerado',
        'Sem Informação': 'Não considerado',
        'Não Identificado': 'Não considerado',
        'Não se Aplica': 'Não considerado',e valor total
matriz_motos_consolidado = matriz_motos_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO HOT SOAK'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_motos_consolidado['EMISSAO HOT SOAK'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões hot soak é: {soma} ton/mês") #VERIFICAR MUNIC NULOS

## Verificando emissão RUNNING LOSSES por município e valor total
matriz_motos_consolidado = matriz_motos_completa.groupby(['CODIGO IBGE', 'MES'])['EMISSAO RUNNING LOSSES'].sum().reset_index()

# Somando a coluna 'EMISSAO'
soma_emissao_total = matriz_motos_consolidado['EMISSAO RUNNING LOSSES'].sum()
soma = soma_emissao_total/10**6
# Exibindo o resultado
print(f"A soma total das emissões hot soak é: {soma} ton/mês") #VERIFICAR MUNIC NULOS








