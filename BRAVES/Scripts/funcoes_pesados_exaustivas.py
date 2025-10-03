# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 15:02:28 2025

@author: Marcos.Perrude
"""


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
        'Não se Aplica': 'Não considerado',
        'CELULA COMBUSTIVEL': 'Não considerado'}

# Mapeamento dos tipos de combustível
codigos_combustivel_mai = {'ETANOL HIDRATADO': 1, 'DIESEL': 2,
                           'FLEX-ETANOL HIDRATADO': 3, 'FLEX-GASOLINA C': 4,
                           'GASOLINA C': 5}

codigos_combustivel_min = {'Etanol': 1,'Diesel': 2,'Flex Etanol': 3,
                           'Flex Gasolina': 4, 'Gasolina C': 5}

# Mapeamento dos tipos de combustível de acordo com strings do df de autonomia


def identificando_cod_ibge(caminho_diretorio):
    
    """
    Processa e extrai informações dos códigos do IBGE para estados e municípios 
    do Brasil

    Parâmetros:
    caminho_diretorio (str): Caminho do diretório onde estão localizados os 
    arquivos

    Retorna:
        list[tuple]: Lista de tuplas contendo (nome do município, código do 
        município IBGE).

        list[tuple]: Lista de tuplas contendo (nome do estado, código do estado
        IBGE).

        list[str]: Lista de siglas das unidades federativas (UF).
        
        numpy.ndarray: Array com os códigos numéricos das unidades federativas.
    """
    
    # Leitura do arquivo Excel
    filename = f'{caminho_diretorio}/RELATORIO_DTB_BRASIL_MUNICIPIO.xls'
    try:
        df = pd.read_excel(filename, skiprows=6)  # Pula as primeiras 6 linhas
    except Exception as e:
        print(f"Erro ao ler o arquivo Excel: {e}")
        return None, None, None, None
    
    # Renomeação das colunas
    df = df.rename(columns={
        'UF': 'cod_uf',
        'Nome_UF': 'nome_uf',
        'Código Município Completo': 'cod_municipio',
        'Nome_Município': 'nome_municipio'})
    
    # Criação da lista de municípios
    IBGE_CITIES = df[['cod_uf', 'nome_uf',
                      'cod_municipio', 'nome_municipio']].values.tolist()
    IBGE_CITIES_CODES = df['cod_municipio'].astype(str).values
    
    # Leitura do shapefile dos estados
    try:
        UFshp = gpd.read_file(f'{caminho_diretorio}/BR_UF_2023/BR_UF_2023.shp')
    except Exception as e:
        print(f"Erro ao ler o shapefile dos estados: {e}")
        return None, None, None, None
    
    # Extração de códigos e nomes dos estados
    codigos_uf = UFshp['CD_UF'].astype(int).values
    UFnames = UFshp['NM_UF'].values
    
    # Dicionário de mapeamento de códigos para siglas
    codigo_para_sigla = {
        11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
        21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
        28: 'SE', 29: 'BA', 31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP', 41: 'PR',
        42: 'SC', 43: 'RS', 50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'}
    
    # Gerar ibge_uf com base na ordem de codigos_uf
    ibge_uf = [codigo_para_sigla[codigo] for codigo in codigos_uf]
    
    # Leitura dos shapefiles dos municípios
    try:
        CITYshpIBGE = gpd.read_file(f'{caminho_diretorio}/BR_Municipios_2023/'
                                    'BR_Municipios_2023.shp')
    except Exception as e:
        print(f"Erro ao ler o shapefile dos municípios do IBGE: {e}")
        return None, None, None, None
    
    CITYcodesIBGE = CITYshpIBGE['CD_MUN'].astype(int).values
    CITYnamesIBGE = CITYshpIBGE['NM_MUN'].values
    
    try:
        CITYshpFOREST = gpd.read_file(f'{caminho_diretorio}/ForestGIS_Cidades_'
                                      'Brasil_pop2021_ibge/ForestGIS_Cidades_'
                                      'Brasil_pop2021_ibge.shp')
    except Exception as e:
        print(f"Erro ao ler o shapefile dos municípios do ForestGIS: {e}")
        return None, None, None, None
    
    CITYcodesFOREST = CITYshpFOREST['IBGECd'].astype(int).values
    CITYnamesFOREST = CITYshpFOREST['Nom_Mun'].values
    
    # Comparação de códigos entre diferentes fontes
    lia1 = np.isin(CITYcodesIBGE, IBGE_CITIES_CODES.astype(int))
    lia2 = np.isin(CITYcodesIBGE, CITYcodesFOREST)
    
    # Identificação de municípios faltantes
    Falta_Municipios = CITYnamesIBGE[~lia1]
    Falta_Codigo = CITYcodesIBGE[~lia1]
    
    # Correção de nomes de municípios faltantes
    Falta_Municipios = [unidecode(str(name)) for name in Falta_Municipios]
    
    # Adição de municípios faltantes na lista completa
    codiState = (Falta_Codigo // 10**5).astype(int)
    loccodes2 = np.searchsorted(codigos_uf, codiState)
    falta_codiState = UFnames[loccodes2]
    
    Municipios_faltantes = list(zip(falta_codiState, codiState, Falta_Codigo, Falta_Municipios))
    IBGE_CITIES.extend(Municipios_faltantes)
    
    # Ordenação da lista completa de municípios
    IBGE_CITIES.sort(key=lambda x: x[2])
    
    # Remoção de caracteres especiais e espaços
    CITYnames = [unidecode(str(city[3])).upper().replace(' ', '') for city in IBGE_CITIES]
    
    # Saída final
    ibge_dados_cidades = [(city[3], city[2]) for city in IBGE_CITIES]
    ibge_estados = list(zip(UFnames, codigos_uf))
    
    return ibge_dados_cidades, ibge_estados, ibge_uf, codigos_uf

def normalizar_nome_municipio(nome):
    
    """
    Normaliza nomes de municípios removendo espaços, caracteres especiais e apóstrofos
    
    Parâmetros:
        ibge_dados_cidades (list): Lista de tuplas com nomes e códigos IBGE dos municípios
    
    Retorna:
        list: Lista de tuplas com nomes e códigos IBGE dos municípios normalizados
    """
    
    # Converte para maiúsculas e remove espaços
    nome = nome.upper().strip()
    # Remove apóstrofos, hífens e outros caracteres especiais
    nome = re.sub(r"[\'\-]", "", nome)
    #Remove acentos e caracteres não-ASCII
    nome = unidecode(nome)
    # Remove todos os espaços internos
    nome = nome.replace(" ", "")

    return nome

def adicionando_codigo_ibge_mun_especiais_sem_espaco(df):
    
    """
    Adiciona manualmente os códigos IBGE para municípios com nomes problemáticos,
    mantendo todos os registros que não encontrarem correspondência.

    Parâmetros:
        df (pd.DataFrame): DataFrame com a coluna 'MUNICIPIO' já normalizada.
        
    Retorna:
        pd.DataFrame: DataFrame com os códigos IBGE preenchidos para os casos especiais.
    """
    
    mapa_mun_especiais = {

        'BOMJESUS': 5203500, #GO, era Bom Jesus de Góias antes
        'COUTODEMAGALHAES': 1706001, #TO Couto do Magalhães
        'FORTALEZADOTABOCAO': 1708254, #TO Era Fortaleza do Tabocão, mudou para Tabocão (no IBGE) 
        'JEQUIRICA': 2918209, #BA Jiquiriçá 
        'LAGEDODOTABOCAL': 2919058, #BA Lajedo do Tabocal
        'MUQUEMDESAOFRANCISCO': 2922250, #BA
        'SANTATERESINHA': 2928505, #BA Santa Terezinha
        'UNA': 2932507, #BA
        'AMPARODASERRA': 3102506, #MG Amparo do Serra
        'BARAODMONTEALTO': 3105509, #MG Barao do Monte Alto
        'BARAODOMONTEALTO': 3105509, #MG Barao do Monte Alto, escrito diferente em dfs de frota e consumo
        'BRASOPOLIS': 3108909, #MG Brazópolis
        'GOUVEA': 	3127602, #MG Gouveia
        'QUELUZITA': 3153806, #MG Queluzito
        'SAOTHOMEDASLETRAS': 3165206, #MG Sao Tome das Letras
        'POXOREO': 	5107008, #MT Poxoréu
        'SANTOANTONIODOLEVERGER': 5107800, #MT Santo Antônio de Leverger
        'VILABELADASANTISSIMATRINDA': 5105507, #MT Vila Bela da Santíssima Trindade
        'ELDORADODOSCARAJAS': 1502954, #PA Eldorado do Carajás
        'SANTAISABELDOPARA': 1506500, #PA Santa Izabel do Pará
        'SANTAREM': 2513653, #Era Santarem de PB, hoje é Joca Claudino
        'SAODOMINGOSDEPOMBAL': 2513968, #PB Sao Domingos
        'BELEMDESAOFRANCISCO': 2601607, #PE Belém do São Francisco
        'IGUARACI': 2606903, #PE Iguaracy
        'LAGOADOITAENGA': 2608503, #PE Lagoa de Itaenga
        'SAOFRANCISCODEASSISDOPIAU': 2209658, #PI São Francisco de Assis do Piauí
        'BELAVISTADOCAROBA': 4102752, #PR Bela Vista da Caroba
        'MUNHOZDEMELLO': 4116307, #PR Munhoz de Melo
        'PINHALDOSAOBENTO': 4119251,#PR Pinhal de São Bento
        'SANTACRUZDOMONTECASTELO': 	4123303, #PR Santa Cruz de Monte Castelo
        'ARMACAODEBUZIOS': 3300233.0, #RJ Armação dos Búzios
        'PARATI': 3303807, #RJ Paraty
        'TRAJANODEMORAIS': 3305901, #RJ Trajano de Moraes
        'AREZ': 2401206, #RN Arês 
        'ASSU': 2400208, #RN Açu
        'BOASAUDE': 2405306, #RN - Era Januário Cicco. no IBGE não atualizou nome
        'ESPIGAODOOESTE': 1100098, #RO Espigão D'Oeste
        'NOVADOMAMORE': 1100338, #RO Nova Mamoré
        'BALNEARIODEPICARRAS': 4212809, #SC Balneário Piçarras
        'LAGEADOGRANDE': 4209458, #SC Lajeado Grande
        'PRESIDENTECASTELOBRANCO': 4213906, #SC é Presidente Castello Branco
        'SAOLOURENCODOESTE': 4216909, #SC São Lourenço do Oeste
        'SAOMIGUELDOESTE': 4217204, #SC São Miguel do Oeste
        'AMPARODESAOFRANCISCO': 2800100, #SE Amparo do São Francisco
        'EMBU': 3515004, #SP agora se chama Embu das Artes
        'FLORINIA': 3516101, #SP Florínea
        'SAOVALERIODANATIVIDADE': 1720499, #SP agora se chama São Valério
        'OLHODAGUADASFLORES': 2705705, #AL Olho d'Água das Flores
        'OLHODAGUADOCASADO': 2705804, #AL Olho d'Água do Casado
        'OLHODAGUAGRANDE': 2705903, #AL Olho d'Água Grande
        'TANQUEDARCA': 2709004, #AL Tanque d'Arca
        'DIASDAVILA': 2910057, #BA Dias d'Ávila
        'XIQUEXIQUE': 2933604, #BA Xique-Xique
        'SAOJOAODALIANCA': 5220009, #PB São João do Rio do Peixe (nome antigo: São João d'Aliança)
        'SITIODABADIA': 5220702, #BA Sítio do Mato (nome antigo: Sítio da Bacia)
        'APICUMACU': 2100832, #MA Apicum-Açu
        'CONCEICAODOLAGOACU': 2103554, #MA Conceição do Lago-Açu
        'OLHODAGUADASCUNHAS': 2107407, #MA Olho d'Água das Cunhãs
        'PINDAREMIRIM': 2108504, #MA Pindaré-Mirim
        'BARAODEMONTEALTO': 3105509, #MG
        'GUARDAMOR': 3128600, #MG Guarda-Mor
        'OLHOSDAGUA': 3145455, #MG Olhos-d'Água
        'PINGODAGUA': 3150539, #MG Pingo-d'Água
        'SAPUCAIMIRIM': 3165404, #MG Sapucaí-Mirim
        'SEMPEIXE': 3165560, #MG Sem-Peixe
        'CONQUISTADOESTE': 5103361, #MT Conquista d'Oeste
        'FIGUEIROPOLISDOESTE': 5103809, #MT Figueirópolis d'Oeste
        'GLORIADOESTE': 5103957, #MT Glória d'Oeste
        'LAMBARIDOESTE': 5105234, #MT Lambari d'Oeste
        'MIRASSOLDOESTE': 5105622, #MT Mirassol d'Oeste
        'IGARAPEACU': 1503200, #PA Igarapé-Açu
        'IGARAPEMIRI': 1503309, #PA Igarapé-Miri
        'PAUDARCO': 2207793, #PA Pau d'Arco
        'PEIXEBOI': 1505601, #PA Peixe-Boi
        'TOMEACU': 1508001, #PA Tomé-Açu
        'MAEDAGUA': 2508703, #PB Mãe d'Água
        'OLHODAGUA': 2510402, #PB Olho d'Água
        'BARRADALCANTARA': 	2201176, #PI Barra d'Alcântara
        'OLHODAGUADOPIAUI': 2207108, #PI Olho d'Água do Piauí
        'PAUDARCODOPIAUI': 2207793, #PI Pau d'Arco do Piauí
        'DIAMANTEDOESTE': 4107157, #PR Diamante d'Oeste
        'ITAPEJARADOESTE': 4111209, #PR Itapejara d'Oeste
        'PEROLADOESTE': 4119004, #PR Pérola d'Oeste
        'RANCHOALEGREDOESTE': 4121356, #PR Rancho Alegre d'Oeste
        'SAOJORGEDOESTE': 4125209, #PR São Jorge d'Oeste
        'VARRESAI': 3306156, #RJ Varre-Sai
        'CEARAMIRIM': 2402600, #RN Ceará-Mirim
        'GOVERNADORDIXSEPTROSADO': 2404309, #RN Governador Dix-Sept Rosado
        'LAGOADANTA': 2406205, #RN Lagoa d'Anta
        'OLHODAGUADOBORGES': 2408409, #RN Olho d'Água do Borges
        'VENHAVER': 2414753, #RN Venha-Ver
        'ALTAFLORESTADOESTE': 1100015, #RO Alta Floresta d'Oeste
        'ALVORADADOESTE': 1100346, #RO Alvorada d'Oeste
        'ESPIGAODOESTE': 1100098, #RO Espigão d'Oeste
        'GUAJARAMIRIM': 1100106, #RO Guajará-Mirim
        'JIPARANA': 1100122, #RO Ji-Paraná
        'MACHADINHODOESTE': 1100130, #RO Machadinho d'Oeste
        'NOVABRASILANDIADOESTE': 1100148, #RO Nova Brasilândia d'Oeste
        'SANTALUZIADOESTE': 1100296, #RO Santa Luzia d'Oeste
        'SAOFELIPEDOESTE': 1101484, #RO São Felipe d'Oeste
        'ENTREIJUIS': 4306932, #RS Entre-Ijuís
        'NAOMETOQUE':	4312658, #RS Não-Me-Toque
        'SANTANADOLIVRAMENTO': 4317103, #RS Sant'Ana do Livramento
        'XANGRILA': 4323804, #RS Xangri-lá
        'GRAOPARA': 4206108, #SC Grão-Pará
        'HERVALDOESTE': 4206702, #SC Herval d'Oeste
        'ITAPORANGADAJUDA': 2803203, #SP Itaporanga d'Ajuda 
        'APARECIDADOESTE': 3502606, #SP Aparecida d'Oeste
        'ARCOIRIS': 3503356, #SP Arco-Íris
        'EMBUGUACU': 3515103, #SP Embu-Guaçu (não confundir com Embu das Artes)
        'ESTRELADOESTE': 3515202, #SP Estrela d'Oeste
        'GUARANIDOESTE': 3518008, #SP Guarani d'Oeste
        'PALMEIRADOESTE': 3535200, #SP Palmeira d'Oeste
        'PARIQUERAACU': 3536208, #SP Pariquera-Açu
        'SANTABARBARADOESTE': 3545803, #SP Santa Bárbara d'Oeste
        'SANTACLARADOESTE': 3546108, #SP Santa Clara d'Oeste
        'SANTARITADOESTE': 3547403, #SP Santa Rita d'Oeste
        'SAOJOAODOPAUDALHO': 3549300,} #SP São João do Pau d'Alho

    # Casos especiais de munícipios com mesmo nome 
    mapa_mun_com_uf = {
        ('PAUDARCO', 'TO'): 1716307,
        ('PAUDARCO', 'PA'): 1505551,}

    # Máscara: municípios que estão no dicionário geral ou no dicionário com UF 
    # e ainda não têm código
    mask = (
        (df['MUNICIPIO'].isin(mapa_mun_especiais.keys()) |
         df.apply(lambda row: (row['MUNICIPIO'], row['UF'])
                  in mapa_mun_com_uf,
                  axis=1))
        & df['CODIGO IBGE'].isna())

    # Aplica a lógica de mapeamento combinando os dois dicionários
    def resolver_codigo(row):
        chave_com_uf = (row['MUNICIPIO'], row['UF'])
        if chave_com_uf in mapa_mun_com_uf:
            return mapa_mun_com_uf[chave_com_uf]
        return mapa_mun_especiais.get(row['MUNICIPIO'])

    df.loc[mask, 'CODIGO IBGE'] = df.loc[mask].apply(resolver_codigo, axis=1)

    return df

def adicionando_dados_ibge_consumo_comb(consumo_combustivel, ibge_dados_cidades, ibge_uf, codigos_uf):
    
    """
    Adiciona o código IBGE aos dados de consumo de combustível com base nos 
    nomes dos municípios e estados

    Parâmetros:
        consumo_combustivel (pd.DataFrame): DataFrame com os dados de consumo 
        de combustível
        
        ibge_dados_cidades (list): Lista de tuplas com nomes e códigos IBGE dos
        municípios
        
        IBGE_STATES (list): Lista de tuplas com nomes e códigos dos estados
        
        ibge_uf (list): Lista de siglas dos estados
        
        codigos_uf (list): Lista de códigos dos estados

    Retorna:
        pd.DataFrame: DataFrame com os dados de consumo de combustível e o 
        código IBGE correspondente
    """
    
    # Criar DataFrame dos municípios do IBGE e normalizar os nomes
    ibge_dados_cidades_df = pd.DataFrame(ibge_dados_cidades,
                                         columns=['Cidade',
                                                  'CODIGO IBGE'])
    ibge_dados_cidades_df['Cidade'] = (ibge_dados_cidades_df['Cidade']
                                   .str.upper()
                                   .apply(unidecode)
                                   .str.replace(" ", ""))
    
    # Criar mapeamento de UF para código
    uf_to_code = dict(zip(ibge_uf, codigos_uf))
    consumo_combustivel['Codigo_UF'] = consumo_combustivel['UF'].map(uf_to_code)
    
    # Normalizar nomes dos municípios no DataFrame de consumo
    consumo_combustivel['MUNICIPIO'] = (consumo_combustivel['MUNICIPIO']
                                             .str.upper()
                                             .apply(unidecode)
                                             .str.replace(" ", ""))
    
    # Criar chave de junção combinando Município e Código UF
    ibge_dados_cidades_df['Chave'] = (ibge_dados_cidades_df['Cidade'] + 
                                      ibge_dados_cidades_df['CODIGO IBGE']
                                      .floordiv(100000)
                                      .astype(str))
    consumo_combustivel['Chave'] = (consumo_combustivel['MUNICIPIO'] +
                                    consumo_combustivel['Codigo_UF']
                                    .astype(str))
    
    # Fazer o merge para adicionar os códigos IBGE
    consumo_combustivel = (consumo_combustivel
                           .merge(ibge_dados_cidades_df[['Chave',
                                                         'CODIGO IBGE']],
                                  on='Chave', how='left'))
    
    # Tratamento de municípios específicos com correção de estado
    correcoes = {'SERRANOPOLIS': 'GO','RIOSONO': 'TO','PONTEALTADONORTE': 'SC',
                  'ITAPORADOTOCANTINS': 'TO'}
    
    for municipio, uf_corrigido in correcoes.items():
        mask = consumo_combustivel['MUNICIPIO'] == municipio
        consumo_combustivel.loc[mask, 'UF'] = uf_corrigido
        consumo_combustivel.loc[mask, 'Codigo_UF'] = uf_to_code[uf_corrigido]
    
    # Refazer a chave e tentar o merge novamente para os corrigidos
    consumo_combustivel['Chave'] = (consumo_combustivel['MUNICIPIO'] +
                                    consumo_combustivel['Codigo_UF']
                                    .astype(str))
    consumo_combustivel = (consumo_combustivel
                           .merge(ibge_dados_cidades_df[['Chave',
                                                         'CODIGO IBGE']],
                                                         on='Chave',
                                                         how='left',
                                                         suffixes=('',
                                                                   '_NOVO')))
    
    # Priorizar o código IBGE atualizado
    (
    consumo_combustivel['CODIGO IBGE']
    ) = (consumo_combustivel['CODIGO IBGE']
         .combine_first(consumo_combustivel['CODIGO IBGE_NOVO']))
    
    consumo_combustivel.drop(columns=['Chave', 'CODIGO IBGE_NOVO'],
                             inplace=True)
    
    # Consolidar os dados somando consumo por chave única
    consumo_final = (consumo_combustivel
                     .groupby(['ANO', 'MES', 'UF', 'MUNICIPIO', 'CODIGO IBGE'],
                              dropna=False,
                              as_index=False)
                              .agg({'CONSUMO': 'sum'}))
    
    return consumo_final

def adicionando_dados_ibge_frota(dados_frota, ibge_dados_cidades, ibge_uf, codigos_uf):
    
    """
    Adiciona o código IBGE aos dados de frota com base nos nomes dos municípios
    e estados

    Parâmetros:
        dados_frota (pd.DataFrame): DataFrame com os dados de frota
        
        ibge_dados_cidades (list): Lista de tuplas com nomes e códigos IBGE dos
        MUNICIPIOs
        
        IBGE_STATES (list): Lista de tuplas com nomes e códigos dos estados
        
        ibge_uf (list): Lista de siglas dos estados
        
        codigos_uf (list): Lista de códigos dos estados
    
    Retorna:
        pd.DataFrame: DataFrame com os dados de frota e o código IBGE
        correspondente
    """

    # Converter ibge_dados_cidades para DataFrame
    ibge_dados_cidades_df = pd.DataFrame(ibge_dados_cidades,
                                         columns=['Cidade', 'CODIGO IBGE'])

    # Normalizar nomes dos MUNICIPIOs em ibge_dados_cidades
    ibge_dados_cidades_df['Cidade'] = (ibge_dados_cidades_df['Cidade']
                                       .apply(lambda x: unidecode(x)
                                              .upper()
                                              .replace(" ", "")))

    # Criar um dicionário de mapeamento de UF para Código UF
    uf_to_code = {uf: code 
                  for uf,code 
                  in zip(ibge_uf, codigos_uf)}

    # Normalizar nomes dos MUNICIPIOs em dados_frota
    dados_frota['MUNICIPIO_NORM'] = dados_frota['MUNICIPIO'].apply(
        lambda x: unidecode(x).upper().replace(" ", ""))
    
    # Adicionar coluna de código UF ao DataFrame de frota
    dados_frota['Codigo_UF'] = dados_frota['UF'].map(uf_to_code)

    # Criar código UF para cada município em ibge_dados_cidades_df
    ibge_dados_cidades_df['Codigo_UF'] = ibge_dados_cidades_df['CODIGO IBGE'] // 100000

    # Fazer o merge dos dados com base no MUNICIPIO e Código UF
    frota_completa = dados_frota.merge(
        ibge_dados_cidades_df, 
        left_on=['MUNICIPIO_NORM', 'Codigo_UF'], 
        right_on=['Cidade', 'Codigo_UF'], 
        how='left')

    # Correções manuais para municípios problemáticos
    correcoes = {
        'SERRANOPOLIS': ('GO', 52),
        'RIOSONO': ('TO', 17),
        'PONTEALTADONORTE': ('SC', 42)}

    for municipio, (uf, codigo_uf) in correcoes.items():
        mask = ((frota_completa['MUNICIPIO_NORM'] == municipio) &
                frota_completa['CODIGO IBGE'].isna())
        frota_completa.loc[mask, ['UF', 'Codigo_UF']] = uf, codigo_uf

    # Tentar novamente encontrar os códigos IBGE para os municípios corrigidos
    frota_completa = frota_completa.merge(
        ibge_dados_cidades_df[['Cidade', 'Codigo_UF', 'CODIGO IBGE']],
        on=['Cidade', 'Codigo_UF'],
        how='left',
        suffixes=('', '_corrigido'))

    # Se encontrar o código na correção, substituir
    frota_completa['CODIGO IBGE'] = (
        frota_completa['CODIGO IBGE']
        .fillna(frota_completa['CODIGO IBGE_corrigido'])
        )
    frota_completa.drop(columns=['CODIGO IBGE_corrigido'], inplace=True)


    # Remover colunas auxiliares se existirem
    colunas_remover = ['MUNICIPIO_NORM', 'Cidade', 'Codigo_UF']
    colunas_remover = [col 
                       for col in colunas_remover
                       if col in frota_completa.columns]
    frota_completa.drop(columns=colunas_remover, inplace=True, errors='ignore')

    return frota_completa

#%% Funções de importação e processamento inicial das planilhas

def processamento_arquivos_frota_categoria(caminho_diretorio, 
                                           caminho_arquivos_frota_categoria,
                                           estados_brasileiros,
                                           meses_para_numeros):
    
    """
    Importa e processa os arquivos de frota por categoria.

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta contendo os arquivos de 
        entrada.

        caminho_arquivos_frota_categoria (str): Pasta contendo os arquivos de
        frota categoria
        
        estados_brasileiros (dict): Relacionando os estados com as respectivas
        siglas
        
        meses_para_numeros (dict): Relacionando os nomes dos meses com seu
        respectivo número
    
    Retorna:
        pd.DataFrame: DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF' e 
        as categorias de veículos, com colunas originais para a maioria das
        categorias e 'Automóveis', 'Comerciais Leves' e 'Não considerado'
        agregados.
    """ 
     
    caminho_arquivo = os.path.join(caminho_diretorio, 
                                   caminho_arquivos_frota_categoria)
    arquivos = [f 
                for f in os.listdir(caminho_arquivo) 
                if f.endswith('.xls') and not f.startswith('~$')]
    dfs_processados = []
    
    for arquivo in arquivos:
        # arquivo  = 'frota_munic_modelo_junho_2019.xls'
        try:
            # Extrair o ano e o mês do nome do arquivo
            partes_nome = arquivo.split('_')
            mes_nome = partes_nome[-2].lower() 
            ano = int(partes_nome[-1].split('.')[0]) 
            
            # Mapear o nome do mês para o número correspondente
            mes_numero = meses_para_numeros.get(mes_nome, None)
            if mes_numero is None:
                print(f"Mês '{mes_nome}' não reconhecido no arquivo {arquivo}.")
                continue
            
            print(f"Lendo dados de frota categoria para "
                  "{mes_nome.capitalize()} de {ano}")

            # Caminho completo do arquivo
            caminho_completo = os.path.join(caminho_arquivo, arquivo)
            df = pd.read_excel(caminho_completo, skiprows=3)
            
            # Criar mapeamento para as categorias agregadas
            df = df.assign(
                **{
                    'Automóveis': df[['AUTOMOVEL',
                                      'OUTROS']]
                                      .sum(axis=1),
                    'Comerciais Leves': df[['CAMINHONETE', 
                                            'CAMIONETA', 
                                            'UTILITARIO']]
                                            .sum(axis=1),
                    'Não considerado': df[['BONDE', 
                                           'REBOQUE', 
                                           'SEMI-REBOQUE', 
                                           'SIDE-CAR']]
                                           .sum(axis=1)
                })
            
            # Lista de colunas originais para manter (categorias de motos e
            # pesados)
            colunas_originais = [
                'CICLOMOTOR', 'MOTOCICLETA', 'MOTONETA', 'QUADRICICLO',
                'TRICICLO', 'CAMINHAO', 'CAMINHAO TRATOR', 'CHASSI PLATAF',
                'MICRO-ONIBUS', 'ONIBUS', 'TRATOR ESTEI', 'TRATOR RODAS'
            ]
            
            # Filtrar apenas colunas que existem no DataFrame
            colunas_para_manter = [col 
                                   for col in colunas_originais 
                                   if col in df.columns]
            
            # Criar DataFrame final com padronização
            frota_categoria_mapeado = df[['UF', 'MUNICIPIO']].copy()
            
            # Adicionar as colunas originais (em maiúsculas)
            for col in colunas_para_manter:
                frota_categoria_mapeado[col.upper()] = df[col]
            
            # Adicionar as colunas agregadas
            frota_categoria_mapeado = frota_categoria_mapeado.assign(
                **{
                    'AUTOMOVEIS': df['Automóveis'],
                    'COMERCIAIS LEVES': df['Comerciais Leves'],
                    'NAO CONSIDERADO': df['Não considerado'],
                    'ANO': ano,
                    'MES': mes_numero
                })
            
            # Reordenar colunas
            colunas_base = ['ANO', 'MES', 'UF', 'MUNICIPIO', 'AUTOMOVEIS',
                            'COMERCIAIS LEVES', 'NAO CONSIDERADO']
            colunas_ordenadas = (colunas_base +
                                 [col 
                                  for col in frota_categoria_mapeado.columns 
                                  if col not in colunas_base and col not in ['UF', 'MUNICIPIO']])
            
            frota_categoria_mapeado = frota_categoria_mapeado[colunas_ordenadas]
            
            # Processamento dos nomes das cidades
            frota_categoria_mapeado["MUNICIPIO"] = (
                frota_categoria_mapeado["MUNICIPIO"]
                .str.upper()
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace(r'[^A-Za-z]', '', regex=True))
            
            # Substituir nome dos UFs pela sigla
            frota_categoria_mapeado["UF"] = (frota_categoria_mapeado["UF"]
                                             .str.upper()
                                             .replace(estados_brasileiros))
            
            # Remover linhas com "MUNICIPIONAOINFORMADO"
            frota_categoria_mapeado = (frota_categoria_mapeado[
                ~frota_categoria_mapeado["MUNICIPIO"].str.contains(
                    "MUNICIPIONAOINFORMADO",
                    case=False,
                    na=False)]
                    .copy())
            
            # Converter nomes das colunas para caixa alta e remover acentos
            frota_categoria_mapeado.columns = [unidecode(col).upper()
                                               for col in frota_categoria_mapeado.columns]
            
            # Adicionar o DataFrame processado à lista de acumulação
            dfs_processados.append(frota_categoria_mapeado)
        
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
    
    # Concatenar todos os DataFrames processados em um único DataFrame
    frota_categoria_consolidada = pd.concat(dfs_processados, ignore_index=True)
    
    return frota_categoria_consolidada
def carregar_temperatura_media(caminho_diretorio):
    
    """
    Carrega e processa o arquivo de temperatura média
    
    Parâmetros:
        caminho_diretorio (str): Caminho para o diretório contendo o arquivo
    
    Retorna:
        pd.DataFrame: DataFrame com os dados de temperatura média processados
    """

    temperatura_media = pd.read_excel(
        caminho_diretorio + "/6.TemperaturaMediaNormalClimatologia1991-2020"
        "/Normal-Climatologica-TMEDSECA.xlsx",
        skiprows=2)
    temperatura_media = (temperatura_media
                         .rename(columns={col: col
                                          .lower()
                                          .replace("ç", "c") 
                                for col in temperatura_media.columns}
                                )
                        )
    temperatura_media.columns = [unidecode(col).upper().strip() for col in temperatura_media.columns]
    temperatura_media = temperatura_media.rename(columns={'ANO': 'TEMPERATURA MEDIA'})
    
    return temperatura_media




def carregar_fator_emissao(caminho_diretorio, nome_arquivo):
    # nome_arquivo = "EF_Evaporative_LightDuty.xlsx"
    """
    Carrega e processa arquivos de fator de emissão, sendo diferente para leves e comleves

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta contendo os arquivos
        nome_arquivo (str): Nome do arquivo de fator de emissão a ser carregado

    Retorna:
        pd.DataFrame: DataFrame contendo os fatores de emissão processados
    """
    
    fator_emissao = pd.read_excel(f"{caminho_diretorio}/7.FatorDeEmissaoEAutonomia/{nome_arquivo}")
    fator_emissao = fator_emissao.rename(columns={'Ano': 'ANO MODELO'})
    fator_emissao.columns = [unidecode(col).upper() for col in fator_emissao.columns]
    fator_emissao['COMBUSTIVEL'] = fator_emissao['COMBUSTIVEL'].replace({
        'Gasolina': 'GASOLINA C',
        'Etanol': 'ETANOL HIDRATADO',
        'Flex Gasolina': 'FLEX-GASOLINA C',
        'Flex Etanol': 'FLEX-ETANOL HIDRATADO',
        'Diesel': 'DIESEL',})

    return fator_emissao


def carregar_autonomia(caminho_diretorio, codigos_combustivel_autonomia, nome_arquivo):
    
    """
    Carrega e processa arquivos de autonomia, sendo diferente para leves e comleves

    Parâmetros:
        caminho_diretorio (str):Caminho da pasta contendo os arquivos
        
        codigos_combustivel_autonomia (dict): Dicionário contendo o mapeamento
        dos combustíveis para códigos
        
        nome_arquivo (str): Nome do arquivo 

    Retorna:
        pd.DataFrame: DataFrame processado contendo 'ANO MODELO', 'CODIGO COMBUSTIVEL' e 'AUTONOMIA'
    """
    
    autonomia = pd.read_excel(f"{caminho_diretorio}/7.FatorDeEmissaoEAutonomia/{nome_arquivo}")
    autonomia.columns = [unidecode(col).upper()
                         for col in autonomia.columns]
    autonomia['CODIGO COMBUSTIVEL'] = (autonomia['COMBUSTIVEL']
                                       .map(codigos_combustivel_autonomia))
    autonomia = autonomia[['ANO', 'CODIGO COMBUSTIVEL', 'AUTONOMIA']]
    autonomia = autonomia.rename(columns={'ANO': 'ANO MODELO'})
    
    return autonomia

#%% Processando Frota Catregoria e Subcategorias para motos e Pesados

def processamento_arquivos_frota_ano(caminho_diretorio, caminho_arquivos_frota_ano, estados_brasileiros, meses_para_numeros):
    
    """
    Importa e processa os arquivos de frota ano (ano modelo dos veículos)

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta contendo os arquivos de entrada.
        caminho_arquivos_frota_ano (str): Pasta conendo os arquivos de frota ano
        estados_brasileiros (dict): Relacionado os estados com as respectivas siglas
        meses_para_numeros (dict): Relacionando os nomes dos meses com seu respectivo número
    
    Retorna:
        pd.DataFrame: DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF' e 'ANO MODELO'
        list: lista contendo os anos dos dados que estão sendo processados
    """ 

    caminho_arquivo = Path(caminho_diretorio) / caminho_arquivos_frota_ano
    arquivos = [f for f in os.listdir(caminho_arquivo) if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('~$')]
    
    anos_dados = set() 
    dfs = [] 
    
    for arquivo in arquivos:
        try:
            partes_nome = arquivo.split('_')
            ano = int(partes_nome[-1].split('.')[0])
            mes_nome = partes_nome[-2].lower()
            mes_numero = meses_para_numeros.get(mes_nome)
            
            if mes_numero is None:
                raise ValueError(f"Mês '{mes_nome}' não encontrado no dicionário de meses.")
            
            file_path = caminho_arquivo / arquivo
            frota_ano = pl.read_excel(file_path)
            
            print(f"Lendo dados de frota ano modelo para {mes_nome.capitalize()} de {ano}")
            
            # Excluir coluna 'Ano Fabricação CRV'
            if "Ano Fabricação CRV" in frota_ano.columns:
                frota_ano = frota_ano.drop("Ano Fabricação CRV")
            
            # Converter 'Ano Modelo' para numérico
            frota_ano = frota_ano.with_columns(
                pl.col("Ano Modelo").cast(pl.Int64, strict=False))
            
            # Adicionar coluna do ano e do mês
            frota_ano = frota_ano.with_columns(
                pl.lit(ano).alias("Ano"),
                pl.lit(mes_numero).alias("MES"))
            
            # Adicionar o DataFrame ajustado à lista de DataFrames
            dfs.append(frota_ano)
            anos_dados.add(ano)
            
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
    
    # Concatenar todos os DataFrames da lista em um único DataFrame
    frota_ano_final = pl.concat(dfs)
    
    # Reordenar colunas
    colunas_ordenadas = ["Ano", "MES", "UF", "Município", "Ano Modelo", "Qtd. Veículos"]
    frota_ano_final = frota_ano_final.select(colunas_ordenadas)
    
    # Processamento dos nomes das cidades
    frota_ano_final = frota_ano_final.with_columns(
        pl.col("Município")
        .str.to_uppercase()
        .map_elements(lambda x: unidecode(x) if x is not None else x, return_dtype=pl.Utf8)
        .str.replace_all(r'[^A-Za-z]', ''))
    
    # Substituindo nome dos UFs pela sigla
    frota_ano_final = frota_ano_final.with_columns(
        pl.col("UF").str.to_uppercase().replace(estados_brasileiros))
    
    # Remover linhas com valores NaN na coluna 'Ano Modelo'
    frota_ano_final = frota_ano_final.drop_nulls("Ano Modelo")
    
    # Remover linhas com valores vazios ou "Sem Informação" em 'Qtd. Veículos'
    frota_ano_final = frota_ano_final.filter(pl.col("Qtd. Veículos").is_not_null())
    
    # Converter nomes das colunas para caixa alta e remover acentos
    frota_ano_final = frota_ano_final.rename({col: unidecode(col).upper() for col in frota_ano_final.columns})
    frota_ano_final = frota_ano_final.to_pandas()
    frota_ano_final = frota_ano_final[frota_ano_final['MUNICIPIO'] != 'SEMINFORMAASSAPSO'] 

    return frota_ano_final, list(anos_dados)

#%% Probabilidade Categorias Motos e Pesados

def prob_motorizacao_motos(frota_categoria_processada):
    
    """
    Calcula a probabilidade de motorização para veículos de duas rodas por
    município (CODIGO IBGE) gerando TODAS as categorias necessárias (pré e pós
    2010) baseado na frota atual. Versão otimizada sem a coluna TOTAL_MOTOS.

    Args:
        frota_categoria_processada (DataFrame): DataFrame contendo a frota por
        categorias.
               Deve conter as colunas: ['CODIGO IBGE', 'CICLOMOTOR',
                                        'MOTOCICLETA', 'MOTONETA', 
                                        'QUADRICICLO', 'TRICICLO']
        
    Returns:
        DataFrame: Contém CODIGO_IBGE, MOTORIZACAO_ID (1, 2, 3, 4), 
                  PROBABILIDADE_MOTORIZACAO
    """
    
    # Seleciona e agrupa os dados
    df_motos = (
        frota_categoria_processada[
            ['CODIGO IBGE', 'CICLOMOTOR', 'MOTOCICLETA', 'MOTONETA', 
             'QUADRICICLO', 'TRICICLO']
        ]
        .groupby('CODIGO IBGE')
        .first()
        .reset_index()
    )
    
    # Calcula os totais para filtrar municípios com frota positiva
    total_motos = df_motos[['CICLOMOTOR', 'MOTOCICLETA', 'MOTONETA',
                            'QUADRICICLO', 'TRICICLO']].sum(axis=1)
    df_motos = df_motos[total_motos > 0]
    
    # Calcula as categorias de motorização
    df_motos['ID_1'] = df_motos['CICLOMOTOR'] + df_motos['MOTOCICLETA']     # <150 cc
    df_motos['ID_2'] = df_motos['MOTONETA']                                # 150–500 cc
    df_motos['ID_3'] = df_motos['QUADRICICLO'] + df_motos['TRICICLO']      # >500 cc
    df_motos['ID_4'] = df_motos['MOTONETA'] + df_motos['QUADRICICLO'] + df_motos['TRICICLO']  # >150 cc

    # Calcula as probabilidades para todas as categorias
    for i in range(1, 5):
        df_motos[f'PROB_ID_{i}'] = (df_motos[f'ID_{i}'] /
                                    total_motos[total_motos > 0])
    
    # Prepara o DataFrame final usando melt para transformar colunas em linhas
    prob_motos = (
        df_motos.melt(
            id_vars=['CODIGO IBGE'],
            value_vars=[f'PROB_ID_{i}' for i in range(1, 5)],
            var_name='MOTORIZACAO_TEMP',
            value_name='PROBABILIDADE_MOTORIZACAO'
        )
    )
    
    # Mapeia os IDs de motorização corretamente
    prob_motos['MOTORIZACAO_ID'] = (
        prob_motos['MOTORIZACAO_TEMP']
        .str.extract(r'PROB_ID_(\d+)')
        .astype(int))
    
    # Seleciona e ordena as colunas finais
    prob_motos = (
        prob_motos[['CODIGO IBGE', 'MOTORIZACAO_ID',
                    'PROBABILIDADE_MOTORIZACAO']]
        .sort_values(['CODIGO IBGE', 'MOTORIZACAO_ID'])
        .rename(columns={'CODIGO IBGE': 'CODIGO_IBGE'})
        .reset_index(drop=True))
    
    return prob_motos

def adicionar_probabilidade_motorizacao(matriz_motos, prob_motorizacao):
    
    """
    Adiciona a probabilidade de motorização à matriz de motos baseada no código
    IBGE e categoria.
    Agora considera os 4 IDs de motorização: 
    1: < 150 cc
    2: >150 e < 500 cc
    3: > 501 cc
    4: >150 cc (agregado para veículos pós 2010)
    
    Parâmetros:
        matriz_motos (pd.DataFrame): DataFrame com dados de emissão das motos 
        (deve conter 'CODIGO IBGE' e 'CATEGORIA')
        
        prob_motorizacao (pd.DataFrame): DataFrame com as probabilidades de 
        motorização por código IBGE (deve conter CODIGO_IBGE, MOTORIZACAO_ID e
        PROBABILIDADE_MOTORIZACAO)
        
    Retorna:
        pd.DataFrame: Matriz de motos com as colunas de probabilidade adicionadas
    """
    
    # Verificar se as colunas necessárias existem
    required_motos_cols = ['CODIGO IBGE', 'CATEGORIA']
    missing_cols = [col for col in required_motos_cols if col not in matriz_motos.columns]
    if missing_cols:
        raise ValueError(f"O DataFrame de motos deve conter as colunas: {missing_cols}")
    
    required_prob_cols = ['CODIGO_IBGE', 'MOTORIZACAO_ID', 'PROBABILIDADE_MOTORIZACAO']
    missing_cols = [col for col in required_prob_cols if col not in prob_motorizacao.columns]
    if missing_cols:
        raise ValueError(f"O DataFrame de probabilidades deve conter as colunas: {missing_cols}")
    
    # Criar cópia para não modificar o original
    matriz_motos_com_prob = matriz_motos.copy()
    
    # Mapeamento completo das categorias para IDs de motorização
    mapeamento_categorias = {
        '< 150 cc': 1,
        '>150 e < 500 cc': 2,
        '> 501 cc': 3,
        '>150 cc': 4  # Nova categoria agregada para veículos pós 2010
    }
    
    # Criar coluna temporária com o ID de motorização
    matriz_motos_com_prob['MOTORIZACAO_ID'] = matriz_motos_com_prob['CATEGORIA'].map(mapeamento_categorias)
    
    # Verificar se todos os valores foram mapeados
    if matriz_motos_com_prob['MOTORIZACAO_ID'].isna().any():
        categorias_nao_mapeadas = matriz_motos_com_prob.loc[
            matriz_motos_com_prob['MOTORIZACAO_ID'].isna(), 'CATEGORIA'
        ].unique()
        raise ValueError(f"Categorias não mapeadas: {categorias_nao_mapeadas}")
    
    # Converter CODIGO IBGE para o mesmo tipo em ambos DataFrames
    matriz_motos_com_prob['CODIGO_IBGE'] = matriz_motos_com_prob['CODIGO IBGE'].astype(float)
    prob_motorizacao = prob_motorizacao.copy()
    prob_motorizacao['CODIGO_IBGE'] = prob_motorizacao['CODIGO_IBGE'].astype(float)
    
    # Realizar o merge considerando todos os IDs (1, 2, 3, 4)
    matriz_motos_com_prob = matriz_motos_com_prob.merge(
        prob_motorizacao[['CODIGO_IBGE', 'MOTORIZACAO_ID', 'PROBABILIDADE_MOTORIZACAO']],
        on=['CODIGO_IBGE', 'MOTORIZACAO_ID'],
        how='left'
    )
    
    # Remover a coluna temporária (mantemos CODIGO_IBGE para possível uso posterior)
    matriz_motos_com_prob.drop(columns=['MOTORIZACAO_ID'], inplace=True)
    
    # Preencher valores faltantes com 0 (caso não encontre a combinação código IBGE + ID)
    matriz_motos_com_prob['PROBABILIDADE_MOTORIZACAO'] = matriz_motos_com_prob['PROBABILIDADE_MOTORIZACAO'].fillna(0)
    
    # Restaurar o nome original da coluna de código IBGE se necessário
    if 'CODIGO IBGE' not in matriz_motos_com_prob.columns:
        matriz_motos_com_prob.rename(columns={'CODIGO_IBGE': 'CODIGO IBGE'}, inplace=True)
    
    return matriz_motos_com_prob

def numero_motos(matriz_motos):
    # Colunas que compõem o total de motos
    colunas_motos = ['CICLOMOTOR', 'MOTOCICLETA', 'MOTONETA', 'QUADRICICLO', 'TRICICLO']

    # Preencher possíveis valores ausentes com 0
    matriz_motos[colunas_motos] = matriz_motos[colunas_motos].fillna(0)

    # Selecionar a primeira ocorrência por município, ano e mês
    df_unico = (
        matriz_motos
        .drop_duplicates(subset=['CODIGO IBGE', 'ANO', 'MES'])
        .copy()
    )

    # Calcular a soma das colunas de motos
    df_unico['MOTOS'] = df_unico[colunas_motos].sum(axis=1)

    # Criar coluna de total de motos no dataframe original
    matriz_motos = matriz_motos.merge(
        df_unico[['CODIGO IBGE', 'ANO', 'MES', 'MOTOS']],
        on=['CODIGO IBGE', 'ANO', 'MES'],
        how='left'
    )

    # Remover colunas desnecessárias
    colunas_para_excluir = [
        'AUTOMOVEIS', 'COMERCIAIS LEVES', 'NAO CONSIDERADO',
        'CICLOMOTOR', 'MOTOCICLETA', 'MOTONETA', 'QUADRICICLO', 'TRICICLO',
        'CAMINHAO', 'CAMINHAO TRATOR', 'CHASSI PLATAF',
        'MICRO-ONIBUS', 'ONIBUS', 'TRATOR ESTEI', 'TRATOR RODAS'
    ]
    matriz_motos = matriz_motos.drop(columns=colunas_para_excluir, errors='ignore')

    return matriz_motos

def calcular_matriz_probabilidade_pesados(frota_categoria_processada: pd.DataFrame) -> pd.DataFrame:
    df = frota_categoria_processada.copy()
    # Subconjunto de colunas de interesse para pesados
    colunas_pesados = [
        'ANO', 'MES', 'UF', 'MUNICIPIO', 'CODIGO IBGE',
        'CAMINHAO', 'CAMINHAO TRATOR', 'CHASSI PLATAF',
        'TRATOR ESTEI', 'TRATOR RODAS',
        'MICRO-ONIBUS', 'ONIBUS'
    ]

    matriz_probabilidade_pesados = df[colunas_pesados].copy()

    # Total de pesados
    matriz_probabilidade_pesados['TOTAL_PESADOS'] = matriz_probabilidade_pesados[[ 
        'CAMINHAO', 'CAMINHAO TRATOR', 'CHASSI PLATAF',
        'TRATOR ESTEI', 'TRATOR RODAS',
        'MICRO-ONIBUS', 'ONIBUS'
    ]].sum(axis=1)

    # Total de caminhões
    matriz_probabilidade_pesados['TOTAL_CAMINHOES'] = matriz_probabilidade_pesados[[ 
        'CAMINHAO', 'CAMINHAO TRATOR', 'CHASSI PLATAF',
        'TRATOR ESTEI', 'TRATOR RODAS'
    ]].sum(axis=1)

    # Total de ônibus
    matriz_probabilidade_pesados['TOTAL_ONIBUS'] = matriz_probabilidade_pesados[['MICRO-ONIBUS', 'ONIBUS']].sum(axis=1)

    # Proporção de micro-ônibus sobre total de pesados
    matriz_probabilidade_pesados['frac_micro_onibus'] = matriz_probabilidade_pesados['MICRO-ONIBUS'] / matriz_probabilidade_pesados['TOTAL_PESADOS']

    # Porcentagens segundo ANFAVEA
    porcentagem_semileves = 2390 / 83044
    porcentagem_leves = 17867 / 83044
    porcentagem_medios = 6559 / 83044
    porcentagem_semipesados = 23758 / 83044
    porcentagem_pesados = 32470 / 83044
    porcentagem_onibus_urbanos = 16081 / 20643
    porcentagem_onibus_rodoviarios = 4562 / 20643

    # Probabilidades
    with pd.option_context('mode.chained_assignment', None):
        # Caminhões
        matriz_probabilidade_pesados['PROB_SEMILEVES'] = (matriz_probabilidade_pesados['TOTAL_CAMINHOES'] * porcentagem_semileves) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_LEVES'] = (matriz_probabilidade_pesados['TOTAL_CAMINHOES'] * porcentagem_leves) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_MEDIOS'] = (matriz_probabilidade_pesados['TOTAL_CAMINHOES'] * porcentagem_medios) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_SEMIPESADOS'] = (matriz_probabilidade_pesados['TOTAL_CAMINHOES'] * porcentagem_semipesados) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_PESADOS'] = (matriz_probabilidade_pesados['TOTAL_CAMINHOES'] * porcentagem_pesados) / matriz_probabilidade_pesados['TOTAL_PESADOS']

        # Probabilidades de ônibus por categoria até 2005 (não há microonibus, então apenas onibus urbanos e rodoviarios são considerados na prob)
        matriz_probabilidade_pesados['PROB_URBANOS_ATE_2005'] = (matriz_probabilidade_pesados['TOTAL_ONIBUS'] * porcentagem_onibus_urbanos) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_RODOVIARIOS_ATE_2005'] = (matriz_probabilidade_pesados['TOTAL_ONIBUS'] * porcentagem_onibus_rodoviarios) / matriz_probabilidade_pesados['TOTAL_PESADOS']

        # Probabilidades de ônibus por categoria a partir de 2006
        matriz_probabilidade_pesados['PROB_URBANOS'] = (matriz_probabilidade_pesados['ONIBUS'] * porcentagem_onibus_urbanos) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_RODOVIARIOS'] = (matriz_probabilidade_pesados['ONIBUS'] * porcentagem_onibus_rodoviarios) / matriz_probabilidade_pesados['TOTAL_PESADOS']
        matriz_probabilidade_pesados['PROB_MICROONIBUS'] = (matriz_probabilidade_pesados['MICRO-ONIBUS'] / matriz_probabilidade_pesados['TOTAL_PESADOS'])
        
    return matriz_probabilidade_pesados

#%% Processando Frota Ano

def processamento_arquivos_frota_ano(caminho_diretorio, caminho_arquivos_frota_ano, estados_brasileiros, meses_para_numeros):
    
    """
    Importa e processa os arquivos de frota ano (ano modelo dos veículos)

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta contendo os arquivos de entrada.
        caminho_arquivos_frota_ano (str): Pasta conendo os arquivos de frota ano
        estados_brasileiros (dict): Relacionado os estados com as respectivas siglas
        meses_para_numeros (dict): Relacionando os nomes dos meses com seu respectivo número
    
    Retorna:
        pd.DataFrame: DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF' e 'ANO MODELO'
        list: lista contendo os anos dos dados que estão sendo processados
    """ 

    caminho_arquivo = Path(caminho_diretorio) / caminho_arquivos_frota_ano
    arquivos = [f for f in os.listdir(caminho_arquivo) if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('~$')]
    
    anos_dados = set() 
    dfs = [] 
    
    for arquivo in arquivos:
        try:
            partes_nome = arquivo.split('_')
            ano = int(partes_nome[-1].split('.')[0])
            mes_nome = partes_nome[-2].lower()
            mes_numero = meses_para_numeros.get(mes_nome)
            
            if mes_numero is None:
                raise ValueError(f"Mês '{mes_nome}' não encontrado no dicionário de meses.")
            
            file_path = caminho_arquivo / arquivo
            frota_ano = pl.read_excel(file_path)
            
            print(f"Lendo dados de frota ano modelo para {mes_nome.capitalize()} de {ano}")
            
            # Excluir coluna 'Ano Fabricação CRV'
            if "Ano Fabricação CRV" in frota_ano.columns:
                frota_ano = frota_ano.drop("Ano Fabricação CRV")
            
            # Converter 'Ano Modelo' para numérico
            frota_ano = frota_ano.with_columns(
                pl.col("Ano Modelo").cast(pl.Int64, strict=False))
            
            # Adicionar coluna do ano e do mês
            frota_ano = frota_ano.with_columns(
                pl.lit(ano).alias("Ano"),
                pl.lit(mes_numero).alias("MES"))
            
            # Adicionar o DataFrame ajustado à lista de DataFrames
            dfs.append(frota_ano)
            anos_dados.add(ano)
            
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
    
    # Concatenar todos os DataFrames da lista em um único DataFrame
    frota_ano_final = pl.concat(dfs)
    
    # Reordenar colunas
    colunas_ordenadas = ["Ano", "MES", "UF", "Município", "Ano Modelo", "Qtd. Veículos"]
    frota_ano_final = frota_ano_final.select(colunas_ordenadas)
    
    # Processamento dos nomes das cidades
    frota_ano_final = frota_ano_final.with_columns(
        pl.col("Município")
        .str.to_uppercase()
        .map_elements(lambda x: unidecode(x) if x is not None else x, return_dtype=pl.Utf8)
        .str.replace_all(r'[^A-Za-z]', ''))
    
    # Substituindo nome dos UFs pela sigla
    frota_ano_final = frota_ano_final.with_columns(
        pl.col("UF").str.to_uppercase().replace(estados_brasileiros))
    
    # Remover linhas com valores NaN na coluna 'Ano Modelo'
    frota_ano_final = frota_ano_final.drop_nulls("Ano Modelo")
    
    # Remover linhas com valores vazios ou "Sem Informação" em 'Qtd. Veículos'
    frota_ano_final = frota_ano_final.filter(pl.col("Qtd. Veículos").is_not_null())
    
    # Converter nomes das colunas para caixa alta e remover acentos
    frota_ano_final = frota_ano_final.rename({col: unidecode(col).upper() for col in frota_ano_final.columns})
    frota_ano_final = frota_ano_final.to_pandas()
    frota_ano_final = frota_ano_final[frota_ano_final['MUNICIPIO'] != 'SEMINFORMAASSAPSO'] 

    return frota_ano_final, list(anos_dados)

def curva_sucateamento(anos_dados, frota_ano_processada):
    
    """
    Calcula a taxa de sobrevivência dos veículos de acordo com o ano do modelo, para todas as categorias
    
    Parâmetros:
        anos_dados (list): lista contendo os anos dos dados que estão sendo processados
        frota_ano_processada (DataFrame): DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF' e 'ANO MODELO'
    
    Retorna:
        pd.DataFrame: DataFrame contendo 'ANO', 'ANO MODELO', 'IDADE' e taxas de sobrevivência para todas as categorias
    """
    
    frota_ano_processada = pl.from_pandas(frota_ano_processada)
    max_ano = max(anos_dados) + 1
    
    frota_ano_processada = frota_ano_processada.filter(
        (pl.col("ANO").is_in(anos_dados)) &
        (pl.col("ANO MODELO") >= 1900) & 
        (pl.col("ANO MODELO") <= max_ano)
    )
    
    
    
    # Filtrar apenas os anos dentro do intervalo relevante
    
    
    # Calcular a idade dos veículos
    frota_ano_processada = frota_ano_processada.with_columns(
        (pl.lit(max_ano) - pl.col("ANO MODELO")).alias("IDADE"))

    print("Processando curva de sucateamento completa")

    # Obter vetor de idades
    t = frota_ano_processada["IDADE"].to_numpy()
    
    # Calcular todas as taxas de sobrevivência vetorizadas
    # 1. Veículos leves (gasolina/etanol)
    taxa_leves = np.exp(-np.exp(1.798 - 0.137 * t))
    
    # 2. Comerciais leves (gasolina/etanol)
    taxa_comleves = np.exp(-np.exp(1.618 - 0.141 * t))
    
    # 3. Motocicletas (duas curvas diferentes)
    taxa_motos_menos5 = np.exp(-np.exp(1.317 - 0.175 * t))  # Para t < 5
    taxa_motos_mais5 = np.exp(-np.exp(0.923 - 0.093 * t))   # Para t >= 5
    taxa_motos = np.where(t < 5, taxa_motos_menos5, taxa_motos_mais5)
    
    # 4. Comerciais leves Diesel
    taxa_comleves_diesel = 1 - (1/(1 + np.exp(0.17*(t-15.3))) + (1/(1 + np.exp(0.17*(t+15.3)))))
    
    # 5. Caminhões Diesel
    taxa_caminhoes_diesel = 1 - ((1/(1 + np.exp(0.10*(t-17)))) + (1/(1 + np.exp(0.10*(t+17)))))
    
    # 6. Ônibus Diesel
    taxa_onibus_diesel = 1 - ((1/(1 + np.exp(0.16*(t-19.1)))) + (1/(1 + np.exp(0.16*(t+19.1)))))

    # Criar DataFrame final com todas as taxas
    ValoresSUC = pl.DataFrame({
        "ANO": frota_ano_processada["ANO"],
        "ANO MODELO": frota_ano_processada["ANO MODELO"],
        "IDADE": frota_ano_processada["IDADE"],
        "TAXA SOBREVIVENCIA LEVES": 1 - taxa_leves,
        "TAXA SOBREVIVENCIA COMLEVES": 1 - taxa_comleves,
        "TAXA SOBREVIVENCIA COMLEVES DIESEL": 1 - taxa_comleves_diesel,
        "TAXA SOBREVIVENCIA MOTOS": 1 - taxa_motos,
        "TAXA SOBREVIVENCIA CAMINHOES DIESEL": 1 - taxa_caminhoes_diesel,
        "TAXA SOBREVIVENCIA ONIBUS DIESEL": 1 - taxa_onibus_diesel})

    # Definir taxa de sobrevivência zero para veículos com mais de 40 anos
    for col in ValoresSUC.columns[3:]:  # Apenas colunas de taxas
        ValoresSUC = ValoresSUC.with_columns(
            pl.when(pl.col("IDADE") > 40).then(0).otherwise(pl.col(col)).alias(col))
    
    cols_diesel = [
        "TAXA SOBREVIVENCIA COMLEVES DIESEL",
        "TAXA SOBREVIVENCIA CAMINHOES DIESEL", 
        "TAXA SOBREVIVENCIA ONIBUS DIESEL"]
    
    for col in cols_diesel:
        ValoresSUC = ValoresSUC.with_columns(
            pl.when(pl.col(col) > 1).then(1).otherwise(pl.col(col)).alias(col))
    
    # Converter para Pandas e remover duplicatas
    ValoresSUC = ValoresSUC.to_pandas()
    ValoresSUC = ValoresSUC.drop_duplicates()
    
    return ValoresSUC

def probabilidade_ano_modelo(frota_ano_processada, valores_suc):
    """
    Calcula a probabilidade do veículo ser de determinado ano modelo para todas as categorias,
    considerando a curva de sucateamento e limitando a vida máxima dos veículos em 40 anos.

    Parâmetros:
        frota_ano_processada (DataFrame): DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF' e 'ANO MODELO'
        valores_suc (DataFrame): DataFrame contendo taxas de sobrevivência para todas as categorias
    
    Retorna:
        pd.DataFrame: DataFrame com probabilidades de ano modelo para todas as categorias (excluindo linhas onde todas as probabilidades são zero)
    """
  
    # Consolidação do ANO MODELO
    frota_consolidada = frota_ano_processada.groupby(
        ['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'ANO MODELO'], 
        as_index=False)['QTD. VEICULOS'].sum()

    # Criando cópia para processamento
    frota_processada = frota_consolidada.copy()

    # Aplicando a taxa de sobrevivência
    print("Multiplicando a taxa de sobrevivência dos veículos para obter os veículos sobreviventes")
    
    # Mesclando os valores de sucateamento
    frota_processada = frota_processada.merge(
        valores_suc, 
        how='left', 
        left_on=['ANO', 'ANO MODELO'], 
        right_on=['ANO', 'ANO MODELO'])
    
    # Lista de todas as categorias a serem processadas
    categorias = [
        'LEVES',
        'COMLEVES',
        'COMLEVES DIESEL',
        'MOTOS',
        'CAMINHOES DIESEL',
        'ONIBUS DIESEL']
    
    # 1. Calcular veículos sobreviventes para cada categoria
    for categoria in categorias:
        col_taxa = f'TAXA SOBREVIVENCIA {categoria}'
        col_sobreviventes = f'{categoria} SOBREVIVENTES'
        frota_processada[col_sobreviventes] = frota_processada['QTD. VEICULOS'] * frota_processada[col_taxa]
    
    # 2. Consolidar totais de veículos sobreviventes por município
    grupos = ['ANO', 'MES', 'UF', 'MUNICIPIO']
    totais = {}
    
    for categoria in categorias:
        col_sobreviventes = f'{categoria} SOBREVIVENTES'
        total = frota_processada.groupby(grupos, as_index=False)[col_sobreviventes].sum()
        total.rename(columns={col_sobreviventes: f'{col_sobreviventes} TOTAL'}, inplace=True)
        totais[categoria] = total
    
    # 3. Mesclar totais com a frota processada
    for categoria, total_df in totais.items():
        frota_processada = frota_processada.merge(
            total_df, 
            on=grupos, 
            how='left')
    
    # 4. Calcular probabilidades para cada categoria
    for categoria in categorias:
        col_sobreviventes = f'{categoria} SOBREVIVENTES'
        col_total = f'{col_sobreviventes} TOTAL'
        col_prob = f'PROBABILIDADE {categoria}'
        
        frota_processada[col_prob] = frota_processada[col_sobreviventes] / frota_processada[col_total]
    
    # Substituir NaN por 0 onde o total foi 0
    for categoria in categorias:
        col_prob = f'PROBABILIDADE {categoria}'
        frota_processada[col_prob] = frota_processada[col_prob].fillna(0)

    # Filtrar linhas onde todas as probabilidades são zero
    colunas_prob = [f'PROBABILIDADE {categoria}' for categoria in categorias]
    frota_processada = frota_processada[~frota_processada[colunas_prob].eq(0).all(axis=1)]

    return frota_processada

#%% Processando Frota Combustivel

def processamento_arquivos_frota_combustivel(caminho_diretorio, caminho_arquivos_frota_combustivel, meses_para_numeros, mapa_combustivel):
    

    
    """
    Importa, faz o mapeamento dos combustíveis de acordo com mapa_combustivel, e o processamento das planilhas de frota por combustível, contidas na pasta de entrada

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta contendo os arquivos de entrada.
        caminho_arquivos_frota_combustivel (str): Pasta conendo os arquivos de frota ano
        estados_brasileiros (dict): Relacionado os estados com as respectivas siglas
        meses_para_numeros (dict): Relacionando os nomes dos meses com seu respectivo número
    
    Retorna:
        pd.DataFrame: DataFrame processado contendo 'ANO', 'MUNICIPIO', 'UF', ' COMBUSTIVEL' e 'QTD. VEICULOS' por combustível
    """ 
    
    caminho_arquivo = os.path.join(caminho_diretorio, caminho_arquivos_frota_combustivel)
    arquivos = [f for f in os.listdir(caminho_arquivo) if f.endswith('.xlsx') and not f.startswith('~$')]
    frota_combustivel_consolidado = pd.DataFrame()

    for arquivo in arquivos:
        try:
            partes_nome = arquivo.split('_')
            ano = int(partes_nome[-1].split('.')[0])
            mes_nome = partes_nome[-2].lower()  

            # Obter o número do mês usando o dicionário
            mes_numero = meses_para_numeros.get(mes_nome, None)

            if mes_numero is None:
                raise ValueError(f"Mês '{mes_nome}' não encontrado no dicionário de meses.")

            print(f"Lendo dados da frota combustivel para {mes_nome.capitalize()} de {ano}")
            
            caminho_completo = os.path.join(caminho_arquivo, arquivo)
            frota_combustivel = pl.read_excel(caminho_completo)
            
            # Converter para Pandas para o processamento existente
            frota_combustivel = frota_combustivel.to_pandas()

            # Definindo nomes das colunas 
            frota_combustivel.columns = ["UF", "Município", "Combustível Veículo", "Qtd. Veículos"]

            # Aplicar mapeamento de combustíveis
            frota_combustivel['Categoria Combustível'] = frota_combustivel['Combustível Veículo'].map(mapa_combustivel)

            # Criar um DataFrame auxiliar para armazenar a correspondência entre 'Município' e 'UF'
            uf_municipios = frota_combustivel[['Município', 'UF']].drop_duplicates()

            # Agrupar por município e categoria de combustível
            frota_combustivel_mapeado = (frota_combustivel.groupby(['UF', 'Município', 'Categoria Combustível'])['Qtd. Veículos'].sum().reset_index())

            # Renomear coluna para "Combustível"
            frota_combustivel_mapeado.rename(columns={"Categoria Combustível": "Combustível"}, inplace=True)


            # Adicionar as colunas "Ano" e "MES"
            frota_combustivel_mapeado["Ano"] = ano
            frota_combustivel_mapeado["MES"] = mes_numero

            # Garantir a ordem correta das colunas
            colunas_ordenadas = ["Ano", "MES", "UF", "Município", "Combustível", "Qtd. Veículos"]
            frota_combustivel_mapeado = frota_combustivel_mapeado[colunas_ordenadas]

            # Correções nos dados
            frota_combustivel_mapeado = frota_combustivel_mapeado.dropna(subset=["Qtd. Veículos"])

            # Processamento dos nomes das cidades
            frota_combustivel_mapeado["Município"] = frota_combustivel_mapeado["Município"].str.upper()
            frota_combustivel_mapeado["Município"] = frota_combustivel_mapeado["Município"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            frota_combustivel_mapeado["Município"] = frota_combustivel_mapeado["Município"].str.replace(r'[^A-Za-z]', '', regex=True)

            # Substituir nome dos UFs pela sigla
            frota_combustivel_mapeado["UF"] = frota_combustivel_mapeado["UF"].str.upper().replace(estados_brasileiros)

            # Converter nomes das colunas para caixa alta e remover acentos
            frota_combustivel_mapeado.columns = [unidecode(col).upper() for col in frota_combustivel_mapeado.columns]
            
            # Acumular dados processados
            frota_combustivel_consolidado = pd.concat([frota_combustivel_consolidado, frota_combustivel_mapeado], ignore_index=True)
            frota_combustivel_consolidado = frota_combustivel_consolidado[frota_combustivel_consolidado['MUNICIPIO'] != 'SEMINFORMAAAO']
            
        except FileNotFoundError:
            print(f"Arquivo não encontrado: {arquivo}")
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

    return frota_combustivel_consolidado

def consumos_flex_fuel(caminho_diretorio, frota_combustivel_processada):
   
    """
    Ajusta a proporção de veículos FlexFuel entre FlexFuel gasolina e FlexFuel etanol por munícipio, de acordo com a proporção contida no arquivo 
    "Porcentagem_combustivel_motor_flexfuel.xlsx"

    Parâmetros:
        frota_combustivel_processada (DataFrame): Contendo 'ANO', 'UF', 'MUNICIPIO', 'CODIGO IBGE', 'COMBUSTIVEL', 'QTD. VEICULOS'
        proporcao_flex (DataFrame): Contendo 'ANO', 'GASOLINA C', 'ETANOL HIDRATADO'

    Retorna:
        pd.DataFrame: frota_combustivel_processada desmembrando os veículos que utilizavam combustível flex fuel em 'Flex Gasolina' e 'Flex Etanol'
    """ 
    
    caminho_arquivo = os.path.join(caminho_diretorio, "Porcentagem_combustivel_motor_flexfuel.xlsx")
    proporcao_flex = pd.read_excel(caminho_arquivo)
    
    # Garantir que as proporções Flex tenham valores para todos os anos necessários
    max_ano_proporcao = proporcao_flex['ANO'].max()
    frota_combustivel_processada['ANO'] = frota_combustivel_processada['ANO'].astype(int)
    
    # Preencher anos ausentes com o último valor disponível
    proporcao_flex = proporcao_flex.set_index('ANO').reindex(range(int(proporcao_flex['ANO'].min()), int(frota_combustivel_processada['ANO'].max()) + 1),method='ffill').reset_index()

    # Somar veículos do mesmo tipo por MUNICIPIO e ano
    frota_agrupada = frota_combustivel_processada.groupby(['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'COMBUSTIVEL'], as_index=False)['QTD. VEICULOS'].sum()

    # Separar os veículos FlexFuel
    frota_flex = frota_agrupada[frota_agrupada['COMBUSTIVEL'] == 'Flex'].copy()

    # Fazer merge para obter as proporções correspondentes a cada ano
    frota_flex = frota_flex.merge(proporcao_flex, on='ANO', how='left')

    # Calcular novas colunas para Flex Gasolina e Flex Etanol
    frota_flex_gasolina = frota_flex.copy()
    frota_flex_gasolina['COMBUSTIVEL'] = 'Flex Gasolina'
    frota_flex_gasolina['QTD. VEICULOS'] = (frota_flex_gasolina['QTD. VEICULOS'] * frota_flex_gasolina['GASOLINA C']) / 100

    frota_flex_etanol = frota_flex.copy()
    frota_flex_etanol['COMBUSTIVEL'] = 'Flex Etanol'
    frota_flex_etanol['QTD. VEICULOS'] = (frota_flex_etanol['QTD. VEICULOS'] * frota_flex_etanol['ETANOL HIDRATADO']) / 100

    # Remover a coluna original 'Flex'
    frota_agrupada = frota_agrupada[frota_agrupada['COMBUSTIVEL'] != 'Flex']

    # Concatenar os novos dados de Flex Gasolina e Flex Etanol
    frota_flexfuel = pd.concat([frota_agrupada,
        frota_flex_gasolina[['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'COMBUSTIVEL', 'QTD. VEICULOS']],
        frota_flex_etanol[['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'COMBUSTIVEL', 'QTD. VEICULOS']]])

    # Ordenar os dados
    frota_flexfuel = frota_flexfuel.sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'COMBUSTIVEL']).reset_index(drop=True)

    return frota_flexfuel

def probabilidade_comb_leves(frota_combustivel_processada_flexfuel):
    
    """
    Calcula as probabilidades dos tipos de combustível para veículos leves em diferentes períodos de tempo, os tipos de combustível em cada período estão especificados nos 
    comentários

    Parâmetros:
        frota_combustivel_processada_flexfuel (DataFrame): Contendo 'ANO', 'UF', 'MUNICIPIO', 'COMBUSTÍVEL', 'QTD. VEICULOS']

    Retorna:
        pd.DataFrames: frota_proporcao_leves_82, frota_proporcao_leves_2003, frota_proporcao_leves_2007, contendo as probabilidades do uso de cada combustível para diferentes períodos
    de tempo, conforme combustíveis disponíveis em cada período para veículos leves
    """ 
    
    # Mapeamento dos combustíveis para os códigos equivalentes
    combustivel_map = {"Etanol": 1,"Gasolina C": 5,"Flex Etanol": 3, "Flex Gasolina": 4}
    frota_combustivel_processada_flexfuel["CODIGO COMBUSTIVEL"] = frota_combustivel_processada_flexfuel["COMBUSTIVEL"].map(combustivel_map)
    
    # Filtrar dados para cada período
    def processar_periodo(df, codigos):
        df_filtrado = df[df["CODIGO COMBUSTIVEL"].isin(codigos)].copy()
        df_agrupado = df_filtrado.groupby(["ANO", "MES", "UF", "CODIGO IBGE", "MUNICIPIO", "CODIGO COMBUSTIVEL"], as_index=False).sum()
        
        # Calcular soma total por MUNICIPIO
        soma_total = df_agrupado.groupby(["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"])['QTD. VEICULOS'].sum().reset_index()
        df_final = df_agrupado.merge(soma_total, on=["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"], suffixes=("", " TOTAL"))
        df_final["PROPORCAO"] = df_final["QTD. VEICULOS"] / df_final["QTD. VEICULOS TOTAL"]
        
        df_final = df_final[["ANO", "MES", "UF", "CODIGO IBGE", "CODIGO COMBUSTIVEL", "QTD. VEICULOS", "QTD. VEICULOS TOTAL", "PROPORCAO"]]
        return df_final
    
    # Período 1982-2002 (apenas Gasolina e Etanol)
    frota_proporcao_leves_82 = processar_periodo(frota_combustivel_processada_flexfuel, [1, 5])
    
    # Período 2003-2006 (Gasolina, Etanol, Flex Gasolina, Flex Etanol)
    frota_proporcao_leves_2003 = processar_periodo(frota_combustivel_processada_flexfuel, [1, 5, 3, 4])
    
    # Período 2007-2018 (Gasolina, Flex Gasolina, Flex Etanol)
    frota_proporcao_leves_2007 = processar_periodo(frota_combustivel_processada_flexfuel, [5, 3, 4])
    
    return frota_proporcao_leves_82, frota_proporcao_leves_2003, frota_proporcao_leves_2007

def probabilidade_comb_comleves(frota_combustivel_processada_flexfuel):
    
    """
    Calcula as probabilidades dos tipos de combustível para veículos comerciais leves em diferentes períodos de tempo, os tipos de combustível em cada período estão especificados 
    nos comentários

    Parâmetros:
        frota_combustivel_processada_flexfuel (DataFrame): Contendo 'ANO', 'UF', 'MUNICIPIO', 'COMBUSTÍVEL', 'QTD. VEICULOS']

    Retorna:
        pd.DataFrames: frota_combustivel_processada_flexfuel_ComLEVES83, frota_combustivel_processada_flexfuel_ComLEVES2003, frota_combustivel_processada_flexfuel_ComLEVES2006, 
    frota_combustivel_processada_flexfuel_ComLEVES2007, contendo as probabilidades do uso de cada combustível para diferentes períodos de tempo, conforme combustíveis disponíveis
    em cada período para veículos comerciais leves
    """ 
    
    # Mapeamento dos combustíveis para códigos equivalentes
    combustivel_map = {"Etanol": 1,"Gasolina C": 5,"Flex Etanol": 3,"Flex Gasolina": 4,"Diesel": 2}
    frota_combustivel_processada_flexfuel["CODIGO COMBUSTIVEL"] = frota_combustivel_processada_flexfuel["COMBUSTIVEL"].map(combustivel_map)
    
    # Função auxiliar para calcular a proporção dos combustíveis por MUNICIPIO
    def processar_periodo(df, codigos):
        df_filtrado = df[df["CODIGO COMBUSTIVEL"].isin(codigos)].copy()
        df_agrupado = df_filtrado.groupby(["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES", "CODIGO COMBUSTIVEL"], as_index=False).sum()
        
        # Calcular soma total por MUNICIPIO
        soma_total = df_agrupado.groupby(["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"])['QTD. VEICULOS'].sum().reset_index()
        df_final = df_agrupado.merge(soma_total, on=["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"], suffixes=("", " TOTAL"))
        df_final["PROPORCAO"] = df_final["QTD. VEICULOS"] / df_final["QTD. VEICULOS TOTAL"]
        return df_final
    
    # Período 1983-2002 (apenas Gasolina e Etanol)
    frota_proporcao_ComLEVES83 = processar_periodo(frota_combustivel_processada_flexfuel, [1, 5])
    
    # Período 2003-2005 (Gasolina, Etanol, Flex Gasolina, Flex Etanol)
    frota_proporcao_ComLEVES2003 = processar_periodo(frota_combustivel_processada_flexfuel, [1, 5, 3, 4])
    
    # Período 2006 (Gasolina, Etanol, Flex Gasolina, Flex Etanol, Diesel)
    frota_proporcao_ComLEVES2006 = processar_periodo(frota_combustivel_processada_flexfuel, [1, 5, 3, 4, 2])
    
    # Período 2007-2018 (Gasolina, Flex Gasolina, Flex Etanol, Diesel)
    frota_proporcao_ComLEVES2007 = processar_periodo(frota_combustivel_processada_flexfuel, [5, 3, 4, 2])
    
    return (frota_proporcao_ComLEVES83, frota_proporcao_ComLEVES2003, frota_proporcao_ComLEVES2006, frota_proporcao_ComLEVES2007)

def probabilidade_comb_motos(frota_combustivel_processada_flexfuel):
    """
    Calcula as proporções de combustível para motos com base apenas na QTD. VEICULOS real, sem interpolação.

    Parâmetros:
        frota_combustivel_processada_flexfuel (DataFrame): Deve conter 'ANO', 'MES', 'UF', 'MUNICIPIO',
        'COMBUSTIVEL', 'QTD. VEICULOS', 'CODIGO IBGE'

    Retorna:
        Dois DataFrames:
            - frota_proporcao_motos_2003 (Gasolina)
            - frota_proporcao_motos_2010 (Gasolina, Flex Etanol, Flex Gasolina)
    """

    # Mapeamento dos combustíveis para os códigos equivalentes
    combustivel_map = {"Etanol": 1,"Gasolina C": 5,"Flex Etanol": 3, "Flex Gasolina": 4}
    frota_combustivel_processada_flexfuel["CODIGO COMBUSTIVEL"] = frota_combustivel_processada_flexfuel["COMBUSTIVEL"].map(combustivel_map)
    
    # Filtrar dados para cada período
    def processar_periodo(df, codigos):
        df_filtrado = df[df["CODIGO COMBUSTIVEL"].isin(codigos)].copy()
        df_agrupado = df_filtrado.groupby(["ANO", "MES", "UF", "CODIGO IBGE", "MUNICIPIO", "CODIGO COMBUSTIVEL"], as_index=False).sum()
        
        # Calcular soma total por MUNICIPIO
        soma_total = df_agrupado.groupby(["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"])['QTD. VEICULOS'].sum().reset_index()
        df_final = df_agrupado.merge(soma_total, on=["UF", "MUNICIPIO", "CODIGO IBGE", "ANO", "MES"], suffixes=("", " TOTAL"))
        df_final["PROPORCAO"] = df_final["QTD. VEICULOS"] / df_final["QTD. VEICULOS TOTAL"]
        
        df_final = df_final[["ANO", "MES", "UF", "CODIGO IBGE", "CODIGO COMBUSTIVEL", "QTD. VEICULOS", "QTD. VEICULOS TOTAL", "PROPORCAO"]]
        return df_final

    # Período 2003-2009 — apenas gasolina
    frota_proporcao_motos_2003 = processar_periodo(frota_combustivel_processada_flexfuel, [5])

    # Período 2010-2017 — gasolina, flex etanol, flex gasolina
    frota_proporcao_motos_2010 = processar_periodo(frota_combustivel_processada_flexfuel, [3, 4, 5])

    return frota_proporcao_motos_2003, frota_proporcao_motos_2010

#%% Processando Consumo Combustivel

def processamento_arquivos_consumo_comb(caminho_diretorio, caminho_arquivos_consumo_comb, meses_para_numeros):
   
    """ 
    Função para importar e processar os dados de consumo de combustível mensal
    
    Parâmetros:
        caminho_arquivos_consumo_comb: Caminho da pasta com os arquivos de consumo de combustível
        meses_para_numeros: Dicionário de conversão de nome do mês para número
    
    Retorna:
        dfs_combustiveis: Dicionário com DataFrames separados por tipo de combustível, acessados a partir da inicial do combustível
    """
    
    caminho_arquivo = Path(os.path.join(caminho_diretorio, caminho_arquivos_consumo_comb)).as_posix()
    arquivos = [f for f in os.listdir(caminho_arquivo) if f.endswith('.xlsx') and not f.startswith('~$')]
    dfs_combustiveis = {}
    
    for arquivo in arquivos:
        partes = arquivo.split('_')
        inicial_produto = partes[2] 
        ano = partes[3] 
        
        mes_nome = arquivo.split('_')[-1].replace('.xlsx', '').lower()
        mes_numero = meses_para_numeros.get(mes_nome, 0) 
        
        print(f"Lendo dados de consumo de combustivel {inicial_produto} para {mes_nome.capitalize()} de {ano}")
        
        # Ler o arquivo Excel (sem cabeçalho)
        colunas = ['UF', 'MUNICIPIO', 'CONSUMO']
        caminho_arquivo_completo = Path(os.path.join(caminho_arquivo, arquivo)).as_posix()
        df = pd.read_excel(caminho_arquivo_completo, header=None, names=colunas)
        
        # Adicionar colunas extras
        df['ANO'] = int(ano)
        df['MES'] = mes_numero
        
        # Padronizar nomes das cidades
        df['MUNICIPIO'] = df['MUNICIPIO'].str.upper()  # Converter para maiúsculas
        df['MUNICIPIO'] = df['MUNICIPIO'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')  # Remover acentos
        df['MUNICIPIO'] = df['MUNICIPIO'].str.replace(r'[^a-zA-Z]', '', regex=True)  # Remover caracteres não alfabéticos
        
        # Selecionar e ordenar colunas relevantes
        df = df[['ANO', 'MES', 'UF', 'MUNICIPIO', 'CONSUMO']]
        
        # Converter nomes das colunas para caixa alta e remover acentos
        df.columns = [unidecode(col).upper() for col in df.columns]
        
        # Adicionar DataFrame ao dicionário (usando a inicial como chave)
        if inicial_produto in dfs_combustiveis:
            dfs_combustiveis[inicial_produto] = pd.concat([dfs_combustiveis[inicial_produto], df], ignore_index=True)
        else:
            dfs_combustiveis[inicial_produto] = df
        
    return dfs_combustiveis

def combustivel_transportes_ben(caminho_diretorio, consumo_oleo, consumo_gasolina, consumo_etanol):
    
    """
    Função para determinar a quantidade de combustível vendido pela ANP que é efetivamente consumida pelo setor de transportes, de acordo com a proporção definida no arquivo 
    "ConsumoCombustiveTransporte_BEN.xlsx" do Balanço Energético Nacional

    Parâmetros:
        caminho_diretorio (str): Caminho da pasta onde está o arquivo do BEN
        consumo_oleo (DataFrame): Contendo o consumo total de óleo por ano, cidade e produto
        consumo_gasolina (DataFrame): Contendo o consumo total de gasolina por ano, cidade e produto
        consumo_etanol (DataFrame): Contendo o consumo total de etanol por ano, cidade e produto
    
    Retorna:
        pd.DataFrames: consumo_oleo, consumo_gasolina, consumo_etanol: Contendo o consumo do respectivo combustível mantendo apenas a fração destinada ao transporte
    """

    filename_ben = f"{caminho_diretorio}/5.FracaoDoVolumeParaTransportes/ConsumoCombustiveTransporte_BEN.xlsx"
    df_ben_pl = pl.read_excel(filename_ben)
    df_ben = df_ben_pl.to_pandas()

    # Garantir que a coluna 'CONSUMO' esteja no formato float para evitar problemas de dtype
    consumo_oleo['CONSUMO'] = consumo_oleo['CONSUMO'].astype(float)
    consumo_gasolina['CONSUMO'] = consumo_gasolina['CONSUMO'].astype(float)
    consumo_etanol['CONSUMO'] = consumo_etanol['CONSUMO'].astype(float)

    # Ajuste do consumo de óleo, gasolina e etanol por ano
    for _, row in df_ben.iterrows():
        ano = row['Ano']
        perc_oleo = row['Porcentagem Diesel']
        perc_gasolina = row['Porcentagem Gasolina']
        perc_etanol = row['Porcentagem Etanol']

        # Ajuste para óleo
        mask_oleo = consumo_oleo['ANO'] == ano
        consumo_oleo.loc[mask_oleo, 'CONSUMO'] *= perc_oleo

        # Ajuste para gasolina
        mask_gasolina = consumo_gasolina['ANO'] == ano
        consumo_gasolina.loc[mask_gasolina, 'CONSUMO'] *= perc_gasolina

        # Ajuste para etanol
        mask_etanol = consumo_etanol['ANO'] == ano
        consumo_etanol.loc[mask_etanol, 'CONSUMO'] *= perc_etanol

    return consumo_oleo, consumo_gasolina, consumo_etanol

def segregacao_consumos_comb(caminho_diretorio, consumo_gasolina, consumo_etanol, consumo_diesel):
    
    """
    Função para segregar o consumo total de combustíveis (gasolina, etanol e diesel) com base em proporções definidas
    no arquivo "ConsumoCategoria_2012.xlsx" (adaptado do inventário nacional).

    Parâmetros:
        caminho_diretorio: Caminho da pasta onde está o arquivo de proporções
        consumo_gasolina: DataFrame com o consumo de gasolina
        consumo_etanol: DataFrame com o consumo de etanol
        consumo_diesel: DataFrame com o consumo de diesel

    Retorna:
        DataFrames: consumo_gasolina, consumo_etanol, consumo_diesel (já segregados por categoria)
    """
    
    # Lê a planilha com o índice na primeira coluna (nomes dos combustíveis)
    filename = f"{caminho_diretorio}/ConsumoCategoria_2012.xlsx"
    df = pd.read_excel(filename, index_col=0)

    # Ajusta nomes das colunas e do índice
    df.columns = [unidecode(col).upper().strip() for col in df.columns]
    df.index = [unidecode(str(idx)).upper().strip() for idx in df.index]

    # Converte apenas os valores para float
    df = df.astype(float)

    # Define mapeamento dos DataFrames de consumo
    mapa_combustivel = {
        'GASOLINA': consumo_gasolina,
        'ETANOL': consumo_etanol,
        'DIESEL': consumo_diesel
    }

    # Aplica as proporções aos respectivos DataFrames
    for combustivel, df_combustivel in mapa_combustivel.items():
        if combustivel in df.index:
            for categoria in df.columns:
                proporcao = df.loc[combustivel, categoria]
                df_combustivel[categoria] = df_combustivel['CONSUMO'] * proporcao

    # Renomeia as colunas de proporções
    renomear = {
        'LEVES': 'PROPORCAO LEVES',
        'COMERCIAIS LEVES': 'PROPORCAO COMERCIAIS LEVES',
        'MOTOS': 'PROPORCAO MOTOS',
        'PESADOS': 'PROPORCAO PESADOS'
    }

    # Remove colunas desnecessárias e aplica renomeações
    for df_comb in [consumo_gasolina, consumo_etanol, consumo_diesel]:
        df_comb.drop(columns=[col for col in ['MUNICIPIO', 'PRODUTO'] if col in df_comb.columns], inplace=True, errors='ignore')
        df_comb.rename(columns=renomear, inplace=True)
        df_comb.columns = [unidecode(col).upper().strip() for col in df_comb.columns]

    return consumo_gasolina, consumo_etanol, consumo_diesel

#%% Separação em Diferentes Dtaframes dos municipios que vendem ou nao vendem etanol e atribuição do fator de emissao

def processar_matrizes_etanol(frota_categoria_processada, consumo_etanol, fator_emissao):
    
    """
    Processa os DataFrames de frota categoria, segregando em dois novos DataFrames, contendo os munícipios que vendem ou não etanol e adiciona os fatores de emissão evaporativas
    da CETESB de acordo com a categoria
    
    Parâmetros:
        frota_categoria_processada (pd.DataFrame): DataFrame com dados de frota por categoria
        consumo_etanol (pd.DataFrame): DataFrame com dados de consumo de etanol por município
        fator_emissao (pd.DataFrame): DataFrame com fatores de emissão por categoria de veículo
        
    Retorna:
        tuple: Tupla contendo os DataFrames dos munícpios com e sem etanol, já adicionado os fatores de emissão
    """
    
    # Verificar e ajustar ano de referência
    ano_frota = frota_categoria_processada['ANO'].iloc[0] 
    anos_disponiveis = consumo_etanol['ANO'].unique()
    
    ano_ajustado = ano_frota if ano_frota in anos_disponiveis else max(anos_disponiveis)
    
    # Filtrar consumo de etanol para o ano ajustado
    consumo_etanol_filtrado = consumo_etanol[consumo_etanol['ANO'] == ano_ajustado]
    
    # Identificar municípios que vendem etanol
    cols_chave = ['UF', 'CODIGO IBGE']
    vendemetanol = pd.merge(
        frota_categoria_processada[cols_chave],
        consumo_etanol_filtrado[cols_chave],
        on=cols_chave,
        how='inner').drop_duplicates()
    
    # Criar matrizes com e sem etanol
    matriz_com_Etanol = pd.merge(frota_categoria_processada, vendemetanol, on=cols_chave, how='inner')
    matriz_sem_Etanol = frota_categoria_processada[ ~frota_categoria_processada.set_index(cols_chave).index.isin(vendemetanol.set_index(cols_chave).index)]
    
    # Função otimizada para adicionar fatores de emissão
    def adicionar_fatores(df, fatores):
        # Criar cópias para evitar modificações diretas em subconjuntos
        df = df.copy()
        fatores = fatores.copy()

        # Criar chave temporária para merge
        df['key'] = 1
        fatores['key'] = 1

        # Realizar o merge e remover a chave temporária
        resultado = pd.merge(df, fatores, on='key').drop('key', axis=1) 
        return resultado
    
    # Adicionar fatores de emissão (versão otimizada)
    matriz_com_Etanol = adicionar_fatores(matriz_com_Etanol, fator_emissao)
    matriz_sem_Etanol = adicionar_fatores(matriz_sem_Etanol, fator_emissao)
    
    # Reorganizar colunas (versão mais eficiente)
    for matriz in [matriz_com_Etanol, matriz_sem_Etanol]:
        if 'ANO MODELO' in matriz.columns:
            col = matriz.pop('ANO MODELO')
            matriz.insert(4, 'ANO MODELO', col)
    
    return matriz_com_Etanol, matriz_sem_Etanol

def substituir_combustivel_fe_municipios_sem_etanol(matriz_sem_etanol, fator_emissao):
    
    """
    Substitui os tipos de combustível ETANOL e FLEX-ETANOL por GASOLINA e FLEX-GASOLINA no DataFrme de munícipios que não vendem etanol e atualiza os fatores de emissão 
    correspondentes, sendo que o fator de emissão difere de acordo com a categoria
    
    Parâmetros:
        matriz_sem_etanol (pd.DataFrame): DataFrame com dados de frota categoria e fatores de emissão dos munícipios sem etanol
        fator_emissao (pd.DataFrame): DataFrame com fatores de emissão por categoria de veículo
        
    Retorna:
        pd.DataFrame: contendo o combustível e fator de emissão atualizado para os munícipios que não vendem etanol
    """

    # Substituições conforme o período
    mask_1982_2002 = (matriz_sem_etanol['ANO MODELO'].between(1982, 2002)) & (matriz_sem_etanol['COMBUSTIVEL'] == 'ETANOL HIDRATADO')
    matriz_sem_etanol.loc[mask_1982_2002, 'COMBUSTIVEL'] = 'GASOLINA C'

    mask_2003_2006 = (matriz_sem_etanol['ANO MODELO'].between(2003, 2007)) 
    matriz_sem_etanol.loc[mask_2003_2006 & (matriz_sem_etanol['COMBUSTIVEL'] == 'ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'
    matriz_sem_etanol.loc[mask_2003_2006 & (matriz_sem_etanol['COMBUSTIVEL'] == 'FLEX-ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'

    mask_2007 = matriz_sem_etanol['ANO MODELO'] > 2007
    matriz_sem_etanol.loc[mask_2007 & (matriz_sem_etanol['COMBUSTIVEL'] == 'FLEX-ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'

    # Selecionando apenas as colunas relevantes para atualização
    colunas_fe = [
        'FE_DIURNAL_20A35', 'FE_HOTSOAK_20A35', 'FE_RUNNINGLOSSES_20A35',
        'FE_DIURNAL_10A25', 'FE_HOTSOAK_10A25', 'FE_RUNNINGLOSSES_10A25',
        'FE_DIURNAL_0A15', 'FE_HOTSOAK_0A15', 'FE_RUNNINGLOSSES_0A15']

    # Merge sem criar colunas duplicadas
    matriz_sem_etanol = matriz_sem_etanol.merge(
        fator_emissao[['ANO MODELO', 'COMBUSTIVEL'] + colunas_fe],
        on=['ANO MODELO', 'COMBUSTIVEL'],how='left')

    # Atualizando apenas os valores de fatores de emissão
    for col in colunas_fe:
        matriz_sem_etanol[col] = matriz_sem_etanol[col + '_y'].combine_first(matriz_sem_etanol[col + '_x'])

    # Removendo colunas desnecessárias criadas pelo merge
    matriz_sem_etanol.drop(columns=[col + '_x' for col in colunas_fe] + [col + '_y' for col in colunas_fe], inplace=True)

    return matriz_sem_etanol

def substituir_combustivel_fe_municipios_sem_etanol_exaustivas(matriz_sem_etanol, fator_emissao):
   
    """
    Substitui os tipos de combustível ETANOL e FLEX-ETANOL por GASOLINA e FLEX-GASOLINA no DataFrame de municípios
    que não vendem etanol e atualiza os fatores de emissão correspondentes.
    
    Parâmetros:
        matriz_sem_etanol (pd.DataFrame): DataFrame com dados de frota e emissões dos municípios sem etanol
        fator_emissao (pd.DataFrame): DataFrame com fatores de emissão por categoria de veículo
        
    Retorna:
        pd.DataFrame: Contendo o combustível e fatores de emissão atualizados para municípios sem etanol
    """
    
    # Criar uma cópia para evitar modificações no DataFrame original
    matriz_atualizada = matriz_sem_etanol.copy()
    
    # Substituições conforme o período do ano modelo
    mask_1982_2002 = (matriz_atualizada['ANO MODELO'].between(1982, 2002)) & (matriz_atualizada['COMBUSTIVEL'] == 'ETANOL HIDRATADO')
    matriz_atualizada.loc[mask_1982_2002, 'COMBUSTIVEL'] = 'GASOLINA C'

    mask_2003_2006 = (matriz_atualizada['ANO MODELO'].between(2003, 2007)) 
    matriz_atualizada.loc[mask_2003_2006 & (matriz_atualizada['COMBUSTIVEL'] == 'ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'
    matriz_atualizada.loc[mask_2003_2006 & (matriz_atualizada['COMBUSTIVEL'] == 'FLEX-ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'

    mask_2007 = matriz_atualizada['ANO MODELO'] > 2007
    matriz_atualizada.loc[mask_2007 & (matriz_atualizada['COMBUSTIVEL'] == 'FLEX-ETANOL HIDRATADO'), 'COMBUSTIVEL'] = 'FLEX-GASOLINA C'

    # Definir as colunas de poluentes baseadas no que existe no DataFrame
    poluentes_disponiveis = ['CO', 'HCTOT', 'CH4', 'NOX', 'MP', 'CO2', 'N2O']
    if 'RCHO' in matriz_atualizada.columns:
        poluentes_disponiveis.append('RCHO')
    
    # Filtrar apenas poluentes que existem em ambos DataFrames
    colunas_poluentes = [p for p in poluentes_disponiveis if p in fator_emissao.columns]
    
    # Criar um DataFrame temporário para o merge
    temp_fe = fator_emissao[['ANO MODELO', 'COMBUSTIVEL'] + colunas_poluentes].copy()
    
    # Realizar o merge para obter os novos fatores de emissão
    matriz_atualizada = matriz_atualizada.merge(
        temp_fe, 
        on=['ANO MODELO', 'COMBUSTIVEL'], 
        how='left', 
        suffixes=('', '_novo'))
    
    # Atualizar os valores dos poluentes com os novos fatores de emissão
    for poluente in colunas_poluentes:
        if f'{poluente}_novo' in matriz_atualizada.columns:
            matriz_atualizada[poluente] = matriz_atualizada[f'{poluente}_novo'].combine_first(matriz_atualizada[poluente])
            matriz_atualizada.drop(columns=[f'{poluente}_novo'], inplace=True)
    
    return matriz_atualizada

def padronizar_combustivel(matriz):
    
    """
    Padroniza os tipos de combustível nas matrizes, substituindo as strings pelos códigos correspondentes, tanto para os leves como para comleves
    
    Parâmetros:
    - matriz: DataFrames com e sem etanol para as diferentes categorias de veículos com a coluna 'CODIGO COMBUSTIVEL' contendo os nomes dos combustíveis.
    
    Retorna:
    - DataFrame com a coluna 'CODIGO COMBUSTIVEL' substituída pelos códigos correspondentes.
    """
    
    matriz = matriz.rename(columns={'COMBUSTIVEL': 'CODIGO COMBUSTIVEL'})

    # Aplica o mapeamento para substituir as strings pelos códigos correspondentes
    matriz['CODIGO COMBUSTIVEL'] = matriz['CODIGO COMBUSTIVEL'].map(codigos_combustivel_mai)
    return matriz

#%% Funções com o objetivo de adicionar as características dos veículos as matrizes com e sem etanol, sendo elas:
#Probabilidade de ano modelo - mesma função independente da categoria de veículo

# Probabilidade do uso de combustíveis - função difere por conta do uso de combustíveis por período para cada categoria de veículo ser diferente

# Consumo Combustível - mesma função independente da categoria de veículo

# Autonomia - mesma função independente da categoria de veículo

def adicionando_prob_ano_modelo(matriz, frota_processada_probAnoModelo, colunas_probabilidade):
    
    """
    Adiciona colunas de probabilidade de ano modelo à matriz contendo os fatores de emissão.
    Se houver veículos em anos anteriores ao primeiro ano presente na matriz, os fatores do ano mais antigo são usados.

    Parâmetros:
        matriz: DataFrame base contendo os anos modelo e fatores de emissão
        frota_processada_probAnoModelo: DataFrame com as probabilidades por ano modelo
        colunas_probabilidade: Lista ou string com os nomes das colunas de probabilidade a serem adicionadas

    Retorna:
        DataFrame: Atualizado com as colunas de probabilidade renomeadas como 'PROBABILIDADE ANO MODELO ...'
    """
    
    
    # matriz = matriz_pesados_exaustivas
    # frota_processada_probAnoModelo = frota_processada_prob_ano_modelo
    # colunas_probabilidade = ['PROBABILIDADE CAMINHOES DIESEL','PROBABILIDADE ONIBUS DIESEL']

    
    # Garante que colunas_probabilidade seja lista
    if isinstance(colunas_probabilidade, str):
        colunas_probabilidade = [colunas_probabilidade]

    # Filtra linhas onde pelo menos uma probabilidade é diferente de zero
    frota_filtrada = frota_processada_probAnoModelo[
        frota_processada_probAnoModelo[colunas_probabilidade].ne(0).any(axis=1)
    ]

    # Anos que faltam na matriz
    ano_min_frota = frota_filtrada['ANO MODELO'].min()
    ano_min_matriz = matriz['ANO MODELO'].min()

    if ano_min_frota < ano_min_matriz:
        anos_faltantes = list(range(ano_min_frota, ano_min_matriz))
        # Base da matriz com o primeiro ano modelo existente
        matriz_base = matriz[matriz['ANO MODELO'] == ano_min_matriz]
        # Cria cópias com os anos faltantes, apenas alterando 'ANO MODELO'
        novas_linhas = pd.concat(
            [matriz_base.assign(**{'ANO MODELO': ano}) for ano in anos_faltantes],
            ignore_index=True
        )
        # Junta com a matriz original
        matriz = pd.concat([matriz, novas_linhas], ignore_index=True)

    # Merge eficiente
    colunas_merge = ['ANO', 'MES', 'UF', 'CODIGO IBGE', 'ANO MODELO']
    df_merge = frota_filtrada[colunas_merge + colunas_probabilidade]

    matriz = matriz.merge(df_merge, on=colunas_merge, how='left')

    # Remove linhas com NaN em todas as colunas de probabilidade
    matriz = matriz.dropna(subset=colunas_probabilidade, how='all')

    # Renomeia colunas
    novos_nomes = {
        col: f'PROBABILIDADE ANO MODELO {col.replace("PROBABILIDADE ", "").strip()}'
        for col in colunas_probabilidade
    }
    matriz = matriz.rename(columns=novos_nomes)

    return matriz
#%% Adicionando Probabilidade uso de combustivel por periodo

def processar_probabilidades_combustivel_leves(matriz_com_Etanol, matriz_sem_Etanol, frota_proporcao_82, frota_proporcao_2003, frota_proporcao_2007):
    
    """
    Processa e atribui as probabilidades de uso de combustível para veículos leves, considerando diferentes períodos de fabricação e disponibilidade de combustíveis 
    em cada período

    Parâmetros:
        matriz_com_Etanol (DataFrame): Dados dos veículos em cidades que vendem etanol
        matriz_sem_Etanol (DataFrame): Dados dos veículos em cidades que não vendem etanol
        frota_proporcao_82 (DataFrame): Proporção de uso de combustível para veículos fabricados até 2002 (Gasolina e Etanol)
        frota_proporcao_2003 (DataFrame): Proporção de uso de combustível para veículos fabricados entre 2003 e 2006 (Gasolina, Etanol, Flex Gasolina e Flex Etanol)
        frota_proporcao_2007 (DataFrame): Proporção de uso de combustível para veículos fabricados a partir de 2007 (Gasolina, Flex Gasolina e Flex Etanol)

    Retorna:
        DataFrame: Atualizado com a coluna de probabilidade de uso do combustível atribuída, para municípios que vendem etanol
        DataFrame: Atualizado com a coluna de probabilidade de uso do combustível atribuída, para municípios que não vendem etanol
    """
    
    def segmentar_e_atribuir_proporcao(df, frota_proporcao, grupo_anos):
        """Filtra os dados por ANO MODELO e faz o merge com a proporção correta do combustível."""
        df_grupo = df[df['ANO MODELO'].between(grupo_anos[0], grupo_anos[1])].copy()
        df_grupo = df_grupo.merge(frota_proporcao[['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL', 'PROPORCAO']], on=['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL'], how='left')
        return df_grupo

    # Definição dos grupos de anos modelo
    grupos_anos = [(1970, 2002), (2003, 2006), (2007, 2023)]

    # Processamento das cidades que NÃO vendem etanol2
    DadosFEsem82 = segmentar_e_atribuir_proporcao(matriz_sem_Etanol, frota_proporcao_82, grupos_anos[0])
    DadosFEsem2003 = segmentar_e_atribuir_proporcao(matriz_sem_Etanol, frota_proporcao_2003, grupos_anos[1])
    DadosFEsem2007 = segmentar_e_atribuir_proporcao(matriz_sem_Etanol, frota_proporcao_2007, grupos_anos[2])

    # Unindo os grupos novamente
    matriz_sem_Etanol = pd.concat([DadosFEsem82, DadosFEsem2003, DadosFEsem2007]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])

    # Processamento das cidades que VENDEM etanol2
    DadosFEcom82 = segmentar_e_atribuir_proporcao(matriz_com_Etanol, frota_proporcao_82, grupos_anos[0])
    DadosFEcom2003 = segmentar_e_atribuir_proporcao(matriz_com_Etanol, frota_proporcao_2003, grupos_anos[1])
    DadosFEcom2007 = segmentar_e_atribuir_proporcao(matriz_com_Etanol, frota_proporcao_2007, grupos_anos[2])

    # Unindo os grupos novamente
    matriz_com_Etanol = pd.concat([DadosFEcom82, DadosFEcom2003, DadosFEcom2007]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])
    
    matriz_com_Etanol = matriz_com_Etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})
    matriz_sem_Etanol = matriz_sem_Etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})

    return matriz_com_Etanol, matriz_sem_Etanol

#NAO PODE REMOVER AS DUPLICATAS, POIS LINHAS AS QUAIS COMBUSTIVEIS ETANOL/FLEX ETANOL FORAM SUBSTITUIDOS SERÃO IGUAIS

def processar_probabilidades_combustivel_comleves(matriz_com_etanol, matriz_sem_etanol, frota_proporcao_83, frota_proporcao_2003, frota_proporcao_2006, frota_proporcao_2007):

    """
    Processa e atribui as probabilidades de uso de combustível para veículos comerciais leves, considerando diferentes períodos de fabricação e disponibilidade de combustíveis 
    em cada período. Vale ressaltar que as linhas contendo veículos à diesel são retiradas já que emissões de hidrocarbonetos por este combustível são consideradas insignificantes

    Parâmetros:
        matriz_com_Etanol (DataFrame): Dados dos veículos em cidades que vendem etanol
        matriz_sem_Etanol (DataFrame): Dados dos veículos em cidades que não vendem etanol
        frota_proporcao_82 (DataFrame): Proporção de uso de combustível para veículos fabricados até 2002 (Gasolina e Etanol)
        frota_proporcao_2003 (DataFrame): Proporção de uso de combustível para veículos fabricados entre 2003 e 2005 (Gasolina, Etanol, Flex Gasolina e Flex Etanol)
        frota_proporcao_2007 (DataFrame): Proporção de uso de combustível para veículos fabricados em 2006 (Gasolina, Etanol, Flex Gasolina, Flex Etanol e Diesel)
        frota_proporcao_2007 (DataFrame): Proporção de uso de combustível para veículos fabricados a partir de 2007 (Gasolina, Etanol, Flex Gasolina e Flex Etanol)

    Retorna:
        DataFrame: Dados processados com a probabilidade de uso do combustível atribuída, para municípios que vendem etanol
        DataFrame: Dados processados com a probabilidade de uso do combustível atribuída, para municípios que não vendem etanol
    """
    
    def segmentar_e_atribuir_proporcao(df, frota_proporcao, grupo_anos):
        """Filtra os dados por ANO MODELO e faz o merge com a proporção correta do combustível."""
        df_grupo = df[df['ANO MODELO'].between(grupo_anos[0], grupo_anos[1])].copy()
        df_grupo = df_grupo.merge(frota_proporcao[['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL', 'PROPORCAO']], 
                                  on=['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL'], how='left')
        return df_grupo

    # Definição dos grupos de anos modelo
    grupos_anos = [(1970, 2002), (2003, 2005), (2006, 2006), (2007, 2023)]

    # Processamento das cidades que NÃO vendem etanol
    DadosFEsem82 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_83, grupos_anos[0])
    DadosFEsem2003 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_2003, grupos_anos[1])
    DadosFEsem2006 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_2006, grupos_anos[2])
    DadosFEsem2007 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_2007, grupos_anos[3])

    # Unindo os grupos novamente
    matriz_sem_etanol = pd.concat([DadosFEsem82, DadosFEsem2003, DadosFEsem2006, DadosFEsem2007]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])

    # Processamento das cidades que VENDEM etanol
    DadosFEcom82 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_83, grupos_anos[0])
    DadosFEcom2003 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_2003, grupos_anos[1])
    DadosFEcom2006 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_2006, grupos_anos[2])
    DadosFEcom2007 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_2007, grupos_anos[3])

    #Unindo os grupos novamente
    matriz_com_etanol = pd.concat([DadosFEcom82, DadosFEcom2003, DadosFEcom2006, DadosFEcom2007]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])
    
    matriz_com_etanol = matriz_com_etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})
    matriz_sem_etanol = matriz_sem_etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})

    return matriz_com_etanol, matriz_sem_etanol

##NAO PODE REMOVER AS DUPLICATAS, POIS LINHAS AS QUAIS COMBUSTIVEIS etanol / FLEX etanol FORAM SUBSTITUIDOS SERÃO IGUAIS

def processar_probabilidades_combustivel_motos(matriz_com_etanol, matriz_sem_etanol, frota_proporcao_2003, frota_proporcao_2010):
    """
    Processa e atribui as probabilidades de uso de combustível para motocicletas, considerando diferentes períodos de fabricação e disponibilidade de combustíveis.

    Parâmetros:
        matriz_com_etanol (DataFrame): Dados dos veículos em cidades que vendem etanol
        matriz_sem_etanol (DataFrame): Dados dos veículos em cidades que não vendem etanol
        frota_proporcao_2003 (DataFrame): Proporção de uso de combustível para veículos fabricados entre 2003 e 2009 (Gasolina)
        frota_proporcao_2010 (DataFrame): Proporção de uso de combustível para veículos fabricados entre 2010 e 2017 (Gasolina, Flex Gasolina, Flex Etanol)

    Retorna:
        DataFrame: Dados processados com a probabilidade de uso do combustível atribuída, para municípios que vendem etanol
        DataFrame: Dados processados com a probabilidade de uso do combustível atribuída, para municípios que não vendem etanol
    """
    
    def segmentar_e_atribuir_proporcao(df, frota_proporcao, grupo_anos):
        """Filtra os dados por ANO MODELO e faz o merge com a proporção correta do combustível."""
        df_grupo = df[df['ANO MODELO'].between(grupo_anos[0], grupo_anos[1])].copy()
        df_grupo = df_grupo.merge(frota_proporcao[['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL', 'PROPORCAO']], 
                                  on=['ANO', 'MES', 'UF', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL'], how='left')
        return df_grupo

    # Definição dos grupos de anos modelo
    grupos_anos = [(1970, 2009), (2010, 2023)]

    # Processamento das cidades que NÃO vendem etanol
    DadosFEsem2003 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_2003, grupos_anos[0])
    DadosFEsem2010 = segmentar_e_atribuir_proporcao(matriz_sem_etanol, frota_proporcao_2010, grupos_anos[1])

    matriz_sem_etanol = pd.concat([DadosFEsem2003, DadosFEsem2010]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])

    # Processamento das cidades que VENDEM etanol
    DadosFEcom2003 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_2003, grupos_anos[0])
    DadosFEcom2010 = segmentar_e_atribuir_proporcao(matriz_com_etanol, frota_proporcao_2010, grupos_anos[1])

    matriz_com_etanol = pd.concat([DadosFEcom2003, DadosFEcom2010]).sort_values(by=['ANO', 'MES', 'UF', 'CODIGO IBGE'])
    
    matriz_com_etanol = matriz_com_etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})
    matriz_sem_etanol = matriz_sem_etanol.rename(columns={'PROPORCAO': 'PROBABILIDADE USO COMBUSTIVEL'})

    return matriz_com_etanol, matriz_sem_etanol

#%% Adiconando Consumo Combustivel


def atribuir_consumo_combustivel(matriz,
                                 consumo_gasolina,
                                 consumo_etanol,
                                 consumo_oleo,
                                 nome_coluna_proporcao):
    """
    Atribui o consumo de etanol, gasolina e diesel à matriz com base nos 
    códigos de combustível aproveitando dados do ano correspondente e, quando 
    ausentes, do ano mais recente disponível.
    """

    # Mapear combustíveis principais
    mapa_combustivel = {1: 1, 3: 1, 4: 5, 5: 5, 2: 2}
    matriz = matriz.copy()
    matriz['COMBUSTIVEL UTILIZADO'] = matriz['CODIGO COMBUSTIVEL'].map(mapa_combustivel)

    # Função interna para processar combustível
    def preparar_consumo(df_consumo, id_combustivel):
        df = df_consumo.copy()
        df['COMBUSTIVEL UTILIZADO'] = id_combustivel
        df_base = df[['UF', 'CODIGO IBGE', 'COMBUSTIVEL UTILIZADO', 'ANO', nome_coluna_proporcao]]

        # Prepara também versão com ano máximo
        ano_max = df['ANO'].max()
        df_ano_max = df[df['ANO'] == ano_max][['UF', 'CODIGO IBGE', 'COMBUSTIVEL UTILIZADO', nome_coluna_proporcao]]
        df_ano_max = df_ano_max.rename(columns={nome_coluna_proporcao: f'{nome_coluna_proporcao}_MAX'})

        return df_base, df_ano_max

    # Preparar todos os combustíveis
    etanol, etanol_max = preparar_consumo(consumo_etanol, 1)
    gasolina, gasolina_max = preparar_consumo(consumo_gasolina, 5)
    diesel, diesel_max = preparar_consumo(consumo_oleo, 2)

    # Concatenar bases de consumo e máximos
    consumo_all = pd.concat([etanol, gasolina, diesel], ignore_index=True)
    consumo_max_all = pd.concat([etanol_max, gasolina_max, diesel_max], ignore_index=True)

    # Merge principal
    merged = matriz.merge(consumo_all, on=['UF', 'CODIGO IBGE', 'COMBUSTIVEL UTILIZADO', 'ANO'], how='left')

    # Merge com ano máximo
    merged = merged.merge(consumo_max_all, on=['UF', 'CODIGO IBGE', 'COMBUSTIVEL UTILIZADO'], how='left')

    # Preenchimento de proporção faltante com valor do ano máximo
    merged[nome_coluna_proporcao] = merged[nome_coluna_proporcao].fillna(merged[f'{nome_coluna_proporcao}_MAX'])

    # Atribuição final às colunas específicas
    merged['CONSUMO ETANOL CATEGORIA'] = merged[nome_coluna_proporcao].where(merged['CODIGO COMBUSTIVEL'].isin([1, 3]))
    merged['CONSUMO GASOLINA CATEGORIA'] = merged[nome_coluna_proporcao].where(merged['CODIGO COMBUSTIVEL'].isin([4, 5]))
    merged['CONSUMO DIESEL CATEGORIA'] = merged[nome_coluna_proporcao].where(merged['CODIGO COMBUSTIVEL'] == 2)

    # Limpeza
    return merged.drop(columns=[nome_coluna_proporcao, f'{nome_coluna_proporcao}_MAX'])
#%%Adcionando autonomia

def adicionar_autonomia(matriz, autonomia):
    
    """
    Atribui a autonomia dos veículos de acordo com o ano modelo do veículo e o combustível utilizado. Para o caso de haver veículos em anos anteriores ao primeiro ano 
    de autonomia disponíveis, o valor de autonomia do ano mais antigo são atribuídos aos veículos de anos anteriores.

    Parâmetros:
        matriz (pd.DataFrame): DataFrame contendo os dados da matriz principal.
        autonomia (pd.DataFrame): DataFrame contendo os dados de autonomia.

    Retorna:
        pd.DataFrame: DataFrame atualizado com a coluna autonomia adicionada.
    """
    
    # Encontrar o menor ano disponível na autonomia
    menor_ano_autonomia = autonomia['ANO MODELO'].min()

    # Realizar o merge mantendo os valores originais de 'ANO MODELO'
    matriz_com_autonomia = pd.merge(matriz, autonomia[['ANO MODELO', 'CODIGO COMBUSTIVEL', 'AUTONOMIA']], 
                                    on=['ANO MODELO', 'CODIGO COMBUSTIVEL'], how='left')

    # Preencher anos anteriores ao menor ano com a autonomia do menor ano disponível
    for combustivel in autonomia['CODIGO COMBUSTIVEL'].unique():
        autonomia_min = autonomia.loc[(autonomia['ANO MODELO'] == menor_ano_autonomia) & 
                                      (autonomia['CODIGO COMBUSTIVEL'] == combustivel), 'AUTONOMIA']
        if not autonomia_min.empty:
            matriz_com_autonomia.loc[(matriz_com_autonomia['ANO MODELO'] < menor_ano_autonomia) & 
                                     (matriz_com_autonomia['CODIGO COMBUSTIVEL'] == combustivel), 'AUTONOMIA'] = autonomia_min.values[0]

    return matriz_com_autonomia

def unindo_matrizes(matriz_leves_com_etanol, matriz_leves_sem_etanol):
    
    """
    Une as matrizes de veículos com e sem etanol, ordena as colunas e reseta o índice.

    Parâmetros:
        matriz_leves_com_etanol (pd.DataFrame): DataFrame contendo a matriz de veículos com etanol.
        matriz_leves_sem_etanol (pd.DataFrame): DataFrame contendo a matriz de veículos sem etanol.

    Retorna:
        pd.DataFrame: DataFrame unificado e ordenado.
    """
    
    matriz_leves = pd.concat([matriz_leves_com_etanol, matriz_leves_sem_etanol], ignore_index=True)
    return matriz_leves

#%%Funções Exautivas

def manter_colunas_exaustivas(matriz_exaustivas, categoria_veiculo):
    """
    Filtra as colunas do DataFrame mantendo apenas as colunas exaustivas especificadas,
    incluindo a(s) coluna(s) de probabilidade e frota conforme a categoria de veículo.

    Parâmetros:
        matriz_exaustivas (pd.DataFrame): DataFrame de entrada (matriz_leves_exaustivas ou matriz_comleves_exaustivas)
        categoria_veiculo (str): 'AUTOMOVEIS', 'COMERCIAIS LEVES', 'MOTOS' ou 'PESADOS'

    Retorna:
        pd.DataFrame: DataFrame contendo apenas as colunas especificadas
    """

    # Verifica se a categoria de veículo é válida
    categorias_validas = ['AUTOMOVEIS', 'COMERCIAIS LEVES', 'MOTOS', 'PESADOS']
    if categoria_veiculo not in categorias_validas:
        raise ValueError(f"categoria_veiculo deve ser uma das: {categorias_validas}")

    # Lista base comum a todas as categorias
    colunas_base = [
        'ANO', 'MES', 'UF', 'MUNICIPIO', 'ANO MODELO', 'CODIGO IBGE', 'CODIGO COMBUSTIVEL',
        'CO', 'HCTOT', 'CH4', 'NOX', 'RCHO', 'MP', 'CO2', 'N2O', 'AUTONOMIA',
        'PROBABILIDADE USO COMBUSTIVEL',
        'COMBUSTIVEL UTILIZADO', 'CONSUMO ETANOL CATEGORIA',
        'CONSUMO GASOLINA CATEGORIA', 'CONSUMO DIESEL CATEGORIA'
    ]

    # Colunas específicas de probabilidade por categoria
    colunas_probabilidade = {
        'AUTOMOVEIS': ['PROBABILIDADE ANO MODELO LEVES'],
        'COMERCIAIS LEVES': [
            'PROBABILIDADE ANO MODELO COMLEVES',
            'PROBABILIDADE ANO MODELO COMLEVES DIESEL'
        ],
        'MOTOS': ['PROBABILIDADE ANO MODELO MOTOS'],
        'PESADOS': [
            'PROBABILIDADE CAMINHOES DIESEL',
            'PROBABILIDADE ONIBUS DIESEL'
        ]
    }

    # Coluna de frota associada por categoria, exceto para PESADOS
    colunas_frota = {
        'AUTOMOVEIS': ['AUTOMOVEIS'],
        'COMERCIAIS LEVES': ['COMERCIAIS LEVES'],
        'MOTOS': ['MOTOS'],
        'PESADOS': []  # PESADOS não possui coluna de frota direta
    }

    # Juntar todas as colunas que devem ser mantidas
    colunas_selecionadas = colunas_base + colunas_probabilidade[categoria_veiculo] + colunas_frota[categoria_veiculo]

    # Verificar disponibilidade das colunas
    colunas_disponiveis = [col for col in colunas_selecionadas if col in matriz_exaustivas.columns]
    colunas_nao_encontradas = [col for col in colunas_selecionadas if col not in matriz_exaustivas.columns]

    if colunas_nao_encontradas:
        print(f"Aviso: As seguintes colunas não foram encontradas no DataFrame e serão ignoradas: {colunas_nao_encontradas}")

    # Filtrar o DataFrame mantendo apenas as colunas selecionadas
    matriz_exaustivas_filtrado = matriz_exaustivas[colunas_disponiveis].copy()

    return matriz_exaustivas_filtrado

def fator_deterioracao(caminho_diretorio, valores_suc, anos_dados):
    
    """
    Calcula fatores de deterioração para diferentes tipos de veículos
    
    Parâmetros:
    caminho_diretorio (str): Caminho do diretório com os arquivos de fatores
    valores_suc (DataFrame): DataFrame com as colunas ANO, ANO MODELO e IDADE
    anos_dados (list/array): Lista de anos únicos a serem processados
    
    Retorna:
    Tuple: (deter_leves_otto, deter_leves_diesel, deter_motos_otto, deter_pesados)
    """
    
    # Leitura dos arquivos
    def ler_fatores(arquivo):
        return pd.read_excel(f"{caminho_diretorio}/{arquivo}")
    
    fator_deterioracao_leves = ler_fatores("Fatores_Deterioracao_Leves.xlsx")
    fator_deterioracao_pesados = ler_fatores("Fatores_Deterioracao_Pesados.xlsx")
    fator_deterioracao_motos = ler_fatores("Fatores_Deterioracao_Motos.xlsx")

    # Filtrar apenas os anos de interesse
    anos_deterioracao = valores_suc[valores_suc['ANO'].isin(anos_dados)][['ANO', 
                                                'ANO MODELO', 'IDADE']].copy()
    
    # Criar DataFrames de saída com cópia explícita
    def criar_df_base(df):
        return df[['ANO', 'ANO MODELO', 'IDADE']].copy()
    
    # Função para adicionar fatores
    def adicionar_fatores(df, fatores):
        for col, val in fatores.items():
            df[col] = val
        return df

    # Obter fatores para cada tipo de veículo
    fatores_otto = fator_deterioracao_leves[fator_deterioracao_leves
                                            ['Ciclo do Motor'] == 'Otto'].iloc[0, 1:].to_dict()
    
    fatores_diesel = fator_deterioracao_leves[fator_deterioracao_leves
                                            ['Ciclo do Motor'] == 'Diesel'].iloc[0, 1:].to_dict()
    
    fatores_pesados = fator_deterioracao_pesados.iloc[0, 1:].to_dict()
    
    fatores_motos = fator_deterioracao_motos.iloc[0, 1:].to_dict()

    # Criar DataFrames para cada tipo de veículo
    deter_leves_otto = adicionar_fatores(criar_df_base(anos_deterioracao), fatores_otto)
    deter_leves_diesel = adicionar_fatores(criar_df_base(anos_deterioracao), fatores_diesel)
    deter_pesados = adicionar_fatores(criar_df_base(anos_deterioracao), fatores_pesados)
    deter_motos_otto = adicionar_fatores(criar_df_base(anos_deterioracao), fatores_motos)

    # Aplicar regras de deterioração
    def aplicar_regras_deterioracao(df):
        # Para veículos com mais de 40 anos: fator = 0
        mask = df['IDADE'] > 40
        cols_fatores = list(fatores_otto.keys())  # Colunas de fatores
        
        for col in cols_fatores:
            if col in df.columns:
                df.loc[mask, col] = 0
        
        # Para veículos com 5 anos ou menos: fator = 1
        mask = df['IDADE'] <= 5
        for col in cols_fatores:
            if col in df.columns:
                df.loc[mask, col] = 1
        
        return df

    # Aplicar regras a todos os DataFrames
    deter_leves_otto = aplicar_regras_deterioracao(deter_leves_otto)
    deter_leves_diesel = aplicar_regras_deterioracao(deter_leves_diesel)
    deter_pesados = aplicar_regras_deterioracao(deter_pesados)
    deter_motos_otto = aplicar_regras_deterioracao(deter_motos_otto)

    # Cálculo do acúmulo de rodagem (blocos de 5 anos)
    def calcular_expoentes(idades):
        fdeter_idade = []
        cont = 0
        mm = 0
        
        for ii, idade in enumerate(sorted(idades)):
            jj = ii - mm - 1
            if jj > 5:
                if cont < 3:
                    cont += 1
                    mm = ii - 2
                else:
                    cont = 3
            fdeter_idade.append((idade, max(cont, 1)))
        
        return pd.DataFrame(fdeter_idade, columns=['IDADE', 'Expoente'])

    fdeter_df = calcular_expoentes(anos_deterioracao['IDADE'].unique())

    # Aplicar expoentes aos fatores
    def aplicar_expoentes(df, fdeter_df):
        df = df.merge(fdeter_df, on='IDADE', how='left')
        cols_fatores = list(fatores_otto.keys())
        
        for col in cols_fatores:
            if col in df.columns:
                df[col] = df[col] ** df['Expoente']
        
        return df.drop(columns=['Expoente'])

    deter_leves_otto = aplicar_expoentes(deter_leves_otto, fdeter_df)
    deter_leves_diesel = aplicar_expoentes(deter_leves_diesel, fdeter_df)
    deter_pesados = aplicar_expoentes(deter_pesados, fdeter_df)
    deter_motos_otto = aplicar_expoentes(deter_motos_otto, fdeter_df)

    return deter_leves_otto, deter_leves_diesel, deter_motos_otto, deter_pesados

def aplicar_deterioracao_veiculos(matriz_exaustivas, fatores_deterioracao, tipo_veiculo=None):
    """
    Aplica fatores de deterioração em matrizes de emissões de veículos.
    Versão corrigida para funcionar com leves e comerciais leves.
    """
    # matriz_exaustivas = matriz_pesados_exaustivas
    # fatores_deterioracao = deter_pesados
    # tipo_veiculo = 'pesados'
    
    # matriz_exaustivas = matriz_comleves_exaustivas
    # fatores_deterioracao = [deter_leves_otto, deter_leves_diesel], 
    # tipo_veiculo = 'comerciais_leves'
    
    # matriz_exaustivas = matriz_leves_exaustivas
    # fatores_deterioracao = deter_leves_otto
    # tipo_veiculo = 'leves'
    
    # matriz_exaustivas = matriz_motos_exaustivas
    # fatores_deterioracao = deter_motos_otto
    # tipo_veiculo = 'motos'
    matriz_resultado = matriz_exaustivas.copy()
    
    # Mapeamento de colunas de poluentes
    mapeamento_colunas = {
        'leves': {'NMHC': 'HCTOT', 'CO': 'CO', 'NOx': 'NOX', 'RCHO': 'RCHO', 'MP': 'MP', 'CH4': 'CH4'},
        'comerciais_leves': {'NMHC': 'HCTOT', 'CO': 'CO', 'NOx': 'NOX', 'RCHO': 'RCHO', 'MP': 'MP', 'CH4': 'CH4'},
        'motos': {'HC': 'HCTOT', 'CO': 'CO', 'NOx': 'NOX', 'MP': 'MP', 'CH4': 'CH4'},
        'pesados': {'CO': 'CO', 'NOx': 'NOX', 'MP': 'MP'}
    }
    
    
    
    # Configurar fatores de deterioração
    if tipo_veiculo == 'comerciais_leves':
        combustiveis_otto = [1, 3, 4, 5]  # Etanol, Flex Etanol, Flex Gasolina, Gasolina
        combustiveis_diesel = [2]         # Diesel

        deter_otto = fatores_deterioracao[0].copy()
        
        deter_diesel = fatores_deterioracao[1].copy()
        # Aplicar fatores Otto
        for _, linha in deter_otto.iterrows():
            mascara = (
                (matriz_resultado['ANO'] == linha['ANO']) & 
                (matriz_resultado['ANO MODELO'] == linha['ANO MODELO']) &
                (matriz_resultado['CODIGO COMBUSTIVEL'].isin(combustiveis_otto))
            )
            for col_deter, col_matriz in mapeamento_colunas['comerciais_leves'].items():
                if col_deter in linha and col_matriz in matriz_resultado.columns:
                    matriz_resultado.loc[mascara, col_matriz] *= linha[col_deter]

        # Aplicar fatores Diesel
        for _, linha in deter_diesel.iterrows():
            mascara = (
                (matriz_resultado['ANO'] == linha['ANO']) & 
                (matriz_resultado['ANO MODELO'] == linha['ANO MODELO']) &
                (matriz_resultado['CODIGO COMBUSTIVEL'].isin(combustiveis_diesel))
            )
            for col_deter, col_matriz in mapeamento_colunas['comerciais_leves'].items():
                if col_deter in linha and col_matriz in matriz_resultado.columns:
                    matriz_resultado.loc[mascara, col_matriz] *= linha[col_deter]

    elif tipo_veiculo == 'leves':
        fatores_deter = fatores_deterioracao.copy()
        for _, linha in fatores_deter.iterrows():
            mascara = (
                (matriz_resultado['ANO'] == linha['ANO']) & 
                (matriz_resultado['ANO MODELO'] == linha['ANO MODELO'])
            )
            for col_deter, col_matriz in mapeamento_colunas['leves'].items():
                if col_deter in linha and col_matriz in matriz_resultado.columns:
                    matriz_resultado.loc[mascara, col_matriz] *= linha[col_deter]
    
    elif tipo_veiculo == "motos":
        fatores_deterioracao = fatores_deterioracao.rename(columns={'NOx': 'NOX','NMHC': 'HCTOT'})
        deter = fatores_deterioracao
        df_merge = matriz_resultado.merge(
            deter,
            on=["ANO", "ANO MODELO"],
            how="left",
            suffixes=("", "_deter")
        )
        for poluente in ["CO", "HCTOT", "NOX", "MP", "CH4"]:
            col_deter = poluente + "_deter"
            if col_deter in df_merge.columns:
                matriz_resultado[poluente] *= df_merge[col_deter].fillna(1)


    elif tipo_veiculo == "pesados":
        deter = fatores_deterioracao
        df_merge = matriz_resultado.merge(
            deter,
            on=["ANO", "ANO MODELO"],
            how="left",
            suffixes=("", "_deter")
        )
        for poluente in ["CO", "HCTOT", "NOX", "MP", "CH4"]:
            col_deter = poluente + "_deter"
            if col_deter in df_merge.columns:
                matriz_resultado[poluente] *= df_merge[col_deter].fillna(1)

    return matriz_resultado

def adicionar_temp_uso_dias(matriz, temperatura_media, categoria_veiculo):
   # matriz = matriz_leves
   # categoria_veiculo = 'AUTOMOVEIS'
    """
    Adiciona a temperatura média à matriz de veículos para o tipo de veículo especificado, classifica os municípios por faixa de temperatura 
    e ajusta os fatores de emissão correspondentes.
    
    Parâmetros:
    matriz (DataFrame): Contendo os dados já processados dos veículos
    temperatura_media (DataFrame): Contendo a temperatura média por município e UF
    categoria_veiculo (str): Tipo de veículo a ser considerado
    
    Retorna:
    DataFrame: Atualizado com a classificação por temperatura, fatores de emissão adequados e colunas adicionais.
    """
    
    # Validação do parâmetro categoria_veiculo
    if categoria_veiculo not in ['AUTOMOVEIS', 'COMERCIAIS LEVES', 'MOTOS', 'PESADOS']:
        raise ValueError("categoria_veiculo deve ser 'AUTOMOVEIS' ou 'COMERCIAIS LEVES' ou 'MOTOS" "PESADOS")

    temperatura_media['TEMPERATURA MEDIA'] = temperatura_media['JANEIRO']
    temperatura_media['TEMPERATURA MEDIA'] = (temperatura_media['TEMPERATURA MEDIA'].astype(str)
                                             .replace('-', np.nan)
                                             .replace('', np.nan))

    # Converte para numérico
    temperatura_media['TEMPERATURA MEDIA'] = pd.to_numeric(temperatura_media['TEMPERATURA MEDIA'], errors='coerce')

    # Adicionando a temperatura média à matriz
    matriz = matriz.merge(temperatura_media, left_on=['MUNICIPIO', 'UF'], right_on=['NOME DA ESTACAO', 'UF'], how='left')
    
    # Preenchendo valores NaN com 25 °C
    matriz['TEMPERATURA MEDIA'] = matriz['TEMPERATURA MEDIA'].fillna(25)
    matriz['TEMPERATURA MEDIA'] = pd.to_numeric(matriz['TEMPERATURA MEDIA'], errors='coerce')
    
    # Classificando os municípios por faixa de temperatura
    matriz['GRUPO_TEMPERATURA'] = 0  # Inicializando a coluna
    matriz.loc[matriz['TEMPERATURA MEDIA'] <= 7.5, 'GRUPO_TEMPERATURA'] = 1
    matriz.loc[(matriz['TEMPERATURA MEDIA'] > 7.5) & (matriz['TEMPERATURA MEDIA'] <= 17.5), 'GRUPO_TEMPERATURA'] = 2
    matriz.loc[matriz['TEMPERATURA MEDIA'] > 17.5, 'GRUPO_TEMPERATURA'] = 3
    
    # Define colunas de probabilidade conforme a categoria de veículo
    if categoria_veiculo == 'AUTOMOVEIS':
        col_prob = ['PROBABILIDADE ANO MODELO LEVES']
        col_veic = [categoria_veiculo]
    elif categoria_veiculo == 'COMERCIAIS LEVES':
        col_prob = ['PROBABILIDADE ANO MODELO COMLEVES', 'PROBABILIDADE ANO MODELO COMLEVES DIESEL']
        col_veic = [categoria_veiculo]
    elif categoria_veiculo == 'MOTOS':
        col_prob = ['PROBABILIDADE ANO MODELO MOTOS']
        col_veic = [categoria_veiculo]
    elif categoria_veiculo == 'PESADOS':
        col_prob = ['PROBABILIDADE CAMINHOES DIESEL', 'PROBABILIDADE ONIBUS DIESEL']
        col_veic = []  # Não há uma coluna única representando a categoria
    else:
        raise ValueError("categoria_veiculo deve ser 'AUTOMOVEIS', 'COMERCIAIS LEVES', 'MOTOS' ou 'PESADOS'")

    colunas_comuns = ['ANO', 'MES', 'UF', 'CODIGO IBGE', 'MUNICIPIO', 'ANO MODELO'] + col_veic + \
                     ['CODIGO COMBUSTIVEL'] + col_prob + \
                     ['PROBABILIDADE USO COMBUSTIVEL', 'COMBUSTIVEL UTILIZADO',
                      'CONSUMO ETANOL CATEGORIA', 'CONSUMO GASOLINA CATEGORIA', 'AUTONOMIA']
    
    grupo1 = matriz[matriz['GRUPO_TEMPERATURA'] == 1][colunas_comuns + ['FE_DIURNAL_0A15',
                                                                        'FE_HOTSOAK_0A15',
                                                                        'FE_RUNNINGLOSSES_0A15']]
    grupo2 = matriz[matriz['GRUPO_TEMPERATURA'] == 2][colunas_comuns + ['FE_DIURNAL_10A25',
                                                                        'FE_HOTSOAK_10A25',
                                                                        'FE_RUNNINGLOSSES_10A25']]
    grupo3 = matriz[matriz['GRUPO_TEMPERATURA'] == 3][colunas_comuns + ['FE_DIURNAL_20A35', 
                                                                        'FE_HOTSOAK_20A35', 
                                                                        'FE_RUNNINGLOSSES_20A35']]
    
    # Unindo os grupos
    matriz_completa = pd.concat([grupo1, grupo2, grupo3])
    
    # Adicionando colunas fixas
    matriz_completa['NUMERO DE DIAS'] = 31
    matriz_completa['INTENSIDADE USO'] = 1/8
    # Criando a coluna com o de combustivel consumo respectivo
    matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'] = matriz_completa[['CONSUMO ETANOL CATEGORIA', 'CONSUMO GASOLINA CATEGORIA']].bfill(axis=1).iloc[:, 0]
    
    # Ordenando a matriz final
    matriz_completa = matriz_completa.sort_values(by=['ANO', 'MES', 'CODIGO IBGE', 'ANO MODELO'])
    
    return matriz_completa

#%% Estimativas Emissoes Evaporativas

def calculo_emissao_diurnal(matriz_completa, coluna_veiculos):
    
    """
    Calcula as emissões evaporativas diurnal, de acordo com a fórmula: 
    EMISSAO DIURNAL = Número de Veículos * Probabilidade Ano Modelo * Probabilidade Uso Combustível * Número de Dias * Fator ED

    A emissão só será calculada se houver consumo de combustível (CONSUMO COMBUSTIVEL RESPECTIVO > 0).

    Parâmetros:
    matriz_completa (DataFrame): Dados da frota veicular, incluindo fatores de emissão e consumo de combustível.
    coluna_veiculos (str): Nome da coluna com a quantidade de veículos.

    Retorna:
    DataFrame: Atualizado com colunas 'FATOR ED' e 'EMISSAO DIURNAL'.
    """
    
    # matriz_completa = matriz_leves_completa
    # coluna_veiculos = 'AUTOMOVEIS'
    
    colunas_ed = ['FE_DIURNAL_0A15', 'FE_DIURNAL_10A25', 'FE_DIURNAL_20A35']

    # Encontrando o fator ED 
    matriz_completa['FATOR ED'] = matriz_completa[colunas_ed].bfill(axis=1).iloc[:, 0]

    # Inicializando com zero
    matriz_completa['EMISSAO DIURNAL'] = 0.0

    # Máscara para linhas com consumo válido
    condicao_consumo = (
        matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'].notna() &
        (matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'] > 0)
    )

    if coluna_veiculos == 'AUTOMOVEIS':
        prob_col = 'PROBABILIDADE ANO MODELO LEVES'
        matriz_completa.loc[condicao_consumo, 'EMISSAO DIURNAL'] = (
            matriz_completa.loc[condicao_consumo, coluna_veiculos] *
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'NUMERO DE DIAS'] *
            matriz_completa.loc[condicao_consumo, 'FATOR ED']
        )

    elif coluna_veiculos == 'COMERCIAIS LEVES':
        diesel_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] == 2)
        outros_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] != 2)

        matriz_completa.loc[diesel_mask, 'EMISSAO DIURNAL'] = (
            matriz_completa.loc[diesel_mask, coluna_veiculos] *
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE ANO MODELO COMLEVES DIESEL'] *
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[diesel_mask, 'NUMERO DE DIAS'] *
            matriz_completa.loc[diesel_mask, 'FATOR ED']
        )

        matriz_completa.loc[outros_mask, 'EMISSAO DIURNAL'] = (
            matriz_completa.loc[outros_mask, coluna_veiculos] *
            matriz_completa.loc[outros_mask, 'PROBABILIDADE ANO MODELO COMLEVES'] *
            matriz_completa.loc[outros_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[outros_mask, 'NUMERO DE DIAS'] *
            matriz_completa.loc[outros_mask, 'FATOR ED']
        )

    elif coluna_veiculos == 'MOTOS':
        prob_col = 'PROBABILIDADE ANO MODELO MOTOS'
        matriz_completa.loc[condicao_consumo, 'EMISSAO DIURNAL'] = (
            matriz_completa.loc[condicao_consumo, coluna_veiculos] *
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'NUMERO DE DIAS'] *
            matriz_completa.loc[condicao_consumo, 'FATOR ED']
        )

    else:
        raise ValueError("Coluna de veículos inválida. Use: 'AUTOMOVEIS', 'COMERCIAIS LEVES' ou 'MOTOS'.")

    return matriz_completa

def calculo_emissao_hotsoak(matriz_completa, coluna_veiculos):
    """
    Calcula as emissões evaporativas hot soak, de acordo com a fórmula: 
    EMISSAO HOT SOAK = Veículos * Probabilidade Ano Modelo * Probabilidade Uso Combustível * Intensidade de Uso * Consumo de Combustível * Autonomia * Fator HS

    Parâmetros:
    matriz_completa (DataFrame): dados da frota, com colunas de probabilidade, consumo, autonomia etc.
    coluna_veiculos (str): nome da coluna com a quantidade de veículos, por categoria ('AUTOMOVEIS', 'COMERCIAIS LEVES' ou 'MOTOS')

    Retorna:
    DataFrame: com colunas adicionadas 'FATOR HS' e 'EMISSAO HOT SOAK'
    """

    colunas_hs = ['FE_HOTSOAK_0A15', 'FE_HOTSOAK_10A25', 'FE_HOTSOAK_20A35']

    # Encontrando o fator HS
    matriz_completa['FATOR HS'] = matriz_completa[colunas_hs].bfill(axis=1).iloc[:, 0]

    # Inicializando a coluna de emissões
    matriz_completa['EMISSAO HOT SOAK'] = 0.0

    # Máscara de linhas com consumo válido
    condicao_consumo = (
        matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'].notna() &
        (matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'] > 0)
    )

    if coluna_veiculos == 'AUTOMOVEIS':
        prob_col = 'PROBABILIDADE ANO MODELO LEVES'
        matriz_completa.loc[condicao_consumo, 'EMISSAO HOT SOAK'] = (
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'INTENSIDADE USO'] *
            matriz_completa.loc[condicao_consumo, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[condicao_consumo, 'AUTONOMIA'] *
            matriz_completa.loc[condicao_consumo, 'FATOR HS']
        )

    elif coluna_veiculos == 'COMERCIAIS LEVES':
        diesel_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] == 2)
        outros_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] != 2)

        matriz_completa.loc[diesel_mask, 'EMISSAO HOT SOAK'] = (
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE ANO MODELO COMLEVES DIESEL'] *
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[diesel_mask, 'INTENSIDADE USO'] *
            matriz_completa.loc[diesel_mask, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[diesel_mask, 'AUTONOMIA'] *
            matriz_completa.loc[diesel_mask, 'FATOR HS']
        )

        matriz_completa.loc[outros_mask, 'EMISSAO HOT SOAK'] = (
            matriz_completa.loc[outros_mask, 'PROBABILIDADE ANO MODELO COMLEVES'] *
            matriz_completa.loc[outros_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[outros_mask, 'INTENSIDADE USO'] *
            matriz_completa.loc[outros_mask, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[outros_mask, 'AUTONOMIA'] *
            matriz_completa.loc[outros_mask, 'FATOR HS']
        )

    elif coluna_veiculos == 'MOTOS':
        prob_col = 'PROBABILIDADE ANO MODELO MOTOS'
        matriz_completa.loc[condicao_consumo, 'EMISSAO HOT SOAK'] = (
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'INTENSIDADE USO'] *
            matriz_completa.loc[condicao_consumo, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[condicao_consumo, 'AUTONOMIA'] *
            matriz_completa.loc[condicao_consumo, 'FATOR HS']
        )

    else:
        raise ValueError("Coluna de veículos inválida. Use: 'AUTOMOVEIS', 'COMERCIAIS LEVES' ou 'MOTOS'.")

    return matriz_completa

def calculo_emissao_running_losses(matriz_completa, coluna_veiculos):
    """
    Calcula as emissões evaporativas running losses, de acordo com a fórmula: 
    EMISSAO RUNNING LOSSES = Veículos * Probabilidade Ano Modelo * Probabilidade Uso Combustível * Intensidade de Uso * 
                              Consumo de Combustível * Autonomia * Fator RL

    Parâmetros:
    matriz_completa (DataFrame): dados da frota, com colunas de probabilidade, consumo, autonomia etc.
    coluna_veiculos (str): nome da coluna com a quantidade de veículos, por categoria ('AUTOMOVEIS', 'COMERCIAIS LEVES' ou 'MOTOS')

    Retorna:
    DataFrame: com colunas adicionadas 'FATOR RL' e 'EMISSAO RUNNING LOSSES'
    """

    colunas_rl = ['FE_RUNNINGLOSSES_0A15', 'FE_RUNNINGLOSSES_10A25', 'FE_RUNNINGLOSSES_20A35']

    # Encontrando o fator RL
    matriz_completa['FATOR RL'] = matriz_completa[colunas_rl].bfill(axis=1).iloc[:, 0]

    # Inicializando a coluna de emissões
    matriz_completa['EMISSAO RUNNING LOSSES'] = 0.0

    # Máscara de linhas com consumo válido
    condicao_consumo = (
        matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'].notna() &
        (matriz_completa['CONSUMO COMBUSTIVEL RESPECTIVO'] > 0)
    )

    if coluna_veiculos == 'AUTOMOVEIS':
        prob_col = 'PROBABILIDADE ANO MODELO LEVES'
        matriz_completa.loc[condicao_consumo, 'EMISSAO RUNNING LOSSES'] = (
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'INTENSIDADE USO'] *
            matriz_completa.loc[condicao_consumo, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[condicao_consumo, 'AUTONOMIA'] *
            matriz_completa.loc[condicao_consumo, 'FATOR RL']
        )

    elif coluna_veiculos == 'COMERCIAIS LEVES':
        diesel_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] == 2)
        outros_mask = condicao_consumo & (matriz_completa['CODIGO COMBUSTIVEL'] != 2)

        matriz_completa.loc[diesel_mask, 'EMISSAO RUNNING LOSSES'] = (
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE ANO MODELO COMLEVES DIESEL'] *
            matriz_completa.loc[diesel_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[diesel_mask, 'INTENSIDADE USO'] *
            matriz_completa.loc[diesel_mask, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[diesel_mask, 'AUTONOMIA'] *
            matriz_completa.loc[diesel_mask, 'FATOR RL']
        )

        matriz_completa.loc[outros_mask, 'EMISSAO RUNNING LOSSES'] = (
            matriz_completa.loc[outros_mask, 'PROBABILIDADE ANO MODELO COMLEVES'] *
            matriz_completa.loc[outros_mask, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[outros_mask, 'INTENSIDADE USO'] *
            matriz_completa.loc[outros_mask, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[outros_mask, 'AUTONOMIA'] *
            matriz_completa.loc[outros_mask, 'FATOR RL']
        )

    elif coluna_veiculos == 'MOTOS':
        prob_col = 'PROBABILIDADE ANO MODELO MOTOS'
        matriz_completa.loc[condicao_consumo, 'EMISSAO RUNNING LOSSES'] = (
            matriz_completa.loc[condicao_consumo, prob_col] *
            matriz_completa.loc[condicao_consumo, 'PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_completa.loc[condicao_consumo, 'INTENSIDADE USO'] *
            matriz_completa.loc[condicao_consumo, 'CONSUMO COMBUSTIVEL RESPECTIVO'] *
            matriz_completa.loc[condicao_consumo, 'AUTONOMIA'] *
            matriz_completa.loc[condicao_consumo, 'FATOR RL']
        )

    else:
        raise ValueError("Coluna de veículos inválida. Use: 'AUTOMOVEIS', 'COMERCIAIS LEVES' ou 'MOTOS'.")

    return matriz_completa

#%% Processamento de matrizes exaustivas

def calcular_emissoes_veiculares_exaustivas(matriz_exaustivas, categoria_veiculo):
    """
    Calcula as emissões veiculares conforme a equação:
    Emissão = Prob_Ano_Modelo * Prob_Combustivel * (Prob_Motorizacao, se aplicável) * Quant_Veículos * Fator_Emissão * Autonomia * Consumo_Combustivel
    
    ATUALIZAÇÃO PARA COMERCIAIS LEVES:
    - Usa APENAS 'PROBABILIDADE COMERCIAS LEVES' (coluna 20 do YEARMOD) para cálculo,
      ignorando distinções por combustível.
    """
    matriz = matriz_exaustivas.copy()

    # Definir configurações por categoria
    configuracoes = {
        'AUTOMOVEIS': {
            'usa_prob_motorizacao': False,
            'poluentes': ['CO', 'HCTOT', 'CH4', 'NOX', 'RCHO', 'MP', 'CO2', 'N2O']
        },
        'COMERCIAIS LEVES': {
            'usa_prob_motorizacao': False,
            'poluentes': ['CO', 'HCTOT', 'CH4', 'NOX', 'RCHO', 'MP', 'CO2', 'N2O']
        },
        'MOTOS': {
            'usa_prob_motorizacao': True,
            'poluentes': ['CO', 'HCTOT', 'CH4', 'NOX', 'MP', 'CO2', 'N2O']
        },
        'PESADOS': {
            'usa_prob_motorizacao': True,
            'poluentes': ['CO', 'HCTOT', 'CH4', 'NOX', 'RCHO', 'MP', 'CO2', 'N2O']
        }
    }

    if categoria_veiculo not in configuracoes:
        raise ValueError(f"Categoria de veículo inválida: {categoria_veiculo}")

    config = configuracoes[categoria_veiculo]

    # Atribuição da probabilidade por ano modelo (MODIFICAÇÃO PRINCIPAL PARA COMERCIAIS LEVES)
    if categoria_veiculo == 'AUTOMOVEIS':
        matriz['PROBABILIDADE ANO MODELO'] = matriz['PROBABILIDADE ANO MODELO LEVES']

    elif categoria_veiculo == 'COMERCIAIS LEVES':
        # Usa APENAS 'PROBABILIDADE COMERCIAS LEVES' (coluna 20 do YEARMOD)
        matriz['PROBABILIDADE ANO MODELO'] = matriz['PROBABILIDADE ANO MODELO COMLEVES']

    elif categoria_veiculo == 'MOTOS':
        matriz['PROBABILIDADE ANO MODELO'] = matriz['PROBABILIDADE ANO MODELO MOTOS']

    elif categoria_veiculo == 'PESADOS':
        matriz['PROBABILIDADE ANO MODELO'] = matriz['PROBABILIDADE ANO MODELO PESADOS'] #alterar para caminhões ou ônibus

    # Termo comum (prob ano modelo * prob combustível)
    termo_comum = (
        matriz['PROBABILIDADE ANO MODELO'] *
        matriz['PROBABILIDADE USO COMBUSTIVEL']
    )

    # Adiciona motorização se necessário (não aplicável para COMERCIAIS LEVES)
    if config['usa_prob_motorizacao']:
        if 'PROBABILIDADE_MOTORIZACAO' not in matriz.columns:
            raise ValueError("Faltando coluna 'PROBABILIDADE_MOTORIZACAO'")
        termo_comum *= matriz['PROBABILIDADE_MOTORIZACAO']

    # Cálculo de consumo baseado no combustível utilizado (mantido original)
    def obter_consumo(row):
        if row['COMBUSTIVEL UTILIZADO'] == 5:
            return row['CONSUMO GASOLINA CATEGORIA']
        elif row['COMBUSTIVEL UTILIZADO'] == 1:
            return row['CONSUMO ETANOL CATEGORIA']
        elif row['COMBUSTIVEL UTILIZADO'] == 2:
            return row['CONSUMO DIESEL CATEGORIA']
        else:
            return 0.0

    matriz['CONSUMO UTILIZADO'] = matriz.apply(obter_consumo, axis=1)

    # Cálculo das emissões (mantido original)
    for poluente in config['poluentes']:
        if poluente not in matriz.columns:
            matriz[poluente] = 0.0

        matriz[f'EMISSAO {poluente}'] = (
            termo_comum *
            matriz[poluente] *
            matriz['AUTONOMIA'] *
            matriz['CONSUMO UTILIZADO']
        )
        
    return matriz

#%% Pesados 
## Funções pesados
def adicionar_fatores_emissao_pesados(frota_categoria_processada,
                                      fator_emissao):
    """
    Adiciona os fatores de emissão aos dados da frota de veículos pesados,
    removendo as colunas de categorias que não serão consideradas.

    Parâmetros:
        frota_categoria_processada (pd.DataFrame): DataFrame com dados de
        frota por categoria.

        fator_emissao (pd.DataFrame): DataFrame com fatores de emissão por
        categoria de veículo.

    Retorna:
        pd.DataFrame: DataFrame com os fatores de emissão adicionados.
    """

    # Criar cópias para evitar modificações nos dados originais
    frota = frota_categoria_processada.copy()
    fatores = fator_emissao.copy()

    # Criar chave temporária para merge cartesiano, designando para cada linha
    # da matriz frota, todos os fatores de emissao
    frota["key"] = 1
    fatores["key"] = 1

    # Merge cartesiano e remoção da chave temporária
    resultado = pd.merge(frota, fatores, on="key").drop("key", axis=1)

    # Reorganizar coluna "ANO MODELO", se existir
    if "ANO MODELO" in resultado.columns:
        col = resultado.pop("ANO MODELO")
        resultado.insert(4, "ANO MODELO", col)

    # Colunas a eliminar
    colunas_remover = [
        'AUTOMOVEIS', 'COMERCIAIS LEVES', 'NAO CONSIDERADO', 'CICLOMOTOR',
        'MOTOCICLETA', 'MOTONETA', 'QUADRICICLO', 'TRICICLO', 'CAMINHAO',
        'CAMINHAO TRATOR', 'CHASSI PLATAF', 'MICRO-ONIBUS', 'ONIBUS',
        'TRATOR ESTEI', 'TRATOR RODAS'
    ]

    # Remover colunas indesejadas, se existirem
    colunas_presentes = [col for col in colunas_remover if col in resultado.columns]
    resultado = resultado.drop(columns=colunas_presentes)

    return resultado



def adicionar_probabilidade_subcategoria_pesados(
                                            matriz_pesados_exaustivas,
                                            matriz_probabilidade_pesados):

    """
    Adiciona a matriz de pesados uma coluna com a probabilidade
    de pertencimento a uma subcategoria ('Semileves', 'Leves', 'Médios',
    'Semipesados', 'Pesados','Urbanos', 'Rodoviários', e 'MicroOnibus', sendo a
    de micro onibus existente apenas após 2005) com base em dados de
    probabilidade por município, e período.

    Parâmetros:
        matriz_pesados_exaustivas (pd.DataFrame): DataFrame contendo dados
            exaustivos de veículos pesados, incluindo colunas como 'CATEGORIA',
            'ANO MODELO', 'CODIGO IBGE', 'UF', 'MUNICIPIO', 'ANO' e 'MES'.

        matriz_probabilidade_pesados (pd.DataFrame): DataFrame com as
            probabilidades de subcategorias por município, ano e mês, contendo
            colunas como 'PROB_SEMILEVES', 'PROB_LEVES', etc.

    Retorna:
        pd.DataFrame: DataFrame original com a coluna 'PROB_SUBCATEGORIA'
        adicionada, contendo a probabilidade correspondente à subcategoria
        atribuída.
    """

    df_merged = matriz_pesados_exaustivas.merge(
        matriz_probabilidade_pesados,
        on=['CODIGO IBGE', 'UF', 'MUNICIPIO', 'ANO', 'MES'],
        how='left'
    )

    condicoes = [
        (df_merged['CATEGORIA'] == 'Semileves'),
        (df_merged['CATEGORIA'] == 'Leves'),
        (df_merged['CATEGORIA'] == 'Médios'),
        (df_merged['CATEGORIA'] == 'Semipesados'),
        (df_merged['CATEGORIA'] == 'Pesados'),
        ((df_merged['CATEGORIA'] == 'Urbanos') &
         (df_merged['ANO MODELO'] <= 2005)),
        ((df_merged['CATEGORIA'] == 'Urbanos') &
         (df_merged['ANO MODELO'] > 2005)),
        ((df_merged['CATEGORIA'] == 'Rodoviários') &
         (df_merged['ANO MODELO'] <= 2005)),
        ((df_merged['CATEGORIA'] == 'Rodoviários') &
         (df_merged['ANO MODELO'] > 2005)),
        (df_merged['CATEGORIA'] == 'MicroOnibus')
    ]

    escolhas = [
        df_merged['PROB_SEMILEVES'],
        df_merged['PROB_LEVES'],
        df_merged['PROB_MEDIOS'],
        df_merged['PROB_SEMIPESADOS'],
        df_merged['PROB_PESADOS'],
        df_merged['PROB_URBANOS_ATE_2005'],
        df_merged['PROB_URBANOS'],
        df_merged['PROB_RODOVIARIOS_ATE_2005'],
        df_merged['PROB_RODOVIARIOS'],
        df_merged['PROB_MICROONIBUS']
    ]

    df_merged['PROB_SUBCATEGORIA'] = np.select(
        condicoes, escolhas, default=np.nan)

    return df_merged



def calcular_emissoes_veiculares_exaustivas_pesados(matriz_pesados_exaustivas, 
                                                    fator_emissao_pesados_exaustivas_CO2):

    """
    Calcula as emissões veiculares de exaustão para veículos pesados

    Com base na equação:
        Emissão = (
            Prob_Ano_Modelo *
            Prob_Combustível *
            Fator_Emissão (já está incluido o fator de deterioração) *
            Autonomia *
            Consumo_Combustível (já está inclusa a proporção de cada categoria)
        )

    Parâmetros:
    matriz_pesados_exaustivas (pd.DataFrame): DataFrame contendo os dados com
    as colunas necessárias.

    Retorna:
    pd.DataFrame
        DataFrame com as colunas originais e as colunas adicionais de emissões
        para cada poluente.
    """

    # Definir qual coluna de probabilidade utilizar com base na categoria
    cond_onibus = (matriz_pesados_exaustivas['CATEGORIA']
                   .str.contains('Urbanos|Rodoviários|MicroOnibus',
                                 case=False, na=False))

    matriz_pesados_exaustivas['PROB_ANO_MODELO'] = (
        matriz_pesados_exaustivas['PROBABILIDADE ANO MODELO ONIBUS DIESEL'])
    matriz_pesados_exaustivas.loc[~cond_onibus, 'PROB_ANO_MODELO'] = (
        matriz_pesados_exaustivas.loc[~cond_onibus,
                                      'PROBABILIDADE ANO MODELO CAMINHOES DIESEL'])
    


    
    # Lista de poluentes e respectivas colunas de fator de emissão
    poluentes = ['CO', 'HCTOT', 'CH4', 'NOX', 'MP', 'N2O']

    for poluente in poluentes:
        coluna_emissao = f'EMISSAO {poluente}'
        matriz_pesados_exaustivas[coluna_emissao] = (
            matriz_pesados_exaustivas['PROB_ANO_MODELO'] *
            matriz_pesados_exaustivas['PROB_SUBCATEGORIA'] *
            matriz_pesados_exaustivas['CONSUMO DIESEL CATEGORIA'] *
            matriz_pesados_exaustivas['AUTONOMIA'] *
            matriz_pesados_exaustivas[poluente]
        )
        
    #Calculo do CO2
    fator_emissao_pesados_exaustivas_CO2 = fator_emissao_pesados_exaustivas_CO2.rename(
    columns={'CO2 (KG/L)': 'CO2'})
    
    # Kg/L -> g/L
    fator_emissao_pesados_exaustivas_CO2['CO2'] = (fator_emissao_pesados_exaustivas_CO2['CO2']*1000)
        
    matriz_pesados_exaustivas = matriz_pesados_exaustivas.merge(
        fator_emissao_pesados_exaustivas_CO2,
        how="left",
        on=["ANO MODELO" , "CATEGORIA"],
    )

    matriz_pesados_exaustivas['EMISSAO CO2'] = (
       matriz_pesados_exaustivas['PROB_ANO_MODELO'] *
       matriz_pesados_exaustivas['PROB_SUBCATEGORIA'] *
       matriz_pesados_exaustivas['CONSUMO DIESEL CATEGORIA'] *
       matriz_pesados_exaustivas['CO2']
   )
   
    return matriz_pesados_exaustivas

#%%

def calculo_SO2(matriz_exaustiva,caminho_diretorio, categoria):
    
    # Calculo para a categoria LEVES
    if categoria == 'LEVES':
        fator_emissao = carregar_fator_emissao(caminho_diretorio,"EF_LightDuty_SO2.xlsx")
        fator_emissao['CODIGO COMBUSTIVEL'] = fator_emissao['COMBUSTIVEL'].map(codigos_combustivel_mai)

        matriz_exaustiva = matriz_exaustiva.merge(
            fator_emissao[["ANO MODELO", "CODIGO COMBUSTIVEL", "SO2"]],
            how="left",
            on=["ANO MODELO", "CODIGO COMBUSTIVEL"]
        )
        matriz_exaustiva['EMISSAO SO2'] = (
            matriz_exaustiva['PROBABILIDADE ANO MODELO'] *
            matriz_exaustiva['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_exaustiva['CONSUMO UTILIZADO'] *
            matriz_exaustiva['SO2']
        ).fillna(0)
        
    elif categoria == 'COMERCIAL LEVES':
    
        # Calculo para a categoria COMERCIAL LEVES
        fator_emissao = carregar_fator_emissao(caminho_diretorio,"EF_LightCommercial_SO2.xlsx")
        fator_emissao['CODIGO COMBUSTIVEL'] = fator_emissao['COMBUSTIVEL'].map(codigos_combustivel_mai)

        matriz_exaustiva = matriz_exaustiva.merge(
            fator_emissao[["ANO MODELO", "CODIGO COMBUSTIVEL", "SO2"]],
            how="left",
            on=["ANO MODELO", "CODIGO COMBUSTIVEL"]
        )
        matriz_exaustiva['EMISSAO SO2'] = (
            matriz_exaustiva['PROBABILIDADE ANO MODELO'] *
            matriz_exaustiva['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_exaustiva['CONSUMO UTILIZADO'] *
            matriz_exaustiva['SO2']
        ).fillna(0)
    
    elif categoria == 'MOTOS':
        
        # Calculo para a categoria MOTOS
        fator_emissao = carregar_fator_emissao(caminho_diretorio,"EF_MotorCycle_SO2.xlsx")
        fator_emissao['CODIGO COMBUSTIVEL'] = fator_emissao['COMBUSTIVEL'].map(codigos_combustivel_mai)

        matriz_exaustiva = matriz_exaustiva.merge(
            fator_emissao[["ANO MODELO", "CODIGO COMBUSTIVEL", "CATEGORIA", "SO2"]],
            how="left",
            on=["ANO MODELO", "CODIGO COMBUSTIVEL", "CATEGORIA"]
        )
        matriz_exaustiva['EMISSAO SO2'] = (
            matriz_exaustiva['PROBABILIDADE ANO MODELO MOTOS'] *
            matriz_exaustiva['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_exaustiva['PROBABILIDADE_MOTORIZACAO'] *
            matriz_exaustiva['CONSUMO UTILIZADO'] *
            matriz_exaustiva['SO2']
        ).fillna(0)

    # Calculo para a categoria PESADOS
    elif categoria == 'PESADOS':
        
        fator_emissao = carregar_fator_emissao(caminho_diretorio,"EF_HeavyDuty_SO2.xlsx")
        fator_emissao['CODIGO COMBUSTIVEL'] = fator_emissao['COMBUSTIVEL'].map(codigos_combustivel_mai)

        
        matriz_exaustiva = matriz_exaustiva.merge(
            fator_emissao[["ANO MODELO", "CODIGO COMBUSTIVEL", "CATEGORIA", "SO2"]],
            how="left",
            on=["ANO MODELO", "CODIGO COMBUSTIVEL", "CATEGORIA"]
        )
        matriz_exaustiva['EMISSAO SO2'] = (
            matriz_exaustiva['PROB_ANO_MODELO'] *
            matriz_exaustiva['PROB_SUBCATEGORIA'] *
            matriz_exaustiva['CONSUMO DIESEL CATEGORIA'] *
            matriz_exaustiva['SO2']
        ).fillna(0)


    return matriz_exaustiva

# matriz_pesados_exaustivas.loc[(matriz_pesados_exaustivas['ANO'] == 2013), 'SO2'] = 0.8

# # Exemplo: ano 2013, ano modelo 2012, diesel
# matriz_pesados_exaustivas.loc[(matriz_pesados_exaustivas['ANO'] == 2013) & (matriz_pesados_exaustivas['ANO MODELO'] == 2012), 'SO2'] = 0.01

# # Exemplo: ano 2013, ano modelo 2013, diesel
# matriz_pesados_exaustivas.loc[(matriz_pesados_exaustivas['ANO'] == 2013) & (matriz_pesados_exaustivas['ANO MODELO'] == 2013), 'SO2'] = 0.01

# # Exemplo: ano 2013, ano modelo 2014, diesel
# matriz_pesados_exaustivas.loc[(matriz_pesados_exaustivas['ANO'] == 2013) & (matriz_pesados_exaustivas['ANO MODELO'] == 2014), 'SO2'] = 0.01

def calculo_emissoes_freios_pneus_estradas(resultados_categoria, matriz_categoria, categoria):
    
    # resultados_categoria = resultados_comleves_exaustivas
    # matriz_categoria = matriz_comleves_exaustivas
    # categoria='COMERCIAL LEVES'

    if categoria in ('LEVES', 'COMERCIAL LEVES'):
        
        # Emissoes freios e penus
        matriz_categoria['EMISSOES PNEUS FREIOS'] = (
            matriz_categoria['AUTONOMIA']*
            matriz_categoria['PROBABILIDADE ANO MODELO'] *
            matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_categoria['CONSUMO UTILIZADO'] *
            0.0138
            )
       
        # Emissoes estradas
        matriz_categoria['EMISSOES ESTRADAS'] = (
            matriz_categoria['AUTONOMIA']*
            matriz_categoria['PROBABILIDADE ANO MODELO'] *
            matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_categoria['CONSUMO UTILIZADO'] *
            0.0075
            )
        
        # Emissoes RESSUSPENSÃO
        matriz_categoria['EMISSOES RESSUSPENSAO'] = (
            matriz_categoria['AUTONOMIA']*
            matriz_categoria['PROBABILIDADE ANO MODELO'] *
            matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
            matriz_categoria['CONSUMO UTILIZADO'] *
            0.062
            )

    if categoria in ('MOTOS'):
       
        # Emissoes freios e penus
         matriz_categoria['EMISSOES PNEUS FREIOS'] = (
             matriz_categoria['AUTONOMIA']*
             matriz_categoria['PROBABILIDADE ANO MODELO'] *
             matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
             matriz_categoria['PROBABILIDADE_MOTORIZACAO'] *
             matriz_categoria['CONSUMO UTILIZADO'] *
             0.0064
             )
         
         # Emissoes estradas
         matriz_categoria['EMISSOES ESTRADAS'] = (
             matriz_categoria['AUTONOMIA']*
             matriz_categoria['PROBABILIDADE ANO MODELO'] *
             matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
             matriz_categoria['PROBABILIDADE_MOTORIZACAO'] *
             matriz_categoria['CONSUMO UTILIZADO'] *
             0.0030
             )
         
         matriz_categoria['EMISSOES RESSUSPENSAO'] = (
             matriz_categoria['AUTONOMIA']*
             matriz_categoria['PROBABILIDADE ANO MODELO'] *
             matriz_categoria['PROBABILIDADE USO COMBUSTIVEL'] *
             matriz_categoria['PROBABILIDADE_MOTORIZACAO'] *
             matriz_categoria['CONSUMO UTILIZADO'] *
             0.062
             )
    if categoria in ('PESADOS',):

        # Categoria LEVES e SEMILEVES
        matriz_categoria.loc[
            matriz_categoria['CATEGORIA'].isin(['Semileves', 'Leves'] ),
            'EMISSOES PNEUS FREIOS'] = (
            matriz_categoria['AUTONOMIA'] *
            matriz_categoria['PROB_ANO_MODELO'] *
            matriz_categoria['PROB_SUBCATEGORIA'] *
            matriz_categoria['CONSUMO DIESEL CATEGORIA'] *
            0.0216
        )
    
        matriz_categoria.loc[
            matriz_categoria['CATEGORIA'].isin(['Semileves', 'Leves']),
            'EMISSOES ESTRADAS'] = (
            matriz_categoria['AUTONOMIA'] *
            matriz_categoria['PROB_ANO_MODELO'] *
            matriz_categoria['PROB_SUBCATEGORIA'] *
            matriz_categoria['CONSUMO DIESEL CATEGORIA'] *
            0.0075
        )
    
        # Categoria restantes
        matriz_categoria.loc[
            matriz_categoria['CATEGORIA'].isin(['Médios', 'Semipesados', 'Pesados', 
                                                'Urbanos', 'Rodoviários', 'MicroOnibus']),
            'EMISSOES PNEUS FREIOS'] = (
            matriz_categoria['AUTONOMIA'] *
            matriz_categoria['PROB_ANO_MODELO'] *
            matriz_categoria['PROB_SUBCATEGORIA'] *
            matriz_categoria['CONSUMO DIESEL CATEGORIA'] *
            0.59
        )
    
        matriz_categoria.loc[
            matriz_categoria['CATEGORIA'].isin(['Médios', 'Semipesados', 'Pesados',
                                                'Urbanos', 'Rodoviários', 'MicroOnibus']),
            'EMISSOES ESTRADAS'] = (
            matriz_categoria['AUTONOMIA'] *
            matriz_categoria['PROB_ANO_MODELO'] *
            matriz_categoria['PROB_SUBCATEGORIA'] *
            matriz_categoria['CONSUMO DIESEL CATEGORIA'] *
            0.0380
        )
                
        # Emissao ressuspensão
        matriz_categoria['EMISSOES RESSUSPENSAO'] = (
             matriz_categoria['AUTONOMIA'] *
             matriz_categoria['PROB_ANO_MODELO'] *
             matriz_categoria['PROB_SUBCATEGORIA'] *
             matriz_categoria['CONSUMO DIESEL CATEGORIA'] *
             0.062
         )

         
    emissoes = (matriz_categoria.groupby(['ANO','MES','UF',
                                               'MUNICIPIO','CODIGO IBGE'])
                                     [['EMISSOES PNEUS FREIOS', 'EMISSOES ESTRADAS',
                                       'EMISSOES RESSUSPENSAO']]
                                     .sum()
                                     .reset_index())
    
    resultados_categoria = resultados_categoria.merge(emissoes,
        on=['ANO','MES','UF','MUNICIPIO','CODIGO IBGE']
        )

    return resultados_categoria, matriz_categoria


def calculo_emissoes_reabastecimento(consumo_gasolina, consumo_etanol, categoria, resultados_matriz):

    # calcular emissões por combustível
    consumo_gasolina[f'EMISSAO NMHC GASOLINA REAB'] = consumo_gasolina[categoria] * 1.14
    consumo_etanol[f'EMISSAO NMHC ETANOL REAB'] = consumo_etanol[categoria] *  0.37

    # juntar e somar
    emiss_NMHC = consumo_gasolina[['ANO','MES','UF','CODIGO IBGE','EMISSAO NMHC GASOLINA REAB']].merge(
        consumo_etanol[['ANO','MES','UF','CODIGO IBGE','EMISSAO NMHC ETANOL REAB']],
        how="outer",
        on=['ANO','MES','UF','CODIGO IBGE']
    ).fillna(0)

    # Calculo total das emissoes
    emiss_NMHC['EMISSAO NMHC REAB'] = (
        emiss_NMHC['EMISSAO NMHC GASOLINA REAB'] +
        emiss_NMHC['EMISSAO NMHC ETANOL REAB']
    )

    # adicionar no dataframe de resultados
    resultados_matriz = resultados_matriz.merge(
        emiss_NMHC[['ANO','MES','UF','CODIGO IBGE','EMISSAO NMHC REAB']],
        on=['ANO','MES','UF','CODIGO IBGE'],
        how='left'
    )

    return resultados_matriz

#%%

def consolidacao_evaporativas(matriz_completa, resultados_matriz):

    for coluna in ['EMISSAO DIURNAL', 'EMISSAO HOT SOAK', 'EMISSAO RUNNING LOSSES']:             # loop sobre cada emissão
        matriz_consolidado = matriz_completa.groupby(
            ['CODIGO IBGE', 'MES'])[coluna].sum().reset_index()
        
        resultados_matriz = resultados_matriz.merge(
            matriz_consolidado,
            on=['MES','CODIGO IBGE'],
            how='left'
        )

    return resultados_matriz

