# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 10:56:08 2025

Este script sera utilizado para o tratamento dos dados e quantifcação das emissoes atmosféricas de Fontes Residenciais,
do qual sera utilizado para integrar o inventário nacional de fontes fixas.


@author: Marcos.Perrude
"""

import pandas as pd

def emissionEstimateWoodCoal(WoodCoalDf,DataPath,OutPath):
    # Classificação de acordo com o tipo do setor censitário
    classificacao = pd.read_csv(DataPath + '/classificacao.csv' , index_col='Codigo')
    
    # Quantidade de residencias que utilizam  de acordo com o tipo de combustível e classificação do setor censitário
    fatores = pd.read_csv(DataPath + '/fatores.csv', index_col = 'Categoria')
    
    #Consumo de lenha (ton/dia) por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
    consumo_regional = pd.read_csv(DataPath + '/consumo_regional.csv')
    
    fatores_pnad = pd.read_csv(DataPath + '/fatores_pnad.csv', encoding = 'latin1')
    
    # Fator de Emissao Fonte: https://www.epa.gov/sites/default/files/2020-09/documents/1.10_residential_wood_stoves.pdf
    #lb/ton
    emissionFactor = pd.read_csv(DataPath + '/fatores_emissao_residencial.csv')
    
    # Verficar se cabe o uso
    # uso_lenha_agrupado = pd.read_csv(DataPath + '/uso_lenha_agrupado.csv', index_col = 0)

    # Remove massas de água
    WoodCoalDf = WoodCoalDf[(WoodCoalDf["CD_SIT"] != 9) & (WoodCoalDf['v0002'] != 0)]  
    

    #Classificando os setores
    WoodCoalDf["Classificacao"] = WoodCoalDf["CD_SIT"].map(classificacao['Descricao'])

    # # Mapear a quantidade de residencias que utilizam lenha (%)
    # WoodCoalDf["Fator_Lenha"] = WoodCoalDf["Classificacao"].map(fatores_pnad["Lenha"].to_dict())
    WoodCoalDf = WoodCoalDf.merge(
        fatores_pnad[["NM_UF", "Classificacao", "Fator"]],
        on=["NM_UF", "Classificacao"],
        how="left"
    )
    WoodCoalDf["Fator"] = WoodCoalDf["Fator"].fillna(
        WoodCoalDf["Classificacao"].map(fatores.loc["Lenha"].to_dict())
        )
    
    # Quantidade de residenciais que utilizam lenha/carvão

    WoodCoalDf["Residencias_Ajustadas"] = WoodCoalDf["v0002"] * WoodCoalDf["Fator"]

    #Calculo do consumo anual de lenha e carvao de acordo com a quantidade de residenciais
    WoodCoalDf['Consumo_lenha[ton/ano]'] = WoodCoalDf['Residencias_Ajustadas'] * WoodCoalDf['NM_REGIAO'].map(
        consumo_regional.set_index('Regiao')['Lenha']) * 365
    WoodCoalDf['Consumo_Carvao[ton/ano]'] = WoodCoalDf['Residencias_Ajustadas'] * WoodCoalDf['NM_REGIAO'].map(
        consumo_regional.set_index('Regiao')['Carvao']) * 365
    

    woodEmission = pd.DataFrame({'CD_SETOR': WoodCoalDf['CD_SETOR'].astype(str),
                                   'CD_UF' : WoodCoalDf['CD_UF']
                                   })
    
    coalEmission = pd.DataFrame({'CD_SETOR': WoodCoalDf['CD_SETOR'].astype(str),
                                    'CD_UF' : WoodCoalDf['CD_UF']
                                    })

     # emissões em toneladas de poluentes
    # Fator de conversão (0.5) de lb/ton --> kg/ton --> ton/ton
    for i, pol in enumerate(emissionFactor['Poluentes']):
        i  = 3
        woodEmission[pol] = ((WoodCoalDf['Consumo_lenha[ton/ano]'] * emissionFactor['Lenha'][i] * 0.5) / 1000)
        coalEmission[pol] = ((WoodCoalDf['Consumo_Carvao[ton/ano]'] * emissionFactor['Lenha'][i] * 0.5) / 1000)
    
    
    # Nome dos poluentes
    poluentesWoodCoal = emissionFactor['Poluentes']    
    return woodEmission, coalEmission, poluentesWoodCoal

