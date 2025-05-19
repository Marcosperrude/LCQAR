# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 10:56:08 2025

Este script sera utilizado para o tratamento dos dados e quantifcação das emissoes atmosféricas de Fontes Residenciais,
do qual sera utilizado para integrar o inventário nacional de fontes fixas.


@author: Marcos.Perrude
"""

import pandas as pd

def emissionEstimateWoodCoal(WoodCoalDf,DataPath,OutPath):
    
    WoodCoalDf = WoodCoalDf[WoodCoalDf["CD_SIT"] != 9]  # Remove massas de água
    WoodCoalDf = WoodCoalDf[WoodCoalDf['v0002'] != 0]  # Remove áreas com 0 domicílios
    
    # Classificação de acordo com o tipo do setor censitário
    classificacao = pd.read_csv(DataPath + '\\classificacao.csv' , index_col='Codigo')

    #Classificando os setores
    WoodCoalDf["Classificacao"] = WoodCoalDf["CD_SIT"].map(classificacao['Descricao'])

    # Quantidade de residencias que utilizam  de acordo com o tipo de combustível e classificação do setor censitário
    fatores = pd.read_csv(DataPath + '\\fatores.csv', index_col = 'Categoria')
    
    # Mapear a quantidade de residencias que utilizam lenha
    WoodCoalDf["Fator_Lenha"] = WoodCoalDf["Classificacao"].map(fatores.loc["Lenha"].to_dict())
    WoodCoalDf["Fator_Carvao"] = WoodCoalDf["Classificacao"].map(fatores.loc["Carvão"].to_dict())
    WoodCoalDf["Residencias_Ajustadas_lenha"] = WoodCoalDf["v0002"] * WoodCoalDf["Fator_Lenha"]
    WoodCoalDf["Residencias_Ajustadas_carvao"] = WoodCoalDf["v0002"] * WoodCoalDf["Fator_Carvao"]

    #Consumo de lenha (ton/dia) por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
    consumo_regional = pd.read_csv(DataPath + '\\consumo_regional.csv')
    
    #Calculo do consumo anual de lenha e carvao de acordo com a quantidade de residenciais
    WoodCoalDf['Consumo_lenha[ton/ano]'] = WoodCoalDf['Residencias_Ajustadas_lenha'] * WoodCoalDf['NM_REGIAO'].map(
        consumo_regional.set_index('Regiao')['Lenha']) * 365
    WoodCoalDf['Consumo_Carvao[ton/ano]'] = WoodCoalDf['Residencias_Ajustadas_carvao'] * WoodCoalDf['NM_REGIAO'].map(
        consumo_regional.set_index('Regiao')['Lenha']) * 365
    
    
    # Fator de Emissao Fonte: https://www.epa.gov/system/files/documents/2022-03/c1s6_final_0.pd
    #lb/MMBtu
    emissionFactor = pd.read_csv(DataPath + '\\fatores_emissao.csv')
    
    #Uilizando o poder calorifico disponibilizado pelo documento da fonte de dados, estimar os fatores de emissao de acordo com o poder calorífico
    #poder calorífico (lenha == 8 Btu/lb (0.08 MMBtu/lb), carvao == 14 Btu/lb (0.14 MMBtu/lb))
    #lb/MMBtu
    
    # Conversao para kg/ton
    emissionFactor['Lenha'] = emissionFactor['Lenha'] * (0.08 * 2000 * 0.5)
    emissionFactor['Carvao'] = emissionFactor['Carvao'] * (0.14 * 2000 * 0.5)

    woodEmission = pd.DataFrame({'CD_SETOR': WoodCoalDf['CD_SETOR'].astype(str),
                                   'CD_UF' : WoodCoalDf['CD_UF']
                                   })
    
    coalEmission = pd.DataFrame({'CD_SETOR': WoodCoalDf['CD_SETOR'].astype(str),
                                    'CD_UF' : WoodCoalDf['CD_UF']
                                    })

    # emissões em toneladas de poluentes
    for i, pol in enumerate(emissionFactor['Poluentes']):
        woodEmission[pol] = (WoodCoalDf['Consumo_lenha[ton/ano]'] * emissionFactor['Lenha'][i]) / 1000
        coalEmission[pol] = (WoodCoalDf['Consumo_Carvao[ton/ano]'] * emissionFactor['Carvao'][i]) / 1000
    

    coalEmission.to_csv(OutPath + '\coalEmission.csv',index = False)
    woodEmission.to_csv(OutPath + '\woodEmission.csv',index = False)

    
    # Nome dos poluentes
    poluentesWoodCoal = emissionFactor['Poluentes']    
    return woodEmission, coalEmission, poluentesWoodCoal

