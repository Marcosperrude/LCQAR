# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 10:56:08 2025

Este script sera utilizado para o tratamento dos dados e quantifcação das emissoes atmosféricas de Fontes Residenciais,
do qual sera utilizado para integrar o inventário nacional de fontes fixas.


@author: Marcos.Perrude
"""

import pandas as pd

def EmissionsEstimate(dff,DataPath,OutPath):
    dff = dff[dff["CD_SIT"] != 9]  # Remove massas de água
    dff = dff[dff['v0002'] != 0]  # Remove áreas com 0 domicílios
    
    # Classificação de acordo com o tipo do setor censitário
    classificacao = pd.read_csv(DataPath + '\\classificacao.csv' , index_col='Codigo')

    #Classificando os setores
    dff["Classificacao"] = dff["CD_SIT"].map(classificacao['Descricao'])

    # Quantidade de residencias que utilizam  de acordo com o tipo de combustível e classificação do setor censitário
    fatores = pd.read_csv(DataPath + '\\fatores.csv', index_col = 'Categoria')
    
    # Mapear a quantidade de residencias que utilizam lenha
    dff["Fator_Lenha"] = dff["Classificacao"].map(fatores.loc["Lenha"].to_dict())
    dff["Fator_Carvao"] = dff["Classificacao"].map(fatores.loc["Carvão"].to_dict())
    dff["Residencias_Ajustadas_lenha"] = dff["v0002"] * dff["Fator_Lenha"]
    dff["Residencias_Ajustadas_carvao"] = dff["v0002"] * dff["Fator_Carvao"]

    #Consumo de lenha (ton/dia) por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
    consumo_regional = pd.read_csv(DataPath + '\\consumo_regional.csv')
    
    #Calculo do consumo anual de lenha e carvao de acordo com a quantidade de residenciais
    dff['Consumo_lenha[ton/ano]'] = dff['Residencias_Ajustadas_lenha'] * dff['NM_REGIAO'].map(
        consumo_regional.set_index('Regiao')['Lenha']) * 365
    dff['Consumo_Carvao[ton/ano]'] = dff['Residencias_Ajustadas_carvao'] * dff['NM_REGIAO'].map(
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

    woodEmission = pd.DataFrame({'CD_SETOR': dff['CD_SETOR'].astype(str),
                                   'CD_UF' : dff['CD_UF']
                                   })
    
    coalEmission = pd.DataFrame({'CD_SETOR': dff['CD_SETOR'].astype(str),
                                    'CD_UF' : dff['CD_UF']
                                    })

    # emissões em toneladas de poluentes
    for i, pol in enumerate(emissionFactor['Poluentes']):
        woodEmission[pol] = (dff['Consumo_lenha[ton/ano]'] * emissionFactor['Lenha'][i]) / 1000
        coalEmission[pol] = (dff['Consumo_Carvao[ton/ano]'] * emissionFactor['Carvao'][i]) / 1000
    
    # Soma total das emissões por setor
    totalEmission = pd.concat([coalEmission, woodEmission])
    totalEmission = totalEmission.groupby('CD_SETOR').sum().reset_index()
    
    coalEmission.to_csv(OutPath + '\coalEmission.csv',index = False)
    woodEmission.to_csv(OutPath + '\woodEmission.csv',index = False)
    totalEmission.to_csv(OutPath + '\\totalEmission.csv',index = False)
    
    # Nome dos poluentes
    poluentes = totalEmission.columns[2:]
    
    return woodEmission, coalEmission, totalEmission, poluentes

