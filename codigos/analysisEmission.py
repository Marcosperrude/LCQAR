# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 10:56:08 2025

Este script sera utilizado para o tratamento dos dados e quantifcação das emissoes atmosféricas de Fontes Residenciais,
do qual sera utilizado para integrar o inventário nacional de fontes fixas.


@author: Marcos.Perrude
"""

import pandas as pd

def analysisEmission(dff):
    dff = dff[dff["CD_SIT"] != 9]  # Remove massas de água
    dff = dff[dff['v0002'] != 0]  # Remove áreas com 0 domicílios
    
    #Definir a classificação de acordo com o tipo do setor censitário
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

    #Definindo a quantidade de residencias que utilizam  de acordo com o tipo de combustível e classificação do setor censitário
    fatores = pd.DataFrame({
        "Categoria": ["Lenha", "Carvão"],
        "URBANA - Cidade ou vila, área urbanizada": [0.019, 0.008],
        "URBANA - Cidade ou vila, área não urbanizada": [0.051, 0.005],
        "URBANA - Área urbana isolada": [0.016, 0.0],
        "RURAL - Aglomerado rural, isolado, povoado": [0.219, 0.051],
        "RURAL - Aglomerado rural, isolado, outros aglomerados": [0.245, 0.028],
        "RURAL - Zona rural exclusive aglomerado rural": [0.407, 0.052]
    })
    fatores.set_index("Categoria", inplace=True)
    
    # Mapear a quantidade de residencias que utilizam lenha
    dff["Fator_Lenha"] = dff["Classificacao"].map(fatores.loc["Lenha"].to_dict())
    dff["Fator_Carvao"] = dff["Classificacao"].map(fatores.loc["Carvão"].to_dict())

    dff["Residencias_Ajustadas_lenha"] = dff["v0002"] * dff["Fator_Lenha"]
    dff["Residencias_Ajustadas_carvao"] = dff["v0002"] * dff["Fator_Carvao"]

    #Consumo de lenha (ton/dia) por regiao de acordo com https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/PublicacoesArquivos/publicacao-578/Nota%20T%C3%A9cnica%20Consumo%20de%20lenhaCV%20-%20Residencial%20final%202021.pdf
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

    #Calculo do consumo anual de lenha e carvao de acordo com a quantidade de residenciais
    dff['Consumo_lenha[ton/ano]'] = dff['Residencias_Ajustadas_lenha'] * dff['NM_REGIAO'].map(consumo_lenha) * 365
    dff['Consumo_Carvao[ton/ano]'] = dff['Residencias_Ajustadas_carvao'] * dff['NM_REGIAO'].map(consumo_carvao) * 365
    
    # Fator de Emissao Fonte: https://www.epa.gov/system/files/documents/2022-03/c1s6_final_0.pd
    #lb/MMBtu
    fator_emissao = pd.DataFrame({
        'Poluentes': ['PM', 'PM10', 'NOx', 'SO2', 'CO'],
        'Lenha': [0.40, 0.36, 0.49, 0.025, 0.60],
        'Carvao': [0.40, 0.36, 0.49, 0.025, 0.60]
    })
    
    #Uilizando o poder calorifico disponibilizado pelo documento da fonte de dados, estimar os fatores de emissao de acordo com o poder calorífico
    #poder calorífico (lenha == 8 Btu/lb (0.08 MMBtu/lb), carvao == 14 Btu/lb (0.14 MMBtu/lb))
    #lb/MMBtu
    
    # Conversao para kg/ton
    fator_emissao['Lenha'] = fator_emissao['Lenha'] * (0.08 * 2000 * 0.5)
    fator_emissao['Carvao'] = fator_emissao['Carvao'] * (0.14 * 2000 * 0.5)

    emissoes_lenha = pd.DataFrame({'CD_SETOR': dff['CD_SETOR'].astype(str)})
    emissoes_carvao = pd.DataFrame({'CD_SETOR': dff['CD_SETOR'].astype(str)})

    # emissões em toneladas de poluentes
    for i, pol in enumerate(fator_emissao['Poluentes']):
        emissoes_lenha[pol] = (dff['Consumo_lenha[ton/ano]'] * fator_emissao['Lenha'][i]) / 1000
        emissoes_carvao[pol] = (dff['Consumo_Carvao[ton/ano]'] * fator_emissao['Carvao'][i]) / 1000
    
    # Soma total das emissões por setor
    emissoes_total = pd.concat([emissoes_carvao, emissoes_lenha])
    emissoes_total = emissoes_total.groupby('CD_SETOR').sum().reset_index()
    
    return emissoes_lenha, emissoes_carvao, emissoes_total

