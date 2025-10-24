# -*- coding: utf-8 -*-
"""
Created on Thu Mar  6 11:00:27 2025

@author: marcos perrude
"""

import pandas as pd


def emissionEstimateGLP(DataPath, OutPath, glpDf ):

    # Somar as colunas p12 e outros
    glpDf['TOTAL (Kg)'] = glpDf['P13'] + glpDf['OUTROS']
    
    # Converter para m³
    # Densidade 552 kg/m³ (tabela 17 - https://static.portaldaindustria.com.br/media/filer_public/ee/78/ee78f794-84fc-4b8b-8aec-5ce5be3c74f0/estudo_especificacao_do_gas_natural_new.pdf)
    
    glpDf['TOTAL (m³)'] = glpDf['TOTAL (Kg)']/(552)
    # glpDf_joinville = glpDf[glpDf['MUNICIPIO'] == 'JOINVILLE']

    #https://www.epa.gov/sites/default/files/2020-09/documents/1.5_liquefied_petroleum_gas_combustion.pdf
    emiFac  = pd.read_csv(DataPath + '/fatorEmissao_Prop_But.csv' , index_col=[0])
    
    # Para Enxofre
    # Teor max de enxofre no glp em mg S/kg de acordo com https://www.sindigas.org.br/Download/PUBLICACOES_SINDIGAS/glp-energetico-de-transicao-estudo-fernando-corner.pdf?utm_source=chatgpt.com
    # COnverter a concentração de enxofre de mg/kg para gr/ft³
    MAxGLPN = (140 * 2.3)/(1000*0.353147*0.0647989)
    
    emiFac['SO2'] =  emiFac['SO2']*MAxGLPN
    #Conversao lb/10³gal --> Kg/m³ -->Ton/m³
    emiFac  = (emiFac * 0.12)/1000
    

    
    emiCidDict = {}
    # Loop pelos combustíveis disponíveis em emiFac (assume que o índice são os nomes)
    for nome in emiFac.index:
        # nome= 'Propano'
        emiCid = glpDf[['CODIGO IBGE', 'ANO', 'UF', 'MUNICIPIO']].copy()
    
        for pol in emiFac.columns:
            # pol = 'PM'
            emiCid[pol] = emiFac.loc[nome, pol] * (glpDf['TOTAL (m³)'] / 2)
    
        emiCidDict[nome] = emiCid.copy()  # Armazena o resultado com a chave do nome do combustível
    
    # Acessando os resultados
    propEmiCid = emiCidDict['Propano']
    butEmiCid  = emiCidDict['Butano']
    
    poluentesGLP = emiFac.columns
    
    return propEmiCid, butEmiCid, poluentesGLP