# -*- coding: utf-8 -*-
"""
Created on Thu Mar  6 11:00:27 2025

@author: marcos perrude
"""

import pandas as pd


def emissionEstimateGLP (DataPath , OutPath, glpDf ):

    #Agrupar colunas iguais caso tenha
    dfAgg = glpDf.groupby(['CODIGO IBGE', 'ANO']).agg({
        'GRANDE REGIAO': 'first',
        'UF': 'first',
        'PRODUTO': 'first',
        'MUNICIPIO': 'first',
        'P13': 'sum',
        'OUTROS': 'sum'
    }).reset_index()
    
    #Somar as colunas p12 e outros
    dfAgg['TOTAL (Kg)'] = dfAgg['P13'] + dfAgg['OUTROS']
    dfAgg['TOTAL (m³)'] = dfAgg['TOTAL (Kg)']/(2.3) #Transformar em m³
    
    #transformar de kg para m³ e dividir pela proporção de butano e propano (50%/50%)
    #https://www.epa.gov/sites/default/files/2020-09/documents/1.5_liquefied_petroleum_gas_combustion.pdf
    emiFac  = pd.read_csv(DataPath + '\\fatorEmissao_Prop_But.csv' , index_col=[0])
    
    #Conversao lb/ton --> Kg/m³ -->Ton/m³
    emiFac  = emiFac * 0.12/1000
    
    emiCidDict = {}
    # Loop pelos combustíveis disponíveis em emiFac (assume que o índice são os nomes)
    for nome in emiFac.index:
        emiCid = dfAgg[['CODIGO IBGE', 'ANO', 'UF', 'MUNICIPIO']].copy()
    
        for pol in emiFac.columns:
            emiCid[pol] = emiFac.loc[nome, pol] * (dfAgg['TOTAL (m³)'] / 2)
    
        emiCidDict[nome] = emiCid.copy()  # Armazena o resultado com a chave do nome do combustível
    
    # Acessando os resultados
    propEmiCid = emiCidDict['Propano']
    butEmiCid  = emiCidDict['Butano']
    
    poluentesGLP = emiFac.columns
    
    return propEmiCid, butEmiCid, poluentesGLP