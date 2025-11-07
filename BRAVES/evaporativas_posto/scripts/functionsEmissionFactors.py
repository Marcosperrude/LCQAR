#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  9 12:38:17 2025

Módulo para cálculo de fatores de emissão e curva RVP (Reid Vapor Pressure)
utilizados na quantificação de emissões evaporativas de Compostos Orgânicos
Voláteis (COVs) em postos de combustíveis.

Desenvolvido com base em estudos da EPA 

Autor: Marcos Perrude  
Data: 09 de outubro de 2025
"""
from scipy.optimize import curve_fit

# Função polinomial de 3ª ordem usada para ajustar a curva RVP em função da 
# porcentagem de etanol no combustível.
def func(x, a, b, c, d):
    return a*x**3 + b*x**2 + c*x + d

# Calculo d o  fator de emissoes de reabastecimento
def carRefuelingEF(tamb_list, ethanolPercentage , rvpCurve):
    # ethanolPercentage = 27
    popt, _ = curve_fit(func, rvpCurve['ETHANOL'], rvpCurve['RVP'])
    EF_list = []
    for tamb in tamb_list:

        # Converter temperatura de celsius para Fahrenheit
        tConv = tamb * (9/5) + 32

        # Extrai RVP para a % de etanol do combsutivel
        rvpVal = func(ethanolPercentage, *popt)

        # Calculo da temperatura de combustivel que sai da bomba (California study)
        # Fonte: https://www.epa.gov/sites/default/files/2020-11/documents/420r20012.pdf
        td = 20.30 + 0.81 * tConv

        # Diferença de temperatura entre o tanque e o dispenser
        deltaT = 0.418 * td - 16.6

        # Conversão automatica para mg/L (EPA)
        EF = 264.2 * (-5.909 - 0.0949*deltaT + 0.084*td + 0.485*rvpVal)

        EF_list.append(EF)
    return EF_list

# Calculo do RVP em função da porcentagem de etanol do combustivel
def rvp(ethanolPercentage, gasolineEmissionServiceEF ,rvpCurve):

    # Extrai a pressão de vapor da curva em função da temperatura
    popt, _ = curve_fit(func, rvpCurve['ETHANOL'], rvpCurve['RVP'])
    rvp_val = func(ethanolPercentage, *popt)

    # Pressão de vaapor adotada nos EUA (=~10%)
    rvpUsaGasoline = 9.965801227
    return gasolineEmissionServiceEF * (rvp_val / rvpUsaGasoline)

