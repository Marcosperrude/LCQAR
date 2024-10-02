# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 15:37:32 2024

@author: marcos perrude
"""
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
senso = pd.read_csv("C:/Users/marcos perrude/Documents/lcqar/Arquivos/censo2022_sidra_4714_PopResid_munic_22_ptSede.csv",encoding='latin-1')


br_shp = gpd.read_file("C:/Users/marcos perrude/Documents/lcqar/Arquivos/sidra_4714_PopResid_munic_22_ptSede/sidra_4714_PopResid_munic_22_ptSede.shp")

ax=plt.gca()
br_shp.plot(linewidth=.3,color='grey', ax=ax)
