#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  8 17:05:18 2025

@author: marcosperrude
"""


#%%
# Importings
# Importings
import geopandas as gpd
from datetime import datetime
from trafficdata import TrafficServer
from trafficdata.core.package import get_traffic_dataset
from pathlib import Path
# from monitor import start_monitor
from pandas import Grouper
import pandas as pd
from trafficdata.core.trafficmeasurement import replace_min_plateau_using_gauss
from trafficdata.core.trafficmeasurement import get_aggregate_weekday_hourly_vehicle_count
import os

#%%
# Data server paths
traffic_server_url = 'http://150.162.52.2:6140'              # Tomtom
reference_server_url = 'http://150.162.52.2:6150/2025_5_12'  # Geofabrik
dataPath = "/home/marcosperrude/Documents/LCQAR/BRAVES/Traffic"

#%% Conecção com o servidor

# Loading traffic server
ts = TrafficServer(
    traffic_server_url,
    reference_server_url
)

_ = ts.sync()


#%%

#Vamos também precisar de uma camada de máscara para facilitar nossa análise de exemplo.
#Neste exemplo, vamos pegar uma pequena área que engloba estradas entre Florianópolis (SC) e continente.
# Importing data mask example
mask = gpd.read_file(dataPath + '/inputs/BR_Pais_2024/BR_Pais_2024.shp')

#%%

# O pacote de análise pega automaticamente somente os dados existentes.
    
# Agora, iremos escolher as datas e horas que gostaríamos de analisar, em UTM.

# Defining dates
start_date = datetime(2025,6,28,7)
end_date = datetime(2025,6,28,7)

print(f'Tempo a ser calculado: {end_date - start_date}')
#%%
td = get_traffic_dataset(
    mask,
    start_date,
    # datetime(2025,6,22,22),
    end_date,
    ts,
    n_processes=5,
    verbose=True,
    clip_by_mask=True,
    buffer_size=5,
    join_factor=0.25,
    use_hd=True
)

# Obtendo dados agrupados por dia, numa média horária
daily_group = td.count_vehicles(
    # road_id,
    grouper=Grouper(
        key='datetime',
        freq='1H'  # Daily frequency
    )
)

vehicle_count_df_weekly = replace_min_plateau_using_gauss(
    daily_group,
    sigma=1,
    set_plateau_to_min=True
    )

vehicle_count_df_weekly['road_length'] = vehicle_count_df_weekly.osm_id.apply(lambda x: td.get_road_length(x).slice_length)  # td.get_road_length(road_id)
vehicle_count_df_weekly['vkt_per_hour'] = (
    vehicle_count_df_weekly['vehicle_count'] * vehicle_count_df_weekly['road_length']
)

vehicle_count_df_weekly['surface'] = td.surface.iloc[:]

# vehicle_count_df_weekly['avg_traffic_level'] = 
td_traffic_level = td.get_values_aggregated(
    values_headers='traffic_level',
    grouper=pd.Grouper(key= 'datetime', freq='1H'),
    operation='mean'
).reset_index(drop=False)

td_traffic_level = td_traffic_level.rename(columns={'traffic_level': 'avg_traffic_level'})

vehicle_count_df_weekly = vehicle_count_df_weekly.merge(
    td_traffic_level,  
    on=['osm_id', 'datetime'],       
    how='left'                       
)

td_bruno = td.merge(
    vehicle_count_df_weekly,  
    on=['osm_id', 'datetime'],       
    how='left'                       
)

td_bruno['average_daily_vehicle_count'] = td_bruno['vehicle_count'].astype(float).sum(skipna=True)

td_bruno.to_parquet(
    './outputs/bruno_florianopolis_2025_7_1_0_23.parquet', 
    index=False
)
#%%





