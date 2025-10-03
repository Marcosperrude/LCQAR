#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exemplo de aplicação de Análise de Tráfego

Autoria: Igor Tibúrcio
Orientação: Leonardo Hoinaski

"""

# Introdução e Inicialização do módulo
#Para fazer a análise de trânsito usando a arquitetura do LCQAr, precisaremos usar dois pacotes exclusivos:

#TrafficServer: pacote para facilitar a obtenção e tratamento dos dados de tráfego e referência;
#TrafficData: pacote obtido a partir da função get_traffic_dataset
#%%
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
mask = gpd.read_file(dataPath + '/inputs/GrandeFlorianopolis.gpkg')
# mask['geometry'] = mask.unary_union.envelope

#%%



# Se quisermos, podemos avaliar os dados disponíveis por data e hora em UTM.
# List available datetime (UTM)
# ts.list_available_dates()

#%%

# O pacote de análise pega automaticamente somente os dados existentes.
    
# Agora, iremos escolher as datas e horas que gostaríamos de analisar, em UTM.

# Defining dates
start_date = datetime(2025,5,28,0)
end_date = datetime(2025,6,23,23)

print(f'Tempo a ser calculado: {end_date - start_date}')
#%%



# start_monitor(interval=2, output="monitoramento.csv")


for uf in mask['NM_MUN'].unique():
    # uf = 'Florianópolis'    
    print(uf)
    # Cria o caminho da pasta
    os.makedirs(os.path.join(dataPath, 'outputs', str(uf)), exist_ok=True)
    mask_cid = mask[mask['NM_MUN'] == f'{uf}'] 
    
    # Usando as datas, a camada de máscara e nosso objeto do servidor, vamos carregar os dados de tráfego (objeto TrafficData).
    # Creating TrafficData object
    td = get_traffic_dataset(
        mask_cid,
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
    
    
    filename = (dataPath +f'/outputs/{uf}/prelim_results_{uf}_2025-07-01_to_2025-07-01-23_rev2.parquet')
    
    # Saving checkpoint
    if not Path(filename).exists():
        td.to_parquet(filename)
    
    # Loading checkpoint
    if Path(filename).exists():
        from trafficdata import read_parquet
    
    filename = dataPath +f'/outputs/{uf}/prelim_results_{uf}_2025-07-01_to_2025-07-01-23_rev2.parquet'
    td = pd.read_parquet(
            filename)


    # Obtendo dados agrupados por dia, numa média horária
    daily_group = td.count_vehicles(
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
    
    # Corrigindo datetime
    vehicle_count_df_weekly['datetime'] = vehicle_count_df_weekly['datetime'] - pd.Timedelta(hours=3)
    vehicle_count_df_weekly
    
    # Multiplicando pelas 24 horas para obter o total diário
    # daily_group['average_daily_vehicle_count'] = daily_group['vehicle_count'] * 24
    vehicle_count_df_weekly['road_length'] = vehicle_count_df_weekly.osm_id.apply(lambda x: td.get_road_length(x).slice_length)  # td.get_road_length(road_id)
    vehicle_count_df_weekly['vkt_per_hour'] = (
        vehicle_count_df_weekly['vehicle_count'] * vehicle_count_df_weekly['road_length']
    )
    
    # Adding metadata information
    vehicle_count_df_weekly.attrs = {
        'vehicle_count': 'Average hourly vehicle count [vehicles / hour]',
        'average_daily_vehicle_count': 'Average daily vehicle count [vehicles / day]',
        'road_length': 'Road length [meters]',
        'vkt_per_hour': 'Vehicle Kilometers Traveled per hour [vehicles * km / hour]',
        'surface': "Road surface type ['asphalt', 'paving_stones', 'compacted', None, 'unpaved', 'sett', "
           "'paved', 'cobblestone', 'metal', 'ground', 'gravel', 'dirt',"
           "'concrete:plates']",
        'avg_traffic_level': 'Average traffic level [km / hour]'
    }
    
    vehicle_count_df_weekly = pd.DataFrame(
        vehicle_count_df_weekly,
        # geometry='geometry',
        # crs=daily_group_geom_crs
    )
    
    # Agrega por dia da semana
    vehicle_count_df_weekly_final = get_aggregate_weekday_hourly_vehicle_count(
        vehicle_count_df_weekly,
        maxmin=True
    )
    
    vehicle_count_df_weekly_final.to_csv(dataPath + f"/outputs/{uf}/vehicle_count_df_weekly_final.csv", index=False)
    



