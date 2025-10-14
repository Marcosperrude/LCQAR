# -*- coding: utf-8 -*-
"""
Created on Mon May 26 10:53:27 2025

@author: Marcos.Perrude
"""

import xarray as xr
import pandas as pd
from tqdm import tqdm



def temporalDisagg(gridMat7D, poluentes, Combustivel, xx, yy):
    # Reshape para 4D: (poluentes, tempo, lat, lon)
   
    # gridMat5D = gridMat5Dglp
    # poluentes = poluentesGLP
    
    
    gridMat4Dtemp = gridMat7D.reshape(
       gridMat7D.shape[0],
       gridMat7D.shape[1] * gridMat7D.shape[2] * gridMat7D.shape[3] * gridMat7D.shape[4] ,
      gridMat7D.shape[5],gridMat7D.shape[6])

    data_vars = {}
    
    if Combustivel in ('Lenha', 'Carvao'):
        fim = pd.Timestamp('2023-12-31 23:00')
    else:
        fim = pd.Timestamp('2022-12-01')


    inicio = fim.year - (gridMat7D.shape[1] - 1)
    inicio = pd.Timestamp(year= inicio, month=1, day=1 , hour= 00)
    data_vars = {}
    for i, nome in enumerate(poluentes):
        # i=0
        # nome = 'PM'
        data_vars[nome] = xr.DataArray(
            gridMat4Dtemp[i,:, :, :],  # shape: (time, lat, lon)Add commentMore actions
            dims=["time", "lat", "lon"],
            coords={
                "time": pd.date_range(
                    start= inicio,
                    periods= gridMat4Dtemp.shape[1],
                   freq="H"
               ),
                "lat": yy[:, 0],
                "lon": xx[0, :]
            }
        )
       # Criar o Dataset
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {Combustivel}"

    return ds 
        
    
def temporalDisagg_mes(gridMat7D, poluentes, Combustivel, xx, yy):
    # Reshape para 4D: (poluentes, tempo, lat, lon)
   
    # gridMat5D = gridMat5Dglp
    # poluentes = poluentesGLP
    
    
    gridMat4Dtemp = gridMat7D.reshape(
       gridMat7D.shape[0],
       gridMat7D.shape[1] * gridMat7D.shape[2] * gridMat7D.shape[3] * gridMat7D.shape[4] ,
      gridMat7D.shape[5],gridMat7D.shape[6])

    data_vars = {}
    
    if Combustivel in ('Lenha', 'Carvao'):
        fim = pd.Timestamp('2023-12-31 23:00')
    else:
        fim = pd.Timestamp('2022-12-01')


    inicio = fim.year - (gridMat7D.shape[1] - 1)
    inicio = pd.Timestamp(year= inicio, month=1, day=1 , hour= 00)
    data_vars = {}
    for i, nome in enumerate(poluentes):
        # i=0
        # nome = 'PM'
        data_vars[nome] = xr.DataArray(
            gridMat4Dtemp[i,:, :, :],  # shape: (time, lat, lon)Add commentMore actions
            dims=["time", "lat", "lon"],
            coords={
                "time": pd.date_range(
                    start= inicio,
                    periods= gridMat4Dtemp.shape[1],
                   freq="H"
               ),
                "lat": yy[:, 0],
                "lon": xx[0, :]
            }
        )
       # Criar o Dataset
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {Combustivel}"

    return ds     
        