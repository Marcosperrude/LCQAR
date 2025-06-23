# -*- coding: utf-8 -*-
"""
Created on Mon May 26 10:53:27 2025

@author: Marcos.Perrude
"""

import xarray as xr
import pandas as pd 


def temporalDisagg(gridMat5D, poluentes, Combustivel, xx, yy):
    # Reshape para 4D: (poluentes, tempo, lat, lon)
   
    gridMat4Dtemp = gridMat5D.reshape(
       gridMat5D.shape[0], 
       gridMat5D.shape[1] * gridMat5D.shape[2], 
       gridMat5D.shape[3],
       gridMat5D.shape[4]
   )

    data_vars = {}

    fim = pd.Timestamp('2023-12-01')
    # Começa no primeiro ano contando para trás
    inicio = fim.year - (gridMat5D.shape[1] - 1)
    inicio = pd.Timestamp(year= inicio, month=1, day=1)
    
    for i, nome in enumerate(poluentes):
        data_vars[nome] = xr.DataArray(
            gridMat4Dtemp[i,:, :, :],  # shape: (time, lat, lon)Add commentMore actions
            dims=["time", "lat", "lon"],
            coords={
                "time": pd.date_range(
                    start= inicio,
                    periods= gridMat4Dtemp.shape[1],
                   freq="MS"
               ),
                "lat": yy[:, 0],
                "lon": xx[0, :]
            }
        )
        
       # Criar o Dataset
    ds = xr.Dataset(data_vars)
    ds.attrs['description'] = f"Emissões residenciais de {Combustivel}"

    return ds 
        
        
        