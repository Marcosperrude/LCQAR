# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 17:20:25 2025

@author: Marcos.Perrude
"""
pm = emiCoal['PM']   
def disaggregate_monthly_to_daily(dataXr, dayDisagg):
    import pandas as pd
    import xarray as xr
    import numpy as np



    # Seleciona lat/lon do domínio das emissões
    lat_sel = dataXr.lat.values
    lon_sel = dataXr.lon.values

    # Recorta dayDisagg para o mesmo domínio
    dayDisagg_sel = dayDisagg.sel(
        lat=slice(lat_sel.min(), lat_sel.max()),
        lon=slice(lon_sel.min(), lon_sel.max())
    )

    # Cria grade base vazia
    lat_vals = dayDisagg_sel.lat.values
    lon_vals = dayDisagg_sel.lon.values

    # Lista de resultados diários
    daily_grids = []
    time_coords = []

    for timestamp in dataXr.time.values:
        ts = pd.Timestamp(timestamp)
        year = ts.year
        month = ts.month

        # Seleciona emissões mensais
        monthly_values = dataXr.sel(time=timestamp)

        # Seleciona fatores diários normalizados
        daily_factors = dayDisagg_sel.sel(month=month)

        # Itera sobre os dias do mês
        for day in daily_factors.day.values:
            factor_day = daily_factors.sel(day=day)

            # Multiplica pixel a pixel: (lat, lon)
            grid_day = monthly_values * factor_day

            # Armazena como novo dia
            daily_grids.append(grid_day)
            time_coords.append(pd.Timestamp(year=year, month=month, day=day))

    # Empilha todos os grids como um único DataArray com tempo
    daily_emi = xr.concat(daily_grids, dim='time')
    daily_emi = daily_emi.assign_coords(time=time_coords)

    daily_emi.sortby('time')
