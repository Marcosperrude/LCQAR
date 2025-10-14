#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 13:46:26 2025

@author: marcos
"""
import dask.array as da
import xarray as xr
import pandas as pd
import calendar
import numpy as np
import os

def GridMat7D_Dask(weekdis, hourdis, gridMat5D, poluentes, OutPath, Combustivel, xx, yy):
    """
    Desagrega temporalmente usando Dask arrays, salvando direto em NetCDF.
    Nenhum array gigante é carregado na memória.
    """
    out_path = os.path.join(OutPath,'emissoes', f"{Combustivel}")


    n_pol = gridMat5D.shape[0]
    n_years = gridMat5D.shape[1]
    n_months = gridMat5D.shape[2]
    n_lat = gridMat5D.shape[3]
    n_lon = gridMat5D.shape[4]

    for iy in range(n_years):
        ano = 2021 + iy
        for im in range(n_months):
            mes = im + 1
            days_in_month = calendar.monthrange(ano, mes)[1]

            # Série horária do mês
            date_range = pd.date_range(f'{ano}-{mes:02d}-01',
                                       periods=days_in_month*24,
                                       freq='H', tz='America/Sao_Paulo')

            weekdays = np.array([d.weekday() for d in date_range])
            hours = np.array([d.hour for d in date_range])
            week_factors = np.array([weekdis.loc[w, 'weekdis'] for w in weekdays])
            hour_factors = np.array([hourdis.loc[h, 'hourdis'] for h in hours])
            combined = week_factors * hour_factors
            combined /= combined.sum()
            combined = combined.reshape(days_in_month, 24)

            # Converte gridMat5D do mês para Dask
            base_emis = da.from_array(gridMat5D[:, iy, im, :, :], chunks=(n_pol, n_lat, n_lon))

            # Cria array Dask 7D do mês (lazy)
            gridMat7D_month = da.zeros((n_pol, days_in_month, 24, n_lat, n_lon), chunks=(1, days_in_month, 24, n_lat, n_lon))
            for iday in range(days_in_month):
                for ihr in range(24):
                    gridMat7D_month[:, iday, ihr, :, :] = base_emis * combined[iday, ihr]

            # Converte para xarray usando Dask
            data_vars = {}
            for i, nome in enumerate(poluentes):
                data_vars[nome] = xr.DataArray(
                    gridMat7D_month[i, :, :, :, :].transpose(0,1,2),  # (time, lat, lon)
                    dims=["time", "lat", "lon"],
                    coords={
                        "time": date_range,
                        "lat": yy[:, 0],
                        "lon": xx[0, :]
                    }
                )

            ds_month = xr.Dataset(data_vars)
            ds_month.attrs['description'] = f"Emissões residenciais de {Combustivel}"

            # Salva direto em NetCDF
            ds_month.to_netcdf(out_path, mode='a' if os.path.exists(out_path) else 'w')

            print(f"Salvo {ano}-{mes:02d} -> {out_path}")

    print("✅ Finalizado com sucesso:", out_path)
    return xr.open_dataset(out_path)