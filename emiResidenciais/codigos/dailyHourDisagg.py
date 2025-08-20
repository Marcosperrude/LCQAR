# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 14:54:09 2025

@author: Marcos.Perrude
"""

import numpy as np
from calendar import monthrange
import pandas as pd
import xarray as xr
import netCDF4 as nc
import pyproj
import matplotlib.pyplot as plt
import os


des = pd.read_csv(r"C:\Users\marcos perrude\Documents\LCQAR\Inputs\hourDes.csv", encoding='latin1')


def daylyProfile(Tb,T2,year,month):
    year = 2023
    month = 1
    T2 = listTemp2
    # T2daily = np.nanmean(T2,axis=0)
    T2daily = T2.copy()
    # T2 = listTemp2
    HDD = Tb - T2daily
    HDD[HDD<1] = 1
    
    #nDays=1
    nDays = monthrange(year, month)[1]
    
    hdd = np.nansum(HDD,axis=0)/nDays
    #hdd = HDD.copy()
    
    FD = (HDD + 0.2*hdd)/((1+0.2)*hdd)/nDays
    
    return FD

def hourlyProfile(FD,des,FH):

    for ii in range(FD.shape[0]):
        for kk in range(FH.shape[1]):
            FH[ii, kk,:,:] = FD[ii, :, :] * (des['fator'].iloc[kk]/24)
    
    return FH

            
# T2 = (40  + 10)*np.random.rand(31,100,100) - 10
# Tb = 15
# FD = daylyProfile(Tb,T2,2020,1)


# FH = np.zeros((FD.shape[0], len(des), FD.shape[1], FD.shape[2]))
# FH = hourlyProfile(FD, des, FH)

# print(FD[1,1,1], FH[1,:,1,1].sum())



#%%


dataPathMetCrod = os.path.join(DataPath, 'METCRO2D')
arquivos = sorted([f for f in os.listdir(dataPathMetCrod) if f.endswith('.nc')])
lista = []

for arquivo in arquivos:
    
    dirArquivo = os.path.join(dataPathMetCrod, arquivo)
    ds = nc.Dataset(dirArquivo)
    temp2 = ds.variables['TEMP2'][:]
    temp2 = np.squeeze(temp2)
    temp2mean = np.nanmean(temp2, axis=0) 
    temp2mean = temp2mean - 273.15
    lista.append(temp2mean)

listTemp2 = np.stack(lista, axis=0)

Tb = 15

FD = daylyProfile(Tb,listTemp2,2023,1)

FH = np.zeros((FD.shape[0], len(des), FD.shape[1], FD.shape[2]))
FH = hourlyProfile(FD, des, FH)

# plt.plot(FH[0, :, 200, 200])
# print(FD[1,1,1], FH[1,:,1,1].sum())

#%%

dsMETCRO3D = nc.Dataset(r"C:\Users\marcos perrude\Documents\LCQAR\Inputs\METCRO2D_BR_20km_2023-01-22.nc")
#TEMP2 = dsMETCRO3D.variables['TEMP2']


def ioapiCoords(ds):
    # Latlon
    lonI = ds.XORIG
    latI = ds.YORIG
    print('lonI = '+str(lonI))
    print('latI = '+str(latI))
    
    # Cell spacing 
    xcell = ds.XCELL
    ycell = ds.YCELL
    ncols = ds.NCOLS
    nrows = ds.NROWS
    
    lon = np.arange(lonI,(lonI+ncols*xcell),xcell)
    lat = np.arange(latI,(latI+nrows*ycell),ycell)
    
    xv, yv = np.meshgrid(lon,lat)
    return xv,yv,lon,lat

def eqmerc2latlonMETCROD(ds,xv,yv):

    mapstr = '+proj=merc +a=%s +b=%s +lat_ts=0 +lon_0=%s' % (
              6370000, 6370000, ds.XCENT)
    #p = pyproj.Proj("+proj=merc +lon_0="+str(ds.P_GAM)+" +k=1 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")
    p = pyproj.Proj(mapstr)
    xlon, ylat = p(xv-ds.XCELL/2, yv-ds.YCELL/2, inverse=True)

    return xlon,ylat


xv,yv,lonMETCROD,latMETCROD = ioapiCoords(dsMETCRO3D)
lonMCIPMETCRO,latMCIPMETCROD = eqmerc2latlonMETCROD(dsMETCRO3D,xv,yv)

TEMP2 = dsMETCRO3D.variables['TEMP2'][:]  # Shape: (TSTEP, LAY, ROW, COL)
TEMP2 = np.squeeze(TEMP2)
meanTEMP2 = np.nanmean(TEMP2, axis=0)
meanTEMP2 = meanTEMP2 - 273.15



plt.figure(figsize=(10, 8))
plt.pcolormesh(lonMCIPMETCRO, latMCIPMETCROD, meanTEMP2, shading='auto', cmap='RdYlBu_r')
plt.colorbar(label='Temperatura Média (K)')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Temperatura Média TEMP2')
plt.show()











