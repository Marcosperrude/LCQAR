# -*- coding: utf-8 -*-
"""
Created on Thu May  8 12:01:21 2025

@author: Marcos.Perrude
"""
import pandas as pd
from timezonefinder import TimezoneFinder
import numpy as np
import datetime

def cellTimeZone(xx,yy):
    test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    tf = TimezoneFinder(in_memory=True)
    ltc =[]
    for ii in range(0,xx.shape[0]):
        #Loop over each cel in y direction
        for jj in range(0,xx.shape[1]):
            local_time_zone = tf.timezone_at(lng=xx[ii,jj], lat=yy[ii,jj])
            ltc.append(float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1]))
            
    ltcGrid = np.reshape(ltc,
                         (np.shape(xx)[0],
                          np.shape(xx))[1])
    return ltcGrid


def temporalDisag(gridMat3D, xx, yy ,year,month,day,hourdis,weekdis,monthdis,ltcGrid):
    # Create the MultiIndex from pollutant and time.
    #year=2013
    print('Temporal disagregation')
    startDate = datetime.datetime(year, month, day, 0, 0)
    endDate = datetime.datetime(year, month, day+1, 0, 0)
    datePfct = np.arange(np.datetime64(startDate),np.datetime64(endDate),3600000000)
    numWeeks = datePfct.shape[0]/(7*24) # Number of weeks
    disvec = pd.DataFrame()
    disvec = disvec.reindex(datePfct, fill_value=np.nan)
    disvec['year'] = disvec.index.year
    disvec['month'] = disvec.index.month
    disvec['day'] = disvec.index.day
    disvec['hour'] = disvec.index.hour
    disvec['weekday'] = disvec.index.weekday # Monday is 0 and Sunday is 6
   # disvec['hourdis'] = numpy.matlib.repmat(
   #     hourdis, 1, int(disvec.shape[0]/24)).transpose() 
    disvec['hourdis']=np.zeros([disvec['hour'].shape[0],1])
    for ii in range(0,disvec['hourdis'].shape[0]):
        if disvec['hour'][ii] == 0:
            disvec['hourdis'][ii] = hourdis[0]
        if disvec['hour'][ii] == 1:
            disvec['hourdis'][ii] = hourdis[1]
        if disvec['hour'][ii] == 2:
            disvec['hourdis'][ii] = hourdis[2]
        if disvec['hour'][ii] == 3:
            disvec['hourdis'][ii] = hourdis[3]
        if disvec['hour'][ii] == 4:
            disvec['hourdis'][ii] = hourdis[4]
        if disvec['hour'][ii] == 5:
            disvec['hourdis'][ii] = hourdis[5]
        if disvec['hour'][ii] == 6:
            disvec['hourdis'][ii] = hourdis[6]
        if disvec['hour'][ii] == 7:
            disvec['hourdis'][ii] = hourdis[7]
        if disvec['hour'][ii] == 8:
            disvec['hourdis'][ii] = hourdis[8]
        if disvec['hour'][ii] == 9:
            disvec['hourdis'][ii] = hourdis[9]
        if disvec['hour'][ii] == 10:
            disvec['hourdis'][ii] = hourdis[10]
        if disvec['hour'][ii] == 11:
            disvec['hourdis'][ii] = hourdis[11]
        if disvec['hour'][ii] == 12:
            disvec['hourdis'][ii] = hourdis[12]
        if disvec['hour'][ii] == 13:
            disvec['hourdis'][ii] = hourdis[13]
        if disvec['hour'][ii] == 14:
            disvec['hourdis'][ii] = hourdis[14]
        if disvec['hour'][ii] == 15:
            disvec['hourdis'][ii] = hourdis[15]
        if disvec['hour'][ii] == 16:
            disvec['hourdis'][ii] = hourdis[16]
        if disvec['hour'][ii] == 17:
            disvec['hourdis'][ii] = hourdis[17]
        if disvec['hour'][ii] == 18:
            disvec['hourdis'][ii] = hourdis[18]
        if disvec['hour'][ii] == 19:
            disvec['hourdis'][ii] = hourdis[19]
        if disvec['hour'][ii] == 20:
            disvec['hourdis'][ii] = hourdis[20]
        if disvec['hour'][ii] == 21:
            disvec['hourdis'][ii] = hourdis[21]
        if disvec['hour'][ii] == 22:
            disvec['hourdis'][ii] = hourdis[22]
        if disvec['hour'][ii] == 23:
            disvec['hourdis'][ii] = hourdis[23] 
   
    disvec['weekdis']=np.zeros([disvec['weekday'].shape[0],1])
    for ii in range(0,disvec['weekday'].shape[0]):
        if disvec['weekday'][ii] == 6:
            disvec['weekdis'][ii] = weekdis[0] 
        if disvec['weekday'][ii] == 0:
            disvec['weekdis'][ii] = weekdis[1]
        if disvec['weekday'][ii] == 1:
            disvec['weekdis'][ii] = weekdis[2] 
        if disvec['weekday'][ii] == 2:
            disvec['weekdis'][ii] = weekdis[3] 
        if disvec['weekday'][ii] == 3:
            disvec['weekdis'][ii] = weekdis[4] 
        if disvec['weekday'][ii] == 4:
            disvec['weekdis'][ii] = weekdis[5] 
        if disvec['weekday'][ii] == 5:
            disvec['weekdis'][ii] = weekdis[6] 
    
    disvec['monthdis']=np.zeros([disvec['month'].shape[0],1])
    for ii in range(0,disvec['month'].shape[0]):
        if disvec['month'][ii] == 1:
            disvec['monthdis'][ii] = monthdis[0] 
        if disvec['month'][ii] == 2:
            disvec['monthdis'][ii] = monthdis[1] 
        if disvec['month'][ii] == 3:
            disvec['monthdis'][ii] = monthdis[2] 
        if disvec['month'][ii] == 4:
            disvec['monthdis'][ii] = monthdis[3] 
        if disvec['month'][ii] == 5:
            disvec['monthdis'][ii] = monthdis[4]     
        if disvec['month'][ii] == 6:
            disvec['monthdis'][ii] = monthdis[5] 
        if disvec['month'][ii] == 7:
            disvec['monthdis'][ii] = monthdis[6] 
        if disvec['month'][ii] == 8:
            disvec['monthdis'][ii] = monthdis[7] 
        if disvec['month'][ii] == 9:
            disvec['monthdis'][ii] = monthdis[8] 
        if disvec['month'][ii] == 10:
            disvec['monthdis'][ii] = monthdis[9]  
        if disvec['month'][ii] == 11:
            disvec['monthdis'][ii] = monthdis[10] 
        if disvec['month'][ii] == 12:
            disvec['monthdis'][ii] = monthdis[11]  
   
    disvec['prod']=disvec['hourdis']*disvec['weekdis']*disvec['monthdis']/numWeeks        
    # converting from hourly to second basis
    disvec['prod'] = disvec['prod']/3600 
    dataTempo = np.zeros([datePfct.shape[0],dataNC.shape[1],
                          dataNC.shape[2],dataNC.shape[3]])
    print(str(dataTempo.shape))
    
    for jj in range(0,dataTempo.shape[1]):
        for ii in range(0,dataTempo.shape[0]):
            utcoffs = numpy.unique(ltz)
            for utcoff in utcoffs:
                idx = ltz==utcoff
                dataTempo[ii,jj,idx]= dataNC[0,jj,idx]* np.roll(disvec['prod'],int(utcoff))[ii]
    
    # #Condition for equal timezone in whole domain - roll vector to UTC time
    # if tag==1:        
    #     for jj in range(0,dataTempo.shape[1]):
    #         for ii in range(0,dataTempo.shape[0]):
    #             utcoffs = numpy.unique(lc2utc)
    #             for utcoff in utcoffs:
    #                 idx = lc2utc==utcoff
    #                 dataTempo[ii,jj,idx.transpose()]= dataNC[0,jj,idx.transpose()]* np.roll(disvec['prod'],int(utcoff))[ii]
    #             #dataTempo[ii,jj,:,:]= dataNC[0,jj,:,:]* np.roll(disvec['prod'],lc2utc)[ii]
    # else:
    #     for jj in range(0,dataTempo.shape[1]):
    #         for ii in range(0,dataTempo.shape[0]):
    #             utcoffs = numpy.unique(lc2utc)
    #             for utcoff in utcoffs:
    #                 idx = lc2utc==utcoff
    #                 dataTempo[ii,jj,idx.transpose()]= dataNC[0,jj,idx.transpose()]* np.roll(disvec['prod'],int(utcoff))[ii]

    return dataTempo,datePfct,disvec