#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 16:24:37 2025

fonte  : "https://github.com/leohoinaski/BRAVES/blob/main/local2UTC.py"

@author: marcos
"""

from timezonefinder import TimezoneFinder
import numpy as np
import pandas as pd

def local2UTC(xX,yY):
    lc2utc = np.zeros([xX.shape[1],xX.shape[0]])
    test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    tf = TimezoneFinder(in_memory=True)
    
    ltz0 = tf.timezone_at(lng=xX[0,0], lat=yY[0,0])
    ltz0 = float(test_naive.tz_localize(ltz0).strftime('%Z')[-1])
    ltzn = tf.timezone_at(lng=xX[0,-1], lat=yY[0,0])
    ltzn = float(test_naive.tz_localize(ltzn).strftime('%Z')[-1])
    
    if ltz0==ltzn: 
        #lc2utc=ltz0
        lc2utc =np.ones([xX.shape[1],xX.shape[0]])*ltz0
        tag = 1
        
    else: 
        for ii in range(0,xX.shape[1]):       
            print("Longitude " +str(ii))
            local_time_zone0 = tf.timezone_at(lng=xX[0,ii], lat=yY[0,ii])
            t0 = float(test_naive.tz_localize(local_time_zone0).strftime('%Z')[-1])
            local_time_zone5 = tf.timezone_at(lng=xX[round(xX.shape[0]/2),ii], 
                                              lat=yY[round(xX.shape[0]/2),ii])
            t1= float(test_naive.tz_localize(local_time_zone5).strftime('%Z')[-1])
            local_time_zone10 = tf.timezone_at(lng=xX[-1,ii], lat=yY[-1,ii])
            t2= float(test_naive.tz_localize(local_time_zone10).strftime('%Z')[-1])
            
            if (t0==t1) and (t1==t2):                      
                lc2utc[:,ii] = float(test_naive.tz_localize(local_time_zone0).strftime('%Z')[-1])
                
            else:
                for jj in range(0,xX.shape[1]):
                    local_time_zone = tf.timezone_at(lng=xX[jj,ii], lat=yY[jj,ii])
                    lc2utc[jj,ii] = float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1])
            #lc2utc[ii,jj] = float(re.split('GMT',local_time_zone)[1])
        tag=0
    return lc2utc, tag
