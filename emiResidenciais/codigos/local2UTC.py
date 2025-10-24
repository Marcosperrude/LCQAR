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

def local2UTC(xx, yy):
    
    """
    xx = matriz de longitudes [lat x lon]
    yy = matriz de latitudes  [lat x lon]
    """
    
    lc2utc = np.zeros([xx.shape[0], xx.shape[1]])  # agora lat x lon
    test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    tf = TimezoneFinder(in_memory=True)

    # Primeiros fusos extremos em longitude (mantendo latitude inicial)
    ltz0 = tf.timezone_at(lng=xx[0, 0], lat=yy[0, 0])
    ltz0 = float(test_naive.tz_localize(ltz0).strftime('%Z')[-1])
    ltzn = tf.timezone_at(lng=xx[0, -1], lat=yy[0, 0])
    ltzn = float(test_naive.tz_localize(ltzn).strftime('%Z')[-1])

    if ltz0 == ltzn:
        # Toda a grade tem o mesmo fuso
        lc2utc = np.ones([xx.shape[0], xx.shape[1]]) * ltz0
        tag = 1

    else:
        # Pode haver variação de fuso entre colunas de longitude
        for j in range(xx.shape[1]):  # varre lon
            print(f"Longitude {j}")
            # # Amostra três latitudes (topo, meio e base)
            # local_time_zone0 = tf.timezone_at(lng=xx[0, j], lat=yy[0, j])
            # t0 = float(test_naive.tz_localize(local_time_zone0).strftime('%Z')[-1])

            # local_time_zone5 = tf.timezone_at(
            #     lng=xx[xx.shape[0] // 2, j],
            #     lat=yy[yy.shape[0] // 2, j]
            # )
            # t1 = float(test_naive.tz_localize(local_time_zone5).strftime('%Z')[-1])

            # local_time_zone10 = tf.timezone_at(lng=xx[-1, j], lat=yy[-1, j])
            # t2 = float(test_naive.tz_localize(local_time_zone10).strftime('%Z')[-1])

            # # Se toda a coluna tem o mesmo fuso
            # if (t0 == t1) and (t1 == t2):
            #     lc2utc[:, j] = t0
            # else:
            # Caso varie com latitude
            for i in range(xx.shape[0]):  # varre lat
                local_time_zone = tf.timezone_at(lng=xx[i, j], lat=yy[i, j])
                lc2utc[i, j] = float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1])
        tag = 0

    return lc2utc, tag



# fig, ax = plt.subplots(figsize=(10, 6))
# pcm = ax.pcolormesh(xx, yy, lc2utc, cmap='coolwarm', shading='auto')
# plt.colorbar(pcm, ax=ax, label='Diferença Local - UTC (horas)')
# BR_UF.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.8)
# ax.set_xlabel('Longitude')
# ax.set_ylabel('Latitude')
# ax.set_title('Mapa de fusos horários estimados (UTC offset)')
# plt.show()

