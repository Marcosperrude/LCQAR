# %%
# -*- coding: utf-8 -*-

"""
Created on Wed Apr 30 14:24:15 2025

@author: Marcos.Perrude
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os
# from timezonefinder import TimezoneFinder
from emissionsGrid import EmssionsGrid
import xarray as xr
import calendar
from temporalDisagg import temporalDisagg
import dask.array as da


#
def EmissionsPixelsWoodCoal(Combustivel, dt
                       , gridGerado, DataPath, OutPath, uf, BR_UF, 
                       poluentesWoodCoal, setores):
    # uf = 'SC'
    # dt = woodEmission
    # # # Padroniza CD_SETOR
    dt['CD_SETOR'] = dt['CD_SETOR'].astype(str)
    # Processa os estados dentro do grid
    #estados_intersectados = br_uf[br_uf.intersects(gridGerado.unary_union)].copy()
    #ufs = list(estados_intersectados['SIGLA_UF'])
    

    #for uf in ufs:
    print(f"Processando {uf}...")
    pasta_uf = [f for f in os.listdir(setores) if f.startswith(uf)][0]
    shapefile_path = os.path.join(setores, pasta_uf)
    shp_file = [f for f in os.listdir(shapefile_path) if f.endswith(".shp")][0]
    gdf_uf = gpd.read_file(os.path.join(shapefile_path, shp_file))
    gdf_uf['CD_SETOR'] = gdf_uf['CD_SETOR'].astype(str)
    
    emissoes_uf = gpd.GeoDataFrame(
    pd.merge(dt, gdf_uf[['CD_SETOR', 'geometry']], on='CD_SETOR', how='right'),
    geometry='geometry',
    crs=BR_UF.crs)

    emiGrid = EmssionsGrid(emissoes_uf, gridGerado, poluentesWoodCoal)
    
    # emiGrid = gridGerado.copy()
    # for pol in poluentesWoodCoal:
    #     print(f"Calculando {pol}...")
    #     valores = []
    #     for cell in gridGerado.geometry:
    #         setores_intersectados = emissoes[emissoes.intersects(cell)].copy()
    #         if setores_intersectados.empty:
    #             valores.append(np.nan)
    #             continue
    #         setores_intersectados["area_total"] = setores_intersectados.geometry.area
    #         setores_intersectados["area_intersectada"] = setores_intersectados.geometry.intersection(cell).area
    #         setores_intersectados["peso"] = setores_intersectados["area_intersectada"] / setores_intersectados["area_total"]
    #         valor = (setores_intersectados[pol] * setores_intersectados["peso"]).sum()
    #         valores.append(valor)  

    #     emiGrid[pol] = valores
        
    # emissoes_uf.plot()
    # emissoes = gpd.GeoDataFrame(pd.concat(setores_brasil, ignore_index=True), crs=br_uf.crs)
    
    return emiGrid

# #%%
# import geopandas as gpd

# emissoes_uf = gpd.GeoDataFrame(emissoes_uf, geometry='geometry')
# emissoes_uf.to_file(r'C:\Users\marcos perrude\Downloads\emissoes_uf.shp')
# import matplotlib.pyplot as plt

# fig, ax = plt.subplots(figsize=(10, 8))
# emissoes_uf.plot(
#     column='Consumo_lenha[ton/ano]',
#     ax=ax,
#     legend=True,
#     cmap='OrRd',
#     edgecolor='black'  # só se for geometria Polygon
# )
# plt.show()

#%%
from tqdm import tqdm

def EmissionsPixelsGLP(dt, BR_MUN, gridGerado, poluentesGLP, DataPath, Combustivel, ufs):
    """
    Calcula emissões de GLP para todos os anos e UFs de uma só vez.
    Retorna a matriz 5D preenchida.
    """
    BR_mun = BR_MUN.copy()
    
    gridMat5Dglp = np.zeros((len(poluentesGLP),
                             len(dt['ANO'].unique()), 12,
                             np.shape(np.unique(gridGerado.lat))[0],
                             np.shape(np.unique(gridGerado.lon))[0]))

    # Merge com geometrias
    BR_mun['CD_MUN'] = BR_mun['CD_MUN'].astype(int)
    dt['CODIGO IBGE'] = dt['CODIGO IBGE'].astype(int)
    geoCombDf = pd.merge(dt, 
                      BR_mun[['CD_MUN', 'geometry']], 
                      left_on='CODIGO IBGE',  # coluna de dt
                      right_on='CD_MUN',      # coluna de BR_mun
                      how='left')
    
    geoCombDf = gpd.GeoDataFrame(geoCombDf, geometry='geometry')

    # Loop ano/UF
    for ano in tqdm(geoCombDf['ANO'].unique(), desc="Processando anos"):
        # ano = 2022
        print(f"Processando {ano}...")
        dados_ano = geoCombDf[geoCombDf['ANO'] == ano]

        for uf in ufs:
            # uf= 'SC'
            dados_ano_uf = dados_ano[dados_ano['UF'] == uf]
            emiGridGLP = EmssionsGrid(dados_ano_uf, gridGerado, poluentesGLP)
            
            gridMat5Dglp = GridMat5D(Combustivel, emiGridGLP, gridMat5Dglp,
                                     poluentesGLP, DataPath, uf, ano)
            
    return gridMat5Dglp

# #Gerar Grid de emissoes de GLP
# def EmissionsPixelsGLP (combDf ,BR_MUN, gridGerado, poluentesGLP):
    
#     # combDf = propEmiCid[(propEmiCid['ANO'] == 2000) & (propEmiCid['UF'] == 'SP')]
    
#     BR_MUN['CD_MUN'] = BR_MUN['CD_MUN'].astype(int)#Shape com geometria
#     combDf['CODIGO IBGE'] = combDf['CODIGO IBGE'].astype(int)
#     combDf.rename(columns={'CODIGO IBGE': 'CD_MUN'}, inplace = True)
    
#     geoCombDf = pd.merge(
#         combDf,
#         BR_MUN[['CD_MUN', 'geometry']],
#         on='CD_MUN',
#         how='left'  # left para manter todos de combDt, mesmo sem geometria
#     )
    
    
#     geoCombDf['CD_MUN'] = geoCombDf['CD_MUN'].astype(object)
#     geoCombDf = gpd.GeoDataFrame(geoCombDf, geometry='geometry')

    
#     emiGrid = EmssionsGrid(geoCombDf, gridGerado, poluentesGLP)

#     return emiGrid

# def cellTimeZone(xx,yy):
    
#     #Tempo de teste apena sapara extrair o fuso
#     test_naive = pd.date_range('2019-01-01', '2019-04-07', freq='4H')
    
#     #Biblioteca que retorno o fuso
#     tf = TimezoneFinder(in_memory=True)
#     ltc =[]
    
#     for ii in range(0,xx.shape[0]):# Para todo x da grade 
#         #Loop over each cel in y direction
#         for jj in range(0,xx.shape[1]):# Para todo y da grade
#             local_time_zone = tf.timezone_at(lng=xx[ii,jj], lat=yy[ii,jj]) #Localiza o time zone
#             ltc.append(float(test_naive.tz_localize(local_time_zone).strftime('%Z')[-1])) #Extrai o numero do time zone
            
#     ltcGrid = np.reshape(ltc,
#                          (np.shape(xx)[0],
#                           np.shape(xx))[1])
#     return ltcGrid


# def GridMat5D(Combustivel, emiGrid, gridMat5D, poluentes, DataPath, uf):
   
#     # Combustivel= 'Propano'
#     # emiGrid = emiGridGLP
#     # gridMat5D = gridMat5Dglp
#     # poluentes = poluentesGLP
               
    
   
#     temporalFactorHist = pd.read_csv(DataPath + '/fatdesEPE.csv', index_col=0)
#     temporalFactor = pd.read_csv(DataPath + '/fatdes.csv')
#     temporalFactorUF = temporalFactor[temporalFactor['UF Destino'] == uf]
#     pesosMensais = temporalFactorUF['Peso'].reset_index(drop=True).values
   
#     for ii, pol in enumerate(poluentes):
#       # ii = 0
#       # pol = 'CO'
#       gridMat = np.reshape(
#           emiGrid[pol].fillna(0),
#           (np.shape(np.unique(emiGrid.lon))[0],
#            np.shape(np.unique(emiGrid.lat))[0])
#       ).transpose()   
      
#       if Combustivel in ['Lenha', 'Carvao']:
#             temporalFactorCombustivel = temporalFactorHist.loc[Combustivel].reset_index(drop=True)
#             for jj in range(gridMat5D.shape[1]):
#                 gridMat4D = gridMat * temporalFactorCombustivel.iloc[jj]  # (lat, lon)
#                 # Vetoriza os 12 meses
#                 gridMat5D[ii, jj, :, :, :] += gridMat4D[np.newaxis, :, :] * pesosMensais[:, np.newaxis, np.newaxis]
#       else:
#             # vetoriza direto os 12 meses
#             for jj in range(gridMat5D.shape[1]):
#                 gridMat5D[ii, jj, :, :, :] += gridMat[np.newaxis, :, :] * pesosMensais[:, np.newaxis, np.newaxis]

#     return gridMat5D


def GridMat5D(combustivel, emiGrid, gridMat5D, poluentes, DataPath, uf, anos):
   
    
       # combustivel= 'Lenha'
       # gridMat5D = gridMat5Dglp
       # poluentes = poluentesWoodCoal
       
       # combustivel= 'Propano'
       # emiGrid = emiGridGLP
       # gridMat5D = gridMat5Dglp
       # poluentes = poluentesGLP
       
    # Verificar, pois foi feita quantificação para 2022, mas atribui o ano como
    # refeferencia 2023
    temporalFactorHist = pd.read_csv(DataPath + '/fatdesEPE.csv', index_col=0)
    temporalFactor = pd.read_csv(DataPath + '/fatdes.csv')
    temporalFactorUF = temporalFactor[temporalFactor['UF Destino'] == uf]
    
    
    #emiGrid = emiGrid.fillna(0)
    for ii, pol in enumerate(poluentes):
    # pol = 'PM'
    # ii = 1
        # gridMat = np.reshape(
        #   emiGrid[pol].fillna(0),
        #   (np.shape(np.unique(emiGrid.lat))[0],
        #    np.shape(np.unique(emiGrid.lon))[0]))
        
        pivot = emiGrid.pivot_table(
            index="lat",
            columns="lon",
            values=pol,
            fill_value=0)
        pivot = pivot.reindex(index=np.unique(emiGrid['lat']), 
                              columns=np.unique(emiGrid['lon']), fill_value=0)
        
        gridMat = pivot.values[::-1, :]
        

        # Caso 1: Lenha ou Carvão → distribui para todos os anos
        if combustivel in ['Lenha', 'Carvao']:
            # combustivel  ='Lenha'
            # Historico de consumo EPE
            for jj in range(gridMat5D.shape[1]):  # loop anos
            # jj=1
                temporalFactorCombustivel = temporalFactorHist.loc[combustivel].reset_index()
                temporalFactorCombustivel = temporalFactorCombustivel[-anos:].reset_index()
                # desagregação anual
                gridMat4D = gridMat * temporalFactorCombustivel[combustivel].iloc[jj]
    
                # desagregação mensal
                for kk in range(12):
                    gridMat5D[ii, jj, kk, :, :] += gridMat4D * temporalFactorUF['Peso'].reset_index().iloc[kk].Peso
                    
    
        # Caso 2: Propano / Butano → escreve só no ano existente
        else:
            # pega o ano do dataframe de emissões 
            jj = anos - 2023
            for kk in range(12):
                gridMat5D[ii, jj, kk, :, :] += gridMat * temporalFactorUF['Peso'].reset_index().iloc[kk].Peso

      # for jj in range(gridMat5D.shape[1]):
                
      #           #Se lenha ou carvao, desagregar anualmente
      #           if combustivel in ['Lenha', 'Carvao']:
                    
      #               temporalFactorCombustivel = temporalFactorHist.loc[combustivel].reset_index()
                    
      #               #Desagregação anual
      #               gridMat4D = gridMat * temporalFactorCombustivel[combustivel].iloc[jj]
                    
      #               # desagregar mensal
      #               for kk in range(12):
      #                   # Aplica fator mensal
      #                   gridMat5D[ii, jj, kk, :, :] += gridMat4D * temporalFactorUF['Peso'].reset_index().iloc[kk].Peso
                
      #           else:
      #               # loop para todos os meses
      #               for kk in range(12):
      #                   # desagregar mensal
      #                   gridMat5D[ii, jj, kk, :, :] = gridMat * temporalFactorUF['Peso'].reset_index().iloc[kk].Peso

    return gridMat5D

def GridMat7D(weekdis_sul,weekdis_norte,hourdis,gridMat5D, poluentes, DataPath , 
              Combustivel,xx,yy , OutPath , lc2utc):
    
    # gridMat5D = gridMat5Dglp
    # poluentes = poluentesGLP
    # Loop temporal
    for iy in range(gridMat5D.shape[1]):
         # iy = 0
        for im in range(gridMat5D.shape[2]):
            # im= 1
            days_in_month = calendar.monthrange(2023+iy, im+1)[1]  # exemplo: ano = 1970 + iy
            # base_emis = gridMat5D[:, iy, im, :, :]  # emissões mensais [pol, lat, lon]
            
            # Cria array 5d
            mes = np.zeros((len(poluentes),days_in_month,24,
                            gridMat5D.shape[3],gridMat5D.shape[4]))
            
            # Gera série temporal horária local
            date_range = pd.date_range(f'{2023+iy}-{im+1:02d}-01', 
                                       periods=days_in_month * 24, 
                                       freq='H',
                                       )
            
            # Localizar o fator de emissao de hora e dia
            weekdays = np.array([d.weekday() for d in date_range])  # 0=Segunda
            hours = np.array([d.hour for d in date_range])
            
            week_factors_sul = np.array([weekdis_sul.iloc[w] for w in weekdays])
            week_factors_norte = np.array([weekdis_norte.iloc[w] for w in weekdays])
            
            hour_factors = np.array([hourdis.iloc[h] for h in hours])
            
            # desagregação taxa de emissao dia*hora norte
            combined_sul = week_factors_sul * hour_factors
            combined_norte = week_factors_norte * hour_factors
            
            # Normalização
            combined_sul  =  combined_sul/ combined_sul.sum()  
            combined_norte  =  combined_norte/ combined_norte.sum() 
            
            # Reshape para [dia, hora]
            combined_sul = combined_sul.reshape(days_in_month, 24)
            combined_norte = combined_norte.reshape(days_in_month, 24)
            
            # Desagregação dia/hora/poluente
            for iday in range(days_in_month):
                for ihr in range(24):
                    for pol in range (mes.shape[0]):
                        
                        # Multiplica emissões mensais pelos fatores diários e horários
                        mes[pol, iday, ihr, :, :] = gridMat5D[pol, iy, im, :, :] * np.where(
                            yy <= -20.0,
                            combined_sul[iday, ihr],
                            combined_norte[iday, ihr])
            
            
            gridMat4Dtemp = mes.reshape(
               mes.shape[0],
               mes.shape[1] * mes.shape[2],
              mes.shape[3],mes.shape[4])
            
            # Calcula o fuso horário local
            # time_local = np.empty((gridMat4Dtemp.shape[1] , gridMat5D.shape[3],
            #                        gridMat5D.shape[4]), dtype='datetime64[ns]')
            
            # for i in range(gridMat5D.shape[3]):
            #     for j in range(gridMat5D.shape[4]):
            #         shift_h = lc2utc[i, j]
            #         time_local[:,i, j] = date_range.values + np.timedelta64(int(shift_h), 'h')

            data_vars = {}
            for i, nome in enumerate(poluentes):
                # i=0
                # nome = 'PM'
                data_vars[nome] = xr.DataArray(
                    gridMat4Dtemp[i,:, :, :],  # shape: (time, lat, lon)Add commentMore actions
                    dims=["time", "lat", "lon"],
                    coords={
                        # "time": (("time","lat", "lon"), time_local),
                        "time": date_range,
                        "lon": xx[0, :],
                        "lat": yy[:,0]
                    }
                )
            ds = xr.Dataset(data_vars)
            
            ds.attrs['description'] = f"Emissões residenciais de {Combustivel} em toneladas"
            arquivo_path = os.path.join(OutPath,'edgar' ,'emissoes', Combustivel , f'{2023+iy}')
            os.makedirs(arquivo_path, exist_ok=True)
            arquivo_saida = os.path.join(arquivo_path , f'{2023+iy}_{im+1}.nc')
            
            ds.to_netcdf(arquivo_saida, mode='w')

    return ds
