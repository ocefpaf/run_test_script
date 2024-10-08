# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 10:11:12 2024

@author: Matthew.Conlin
"""

import warnings
warnings.filterwarnings('ignore')
import datetime
import intake
import numpy as np
import pandas as pd
import pickle

def init():
    '''
    Function to initialize the CORA 500 m grid dataset from AWS storage.

    Parameters
    ----------
    none

    Returns
    -------
    ds : xarray.Dataset
        The CORA dataset, as a dask-enabled xarray dataset. Not loaded into memory.
    '''
    catalog = intake.open_catalog("s3://noaa-nos-cora-pds/CORA_intake.yml",storage_options={'anon':True})
    ds = catalog['CORA-V1-500m-grid-1979-2022'].to_dask()
    ds = ds.assign_coords(latitude=('latitude',ds['lat'].data))
    ds = ds.assign_coords(longitude=('longitude',ds['lon'].data))
    return ds


def load(ds,latlon_or_i,yr_start=None,yr_end=None):       
        if isinstance(latlon_or_i,list):
            d = np.array(haversine(ds['lat'],ds['lon'],latlon_or_i[0],latlon_or_i[1]))
            i_d = int(np.where(d==min(d))[0])
        else:
            i_d = latlon_or_i
        
        if yr_start and yr_end:
            i_t_start = int(np.where(ds['time']==np.datetime64(datetime.datetime(yr_start,1,1)))[0])
            i_t_end = int(np.where(ds['time']==np.datetime64(datetime.datetime(yr_end,2,1)))[0])
            # Load the data into memory #
            z = ds['zeta'][i_t_start:i_t_end+1,i_d].compute()
            t = ds['time'][i_t_start:i_t_end+1].compute()
        else:
            # Load the data into memory #
            z = ds['zeta'][:,i_d].compute()
            t = ds['time'].compute()
        
        hourly_height = pd.DataFrame({'time':t,
                            'val':z})
        return hourly_height
    
def save(ts,save_str):
    with open(save_str,'wb') as f:
            pickle.dump(ts,f)
    
def isin(ds,lon_min,lon_max,lat_min,lat_max):
    i1 = (ds.lat>=lat_min).compute()
    i2 = (ds.lat<=lat_max).compute()
    i3 = (ds.lon>=lon_min).compute()
    i4 = (ds.lon<=lon_max).compute()
    i = np.where(i1 & i2 & i3 & i4)[0]
    return i

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    R = 6371 
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = R * c
    return km  


def test_main():
    i = 0
    f_out = 'CORA_Jan2022_test.pkl'
    try:
        print('Initializing CORA dataset from AWS storage...')
        ds = init()
    except:
        print('FAILED. Something went wrong initializing the data from AWS storage.')
    else:
        try:
            print('Loading hourly data from January 2022 into memory...')
            data = load(ds,i,yr_start=2022,yr_end=2022)
        except:
            print('FAILED. Something went wrong loading the dataset into memory.')
        else:
            try:
                print('Saving a compressed binary version of the data to current directory...')
                save(data,f_out)
            except:
                print('FAILED. Something went wrong saving the dataset locally in compressed binary (pkl) format.')
            else:
                print('TEST PASSED')


if __name__=='__main__':
    test_main()
            


