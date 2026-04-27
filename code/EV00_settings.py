##################################################################
# Settings
# Created by: 02/25/2025
#
# Settings that control: 
#    - the time bounds to pricess
#    - the study population
#    - the EV stations to include
#    - the definitions of EV sessions
##################################################################

# import packages
import pandas as pd
import geopandas as gpd
import numpy as np
#import polars as pl

#!pip install pydeck -q -q
import pydeck as pdk

import os 
import glob

import time
from datetime import datetime as dt
from datetime import date
from datetime import timedelta, timezone

import re
import ast
import json

from scipy.spatial import cKDTree

# MAIN SETTINGS ---------------------------------------------------
root = "/home/jovyan/SAI_EVCS/"
data_path = root+"data/"

debug = False # just runs a few more print statements
overwrite = True
spatial_overwrite = False

# march april may for sf/sj
start_date = "20220301"
end_date = "20220531"
current_city = "SF" 
# SF, Seattle, Boston, Denver
#warning: need to combine Boston MSA cbgs before running the cbg study area file creation

#zip_crosswalk = root+'data/census/geocorr2022_cbg_zip.csv'
#zip_crosswalk= root+'data/census/cbg_zip_map.csv'
zip_crosswalk = root+'data/census/cbg_zcta_map.csv'
never_public_charge_percent = 0.269
ev_percentages = {"Bay": (1 - (0.044 * (1-never_public_charge_percent))), 
                  "SF": (1 - (0.0438 * (1-never_public_charge_percent))), 
                  "Seattle": (1 - (0.0185 * (1-never_public_charge_percent))), 
                  "Boston": (1 - (0.0182 * (1-never_public_charge_percent))), 
                  "Denver": (1 - (0.0546 * (1-never_public_charge_percent))),
                  "Austin": "NEEDS EV PERCENTAGE IN 00",
                  "LA": "NEEDS EV PERCENTAGE IN 00",  
                  "Raleigh": "NEEDS EV PERCENTAGE IN 00"}
ev_percentage = ev_percentages[current_city]

cities = {"Bay": ['US.CA.013', 'US.CA.041', 'US.CA.055', 'US.CA.075', 'US.CA.081', 'US.CA.097', 'US.CA.095', 
                  'US.CA.085', 'US.CA.001'],
          "SF":  ['US.CA.013', 'US.CA.041', 'US.CA.055', 'US.CA.075', 'US.CA.081', 'US.CA.097', 'US.CA.095', 
                  'US.CA.085', 'US.CA.001'],
          # "SF": ['US.CA.001', 'US.CA.013', 'US.CA.081', 'US.CA.041',  'US.CA.075'], 
          # "SJ": ['US.CA.085', 'US.CA.069'],
          "Seattle": ['US.WA.061', 'US.WA.033', 'US.WA.053'],
          "Boston": ['US.MA.009', 'US.MA.017', 'US.MA.021', 'US.MA.023', 'US.MA.025', 'US.NH.015', 'US.NH.017'], 
          "Denver": ['US.CO.031', 'US.CO.005', 'US.CO.059', 'US.CO.001', 'US.CO.035', 'US.CO.014', 
                     'US.CO.039', 'US.CO.093', 'US.CO.019', 'US.CO.047'], 
          "Austin": ['US.TX.021', 'US.TX.055', 'US.TX.209', 'US.TX.453', 'US.TX.491'],
          "LA": ['US.CA.059', 'US.CA.037'], 
          "Raleigh": ['US.NC.183', 'US.NC.101', 'US.NC.069']}

cities_fips = {"Bay": ['06013', '06041', '06055', '06075', '06081', '06097', '06095', '06085', '06001'],
               "SF": ['06013', '06041', '06055', '06075', '06081', '06097', '06095', '06085', '06001'],
              # "SF": ['06001', '06013', '06081', '06041', '06001', '06075'], 
              # "SJ": ['06085', '06069'],
               "Seattle": ['53061', '53033', '53053'],
               "Boston": ['25009', '25017', '25021', '25023', '25025', '25017', '33015', '33017'], 
               "LA": ['06037', '06059'], 
               "Denver": ['08031', '08005', '08059', '08001', '08035', '08014', '08039', '08093', '08019', '08047'], 
               "Austin": ['48021', '48055', '48209', '48453', '48491'],
               "Raleigh": ['37183', '37101', '37069']}
cities_tz = {"Bay": 'America/Los_Angeles', 
            "SF": 'America/Los_Angeles',
            "SJ": 'America/Los_Angeles',
            "Seattle": 'America/Los_Angeles',
            "Boston": 'America/New_York',
            "LA": 'America/Los_Angeles',
            "Denver": 'America/Denver', 
            "Austin": 'America/Chicago', #works for Austin
            "Raleigh": 'America/New_York'}

counties = cities[current_city]
counties_fips = cities_fips[current_city]
timezone = cities_tz[current_city]


# settings for pulling in data ---------------------------------------------------
schema_name = {'cda': 'CUEBIQ_DATA.PAAS_CDA_pe_V3'}
pings_table = f"{schema_name['cda']}.device_location_uplevelled"  
hw_table = f"{schema_name['cda']}.device_recurring_area"
processing_days = 2 # num of days post ping day to allow for the data to be processed, can be 0 to only include processing on event day.


# study population settings -------------------------------------------------------
min_daily_pings = 36
#print(f"CAREFUL, DAILY PINGS ARE SET TO 72!!")
minimum_days_incl = 5


# station settings  ---------------------------------------------------------------
station_set = "all" #("all" or "sub"), where "sub" is only stations that have full information
#run for sub 
exclude_private_stations = True
sum_ports = True # when nearby stations are grouped do you want to assume they are the same station in multiple datasets in which case you don't want to add all the ports (False) or do you want to assume there actually are several stations and add ports across them (True)
max_distance_combine_stations = 5 # dist in m to buffer nearby stations together
max_distance_upleveled_to_evcs = 50


# distance and time bounds for identifying session --------------------------------
max_speed_slow_points = 10 #kmh
driving_speed_thresh_kmh= 15 #kmh

max_distance_ping_to_EVCS = 10 # in meters
max_distance_multiple_station_visits = 100 # in meters

#max_time_multiple_station_visits = 60 # in minutes
max_time_EVCS_session = 240 # in minutes
min_time_EVCS_session = 10 # in minutes
print("************ WARNING THE min_time_EVCS_session IS SET TO 20 ************")
EVCS_session_time_bound = 3 # in hours

max_distance_stop_to_gas = 15 # in meters
gas_station_dwell_time_max = 15 # in minutes


# model for identifing drivers! --------------------------------
# generated in 'validation' folder 2A or 2D
##model_path = 'ols_Bay_results.pickle' #this is for choice model
model_path = 'ols_SeattleSFBostonDenver_results.pickle' #this is for choice model


# POI Visitation Info --------------------------------
max_distance_POI_to_EVCS = 200
max_distance_POI_visit = 50

current_session_path = f'{data_path}ping{min_daily_pings}/sessions_maxEVCSdist{max_distance_ping_to_EVCS}_maxEVCSdur{max_time_EVCS_session}/'

#2c = combine AFDC + open charge
#2ca = combine w safegraph, get gasoline 
#6cb = getting the number of stops during the duration of the session

# CONCERNS -------------------------------------------------------------------------
# census block years
# time zones?

# NOTES ----------------------------------------------------------------------------
# crs enforced
# only one charging per day
# need a function to deal with the duration of a session in 07


# create study area shapefiles if it doesn't already exist
f_exists = os.path.isfile(root+f'data/geo_files/{current_city}/studyarea_cbg.shp')
#does this work for boston?
if not f_exists or spatial_overwrite: 
    
    # read in the full US county shapefile, and filter to study area
    study_area = gpd.read_file(root+'data/geo_files/US_county/tl_2020_us_county.shp')
    study_area = study_area[study_area.GEOID.isin(counties_fips)]
    #study_area = study_area[study_area.STATEFP == '06']
    study_area.to_file(root+f'data/geo_files/{current_city}/studyarea.shp') 

    # # Combine all geometries (polygons) into one big polygon
    gdf_bay_area = study_area.unary_union
    gdf_combined = gpd.GeoDataFrame({'geometry': [gdf_bay_area]},crs=4269)#crs best for area calc
    gdf_combined=gdf_combined.to_crs(5070)
    #remove Farallon Islands
    gdf_exploded = gdf_combined.explode(index_parts=True)
    gdf_exploded['area'] = gdf_exploded.geometry.area # Calculate areas and sort
    gdf_sorted = gdf_exploded.sort_values(by='area', ascending=False)
    gdf_main = gdf_sorted.iloc[[0]] # Keep the largest polygon

    # Save the result
    gdf_main=gdf_main.to_crs(4326)
    gdf_main.to_file(root+f'data/geo_files/{current_city}/Study_Area_geo.shp')

    # based on the county study area, create bg study area
    cbg_gdf = gpd.read_file(root+f'data/geo_files/cbg_files/tl_2022_{cities_fips[current_city][0][0:2]}_bg.shp') #need to update this
    cbg_gdf=cbg_gdf.to_crs(4326)
    cbg_study_area = cbg_gdf.sjoin(gdf_main, how='inner', predicate='intersects')#why study_area not gdf_main
    cbg_study_area = cbg_study_area[['GEOID', 'geometry']]
    cbg_study_area = cbg_study_area.rename(columns={'GEOID_left': 'GEOID'})

    cbg_study_area.to_file(root+f'data/geo_files/{current_city}/studyarea_cbg.shp') 

################################################################################
# global functions
################################################################################

def cur_time_string(): 
    return (dt.now() - timedelta(hours=4)).strftime('%l:%M%p, %b %d')

def cur_time_diff(t): 
    return round((time.time()-t)/60, 2)


def prepare_gdf(df,proj=True):
    """
    Prepare GeoDataFrame with appropriate projection for Bay Area.
    
    :param df: DataFrame with 'lat' and 'lng' columns
    :param crs: Target CRS (default is California State Plane III, EPSG:26943)
    :return: Projected GeoDataFrame
    """
    if proj:
        try:
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df.lng, df.lat), crs="EPSG:4326" #bay area projection
            )
        except Exception as e1:
            try: 
                gdf = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
                )
            except Exception as e2: 
                print("Failed to create GeoDataFrame.")
                print("Error 1:", e1)
                print("Error 2:", e2)
                gdf = df
            
        return gdf.to_crs('EPSG:3857')
    
    else:
        print('CRS=EPSG:4326')
        try:
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df.lng, df.lat), crs="EPSG:4326" #bay area projection
            )
        except Exception as e:
            try: 
                gdf = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
                )
            except Exception as e: 
                try:
                    gdf = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE), crs="EPSG:4326"
                )

                except: 
                    print("CRS transformation failed:", e)
                    gdf = df

        return gdf
    

state_abbrev_to_fips = {
    'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06', 'CO': '08',
    'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12', 'GA': '13', 'HI': '15',
    'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19', 'KS': '20', 'KY': '21',
    'LA': '22', 'ME': '23', 'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27',
    'MS': '28', 'MO': '29', 'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33',
    'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
    'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45', 'SD': '46',
    'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50', 'VA': '51', 'WA': '53',
    'WV': '54', 'WI': '55', 'WY': '56', 'PR': '72'
}

def reformat_block_group_id_to_geoid(blockid):
    """

    """
    if type(blockid) == np.ndarray:
        blockid = blockid[0]
    
    if pd.isna(blockid):
        return None
    
    if len(blockid) == 18:  # Ensure GEOID has the expected length
        
        # set state
        state_abbrev = blockid[3:5]
    
        # Map to FIPS code using dictionary, default to '00' if not found
        state = state_abbrev_to_fips.get(state_abbrev, '00')

        if state == '00':
            print(f"Warning: Unknown state abbreviation '{state_abbrev}' in blockid {blockid}")
#         if blockid[3:5]=='CA':
#             state = '06'
#         else:
#             state='00'
        county = blockid[6:9]  # Extract county
        tract_block = blockid[-8:-2] # Extract tract
        block_group=blockid[-1:] # Extract block
        formatted_geoid = f"{state}{county}{tract_block}{block_group}"  # Concatenate to match block_group_id format
        return formatted_geoid
    
    return None  # In case the format is not as expected


def reformat_geoid_to_block_group_id(geoid):
    """
    Reformats Census track FIPs code to be consistent with spectus block_group_id 
    TODO: expand for all states, HARDCODED for CA
    """

    
    if len(geoid) == 12:  # Ensuring GEOID has the expected length
        state = 'US.CA'  # Hardcoded for California as '06'
        county = geoid[2:5]  # Extract county 
        tract_block = geoid[5:-1]  # Extract tract
        block_id=geoid[-1:] # Extract block
        formatted_block_group_id = f"{state}.{county}.{tract_block}.{block_id}"  # Concatenate to match block_group_id format
        return formatted_block_group_id
    
    return None  # In case the format is not as expected

def log_users(filename, subset_ids):
    """
    Update the file that contains all the infoamtion on 
    """    
    f_exists = os.path.isfile(filename)
    if f_exists and not overwrite: 
        with open(filename, "r") as file:
            set_string = file.read()
        user_set = ast.literal_eval(set_string)
        
    else:
        user_set = set()
        
    # update set with new users
    user_set.update(subset_ids)

    # save user subset
    set_string = str(user_set)
    with open(filename, "w") as file:
        file.write(set_string)
        
        
       
        
def query_hw(uid_list, date_str):    
    """
    For a specific day, reads in all work home locations for the uid_list, and returns a df
    """
    q = snow_engine.read_sql(
            f"""
           select 
                cuebiq_id, 
                tag_type_code,
                block_group_id,                  
                confidence_level,
                snapshot_event_date
            FROM {hw_table} s
                 WHERE s.country_code = 'US'
                 AND s.cuebiq_id IN ({uid_list})
                 AND s.snapshot_event_date = ({date_str})
           """)
    
    return q

def find_nearest_within_distance_chunked(df1, df2, k=1, max_distance=max_distance_ping_to_EVCS, chunk_size=100000):
    results = []
    for i in range(0, len(df1), chunk_size):
        
        chunk = df1.iloc[i:i+chunk_size].copy()
        chunk_result = find_k_nearest_points(chunk, df2, k, max_distance) 
        results.append(chunk_result)
        
    del df1
 
    results_df=pd.concat(results)
    
    return results_df

def haversine(lat1, lon1, lat2, lon2): #we could just be using euclidean in projected 
    """Calculate the great-circle distance between two points on the Earth in meters.
    Not:haversince needs to be based off crs:4326
    """
    R = 6371000  # Earth's radius in meters
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def haversine_vectorized(lat1, lon1, lats2, lons2):
    """Call the haversine function for a vector instead of just one distance"""
    R = 6371000  # radius of Earth in meters
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lats2, lons2 = np.radians(lats2), np.radians(lons2)
    
    dlat = lats2 - lat1
    dlon = lons2 - lon1
    
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lats2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return R * c