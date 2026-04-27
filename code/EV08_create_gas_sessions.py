################################################################################
# Identify Gas Station Stops
# Created on: 04/06/2025
# Created by: Anne and Callie

#Identify all gas station sessions 
################################################################################


################################################################################
# prep
################################################################################
exec(open('EV00_settings.py').read())
from scipy.spatial import cKDTree



################################################################################
# functions
################################################################################


def find_k_nearest_points(df_, gas_gdf,  k=1,max_distance=max_distance_ping_to_EVCS): #remove cbg_df
    # Remove null geometries
    df=prepare_gdf(df_)
    df = df.dropna(subset=['geometry'])
    gas_gdf = gas_gdf.dropna(subset=['geometry'])

    # Ensure CRS match
    assert df.crs == gas_gdf.crs, "Coordinate reference systems do not match (df, evcs)!"
    
    # Extract coordinates
    df_coords = np.column_stack((df.geometry.x, df.geometry.y))
    gas_coords = np.column_stack((gas_gdf.geometry.x, gas_gdf.geometry.y))

    # Build KD-tree and query
    tree = cKDTree(gas_coords)
    distances, indices = tree.query(df_coords, k=k, distance_upper_bound=max_distance)

    results = []

    for i, (dist, idx) in enumerate(zip(distances, indices)):
        if dist != np.inf:
            #if 'cuebiq_id' in df.columns:
            results.append({
           # 'df_index': df.index[i],
            'cuebiq_id': df.loc[df.index[i], 'cuebiq_id'],
            'timestamp': df.loc[df.index[i], 'stop_zoned_datetime'],
            'nearest_SG_ID': gas_gdf.loc[gas_gdf.index[idx],'ID_safegraph'],
            'distance': dist,
            'dwell_time':df.loc[df.index[i], 'dwell_time_minutes'],
            'classification_type': df.loc[df.index[i], 'classification_type']

            })
            
    result_df = pd.DataFrame(results)
    
    return result_df


def process_day(date_str):
    """        
    insert
    """
    
    # start timer
    date_start = time.time()
    print(f"\t * starting")
    
    # read in raw ping data for the date
    stop_data = pd.read_csv(f"{data_path}raw/stop/stop_data_{current_city}_{date_str}.csv.gz")
    #just added, testing 8/25
    
    with open(f'{data_path}ping{min_daily_pings}/user_subset/user_set_{current_city}.txt', 'r') as file:
        content = file.read().strip()
        uid_set = ast.literal_eval(content)
    subset_ids = list(uid_set)  # Convert set to list
    print(len(stop_data))
    stop_data=stop_data[stop_data.cuebiq_id.isin(subset_ids)]
    print(len(stop_data))
    
    stop_data["stop_zoned_datetime"] = pd.to_datetime(stop_data["stop_zoned_datetime"], utc=True).dt.tz_convert(timezone)
    
    #print(f"\t * Read in data by {round((time.time()-date_start)/60, 2)} minutes")
    print('# number of stops in day: ',len(stop_data))
    
    gas_stops = find_nearest_within_distance_chunked(stop_data, gas_station_proj , k=1, max_distance=max_distance_stop_to_gas, chunk_size=10000)
    gas_stops = gas_stops[gas_stops['classification_type']!='RECURRING_AREA']
    gas_stops=gas_stops[gas_stops['dwell_time']<gas_station_dwell_time_max] #add to settings 
   
    # print('# number of pings with EVCS slow points: ',len(evcs_slow_points))
    # print('num users with a stop',len(evcs_slow_points.cuebiq_id.unique()))
    gas_stops.to_csv(data_path+f'ping{min_daily_pings}/gas_station_visits/'+ f"/gas_station_stops_{current_city}_{date_str}_{max_distance_stop_to_gas}m_{gas_station_dwell_time_max}min.csv")

    return gas_stops


def filter_stop_data():
    """
    Process the data that is saved by processing date to be by event date, for all counties. 
    start_date should be string of format "20220130"
    end_date should be string of format "20220130"
    """
    
    # convert inputs to datetime
    start_dt = dt.strptime(start_date, '%Y%m%d')
    end_dt = dt.strptime(end_date, '%Y%m%d')

    # loop through days to pull individually
    current_date = start_dt
    while current_date <= end_dt:
        
        # prep date times
        date_str = str(current_date.strftime('%Y%m%d'))
        date_start = time.time()
        
        # check if date already exists, in which case, next date
        f_exists = os.path.isfile(data_path+f'ping{min_daily_pings}/gas_station_visits/' + f"gas_station_stops_{current_city}_{date_str}_{max_distance_stop_to_gas}m_{gas_station_dwell_time_max}min.csv")
        if f_exists and not overwrite: 
            print(f"---------- Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        
        # print alert of what day is being processed
        print(f"-----------------------------------------------------------------------------\nProcessing date: {date_str}")
        
        # CENTRAL ACTION: filter daily data by ping frequency
        process_day(date_str)
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)
        
        
    
################################################################################
# run
################################################################################

gas_station_gdf=pd.read_csv(data_path+f'evcs_locations/{current_city}/POI_gas_stations.csv')
gas_station_proj=prepare_gdf(gas_station_gdf)

filter_stop_data()