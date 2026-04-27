################################################################################
# Subset Raw Ping data by "slow Points"
# Created on: 04/06/2025
# Created by: Anne and Callie

#This script subsets all points to those that are "slow"
################################################################################


################################################################################
# prep
################################################################################
exec(open('EV00_settings.py').read())


################################################################################
# functions
################################################################################



def find_k_nearest_points(df_, evcs_proj,  k=1, max_distance=max_distance_ping_to_EVCS): #remove cbg_df
    # Remove null geometries
    df=prepare_gdf(df_)
    df = df.dropna(subset=['geometry'])
    evcs_proj = evcs_proj.dropna(subset=['geometry'])

    # Ensure CRS match
    assert df.crs == evcs_proj.crs, "Coordinate reference systems do not match (df, evcs)!"
    
    # Extract coordinates
    df_coords = np.column_stack((df.geometry.x, df.geometry.y))
    evcs_coords = np.column_stack((evcs_proj.geometry.x, evcs_proj.geometry.y))

    # Build KD-tree and query
    tree = cKDTree(evcs_coords)
    distances, indices = tree.query(df_coords, k=k, distance_upper_bound=max_distance)

    results = []

    for i, (dist, idx) in enumerate(zip(distances, indices)):
        if dist != np.inf:
            #if 'cuebiq_id' in df.columns:
            results.append({
           # 'df_index': df.index[i],
            'cuebiq_id': df.loc[df.index[i], 'cuebiq_id'],
            'timestamp': df.loc[df.index[i], 'event_zoned_datetime'],
            'nearest_evcs_ID': evcs_proj.loc[evcs_proj.index[idx],'ID'],
            'distance': dist,
            'dist_acc':df.loc[df.index[i], 'accuracy_meters']

            })
            
    result_df = pd.DataFrame(results)
    
    return result_df


def process_day(date_str, complete_info=False):
    """        
    insert
    """
    
    # start timer
    date_start = time.time()
    if debug: print("\t ** starting **")
    
    # read in raw ping data for the date
    pings_user_subset = pd.read_parquet(data_path+f"ping{min_daily_pings}/user_subset/ping_data_{current_city}_{date_str}.parquet")
    print(f"\t * Read in data by {round((time.time()-date_start)/60, 2)} minutes")
    print(f'\t\t # of pings: {len(pings_user_subset):,}')
    
    # filter to slow points
    pings_slow_points = pings_user_subset[pings_user_subset.speed_kmh <= max_speed_slow_points] 
    del pings_user_subset
    
    # save slow points
    pings_slow_points.to_parquet(data_path + f"ping{min_daily_pings}/user_subset_slow/slow_points_{current_city}_{date_str}.parquet")
    
    print(f'\t * Filtered + saved slow points by {round((time.time()-date_start)/60, 2)} minutes')
    print(f'\t\t # of slow points: {len(pings_slow_points):,}')
    
    
    # calculate EVCS slow points for EVCS subset
    if complete_info == True:
        evcs_sub_gdf = pd.read_csv(root+f"data/evcs_locations/{current_city}/evcs_combined_complete_info_{max_distance_combine_stations}m_buffer.csv") 
        evcs_sub_proj = prepare_gdf(evcs_sub_gdf)
        evcs_slow_points_sub_ = find_nearest_within_distance_chunked(pings_slow_points, 
                                                                     evcs_sub_proj, 
                                                                     k = 1, 
                                                                     max_distance = max_distance_ping_to_EVCS, 
                                                                     chunk_size = 10000)
        evcs_slow_points_sub = evcs_slow_points_sub_.merge(evcs_sub_proj[['ID','Latitude','Longitude']], left_on='nearest_evcs_ID', right_on='ID',how='left')
        evcs_slow_points_sub.to_parquet(current_session_path + f"user_subset_slow_evcs/choice_model_subset/slow_points_{current_city}_{date_str}.parquet")
        
        print(f'\t * Identified + saved EVCS slow points by {round((time.time()-date_start)/60, 2)} minutes')
        print(f'\t\t # of EVCS(sub) slow points: {len(evcs_slow_points_sub):,}')
        print(f'\t\t # of UNIQUE users with an EVCS(sub) slow point: {len(evcs_slow_points_sub.cuebiq_id.unique()):,}')
        del evcs_slow_points_sub
        
    
    # find slow points "near" an EVCS
    evcs_slow_points_ = find_nearest_within_distance_chunked(pings_slow_points, 
                                                             evcs_proj, 
                                                             k = 1, 
                                                             max_distance = max_distance_ping_to_EVCS, 
                                                             chunk_size = 10000)
    del pings_slow_points
    evcs_slow_points = evcs_slow_points_.merge(evcs_proj[['ID','Latitude','Longitude']], 
                                               left_on='nearest_evcs_ID', 
                                               right_on='ID', 
                                               how='left')
    
    # save slow points "near" an EVCS
    evcs_slow_points.to_parquet(current_session_path + 
                                f"user_subset_slow_evcs/slow_points_{current_city}_{date_str}.parquet")
    print(f'\t * Identified + saved EVCS slow points by {round((time.time()-date_start)/60, 2)} minutes')
    print(f'\t\t # of EVCS slow points: {len(evcs_slow_points):,}')
    print(f'\t\t # of UNIQUE users with an EVCS slow point: {len(evcs_slow_points.cuebiq_id.unique()):,}')

    return evcs_slow_points


def filter_ping_data(complete_info=False):
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
        f_exists = os.path.isfile(current_session_path + f"user_subset_slow_evcs/slow_points_{current_city}_{date_str}.parquet")

        if f_exists and not overwrite: 
            print(f"---------- Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        
        # print alert of what day is being processed
        print(f"-----------------------------------------------------------------------------\nProcessing date: {date_str}")
        
        # CENTRAL ACTION: filter daily data by ping frequency
        process_day(date_str, complete_info)
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)
        
        
    
################################################################################
# run
################################################################################

# print some info before starting
print("\n\n-----------------------------------------------------------------------------\nSettings:")
print(f"\t * overwrite = {overwrite}")
print(f"\t * Start date = {start_date}")
print(f"\t * End date  = {end_date}")
print(f"\t * Max distance btwn ping + station = {max_distance_ping_to_EVCS}\n\n")

evcs_gdf = pd.read_csv(root+f'data/evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv')
evcs_proj = prepare_gdf(evcs_gdf)


filter_ping_data(complete_info=False)