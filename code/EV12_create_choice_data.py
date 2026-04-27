exec(open('EV00_settings.py').read())
max_distance_combine_stations = 10
current_session_path='/home/jovyan/SAI_EVCS/data/ping36/sessions_maxEVCSdist10_maxEVCSdur240/'
max_distance_combine_stations=5
start_date = "20220301"
end_date = "20220531"
min_daily_pings=36


def haversine_vector(lat1, lon1, lats2, lons2):
    """Call the haversine function for a vector instead of just one distance"""

    
    R = 6371000  # radius of Earth in meters
    
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lats2, lons2 = np.radians(lats2), np.radians(lons2)
    
    dlat = lats2 - lat1
    dlon = lons2 - lon1
    
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lats2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return (R * c)/1000 #return km
 

def add_cbg_col(df, cbg_proj, lat, lng, geo_col):

    
    gdf_points = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df[lng], df[lat]),
        crs=4326
    )
    gdf_points = gdf_points.to_crs(cbg_proj.crs)#4326
    
    
    result = gpd.sjoin(gdf_points, cbg_proj[['GEOID', 'geometry']], how='left', predicate='intersects')
    
    # Remove duplicates BEFORE extracting GEOID

    result = result[~result.index.duplicated(keep='first')]
    

    df[geo_col] = result['GEOID']
    
    return df

    
def process_day(date_str, evcs_proj):

    evcs_session_stops=pd.read_csv(current_session_path + f"evcs_session_stops/model/driver_EVCS_behavior_{current_city}_{date_str}.csv",index_col=0)

    evcs_session_stops.stop_zoned_datetime=pd.to_datetime(evcs_session_stops.stop_zoned_datetime, utc=True).dt.tz_convert('America/Los_Angeles')

    evcs_session_stops['poi_visit']=(~evcs_session_stops.placekey.isna())*1

    #each day there should only be one POI visit so this should be fine
    ev_poi_bool=evcs_session_stops.groupby('cuebiq_id').poi_visit.max().to_dict()

    #using stop data not raw data 
    df_stops = pd.read_csv(f"{data_path}raw/stop/stop_data_{current_city}_{date_str}.csv.gz")
    #print(len(evcs_session_df))
    df_stops_filtered = df_stops[df_stops.cuebiq_id.isin(ev_driver_ls)].copy()
    df_stops_filtered.stop_zoned_datetime=pd.to_datetime(df_stops_filtered.stop_zoned_datetime, utc=True).dt.tz_convert('America/Los_Angeles')



    # -----------------------------
    # 1. Standardize timestamps
    # -----------------------------
    evcs_session_stops['stop_zoned_datetime'] = (
        pd.to_datetime(evcs_session_stops['stop_zoned_datetime']) .dt.tz_convert('US/Pacific')

    )

    df_stops_filtered['stop_zoned_datetime'] = (
        pd.to_datetime(df_stops_filtered['stop_zoned_datetime']) .dt.tz_convert('US/Pacific')
    )

    # Remove duplicate stop timestamps per user (safe for merge_asof)
    df_stops_filtered = df_stops_filtered.drop_duplicates(
        subset=['cuebiq_id', 'stop_zoned_datetime']
    )

    # -----------------------------
    # 2. Load session durations
    # -----------------------------
    session_df = pd.read_csv(
        current_session_path + f"evcs_sessions/evcs_session_duration_{current_city}_{date_str}.csv",
        index_col=0
    )

    # Remove existing duration column if present
    if 'duration' in evcs_session_stops.columns:
        evcs_session_stops = evcs_session_stops.drop(columns=['duration'])

    # Merge duration
    evcs_session_stops = evcs_session_stops.merge(
        session_df[['cuebiq_id', 'evcs_id', 'duration_LB']],
        on=['cuebiq_id', 'evcs_id'],
        how='inner'
    )

    evcs_session_stops.rename(columns={'duration_LB': 'duration'}, inplace=True)

    # -----------------------------
    # 3. Create search timestamps
    # -----------------------------
    evcs_session_stops['search_ts_pre'] = (
        evcs_session_stops['stop_zoned_datetime'] - pd.Timedelta(minutes=5)
    )

    evcs_session_stops['search_ts_post'] = (
        evcs_session_stops['stop_zoned_datetime']
        + pd.to_timedelta(evcs_session_stops['duration'], unit='m')
        + pd.Timedelta(minutes=5)
    )

    # Remove duplicates that break merge_asof
    evcs_session_stops = evcs_session_stops.drop_duplicates(
        subset=['cuebiq_id', 'search_ts_pre']
    )
    evcs_session_stops = evcs_session_stops.drop_duplicates(
        subset=['cuebiq_id', 'search_ts_post']
    )

    # -----------------------------
    # 4. PRE join (previous stop)
    # -----------------------------
    df_stops_pre = (
        df_stops_filtered[['cuebiq_id', 'stop_zoned_datetime', 'lat','lng']]
        .rename(columns={
            'stop_zoned_datetime': 'ts_pre',
            'lat': 'lat_pre',
            'lng': 'lng_pre'
        })
    )

    # Required sorting for merge_asof
    evcs_session_stops = evcs_session_stops.sort_values(
        ['search_ts_pre', 'cuebiq_id']
    ).reset_index(drop=True)

    df_stops_pre = df_stops_pre.sort_values(
        ['ts_pre', 'cuebiq_id']
    ).reset_index(drop=True)

    evcs_session_stops = pd.merge_asof(
        evcs_session_stops,
        df_stops_pre,
        left_on='search_ts_pre',
        right_on='ts_pre',
        by='cuebiq_id',
        direction='backward'
    )

    # -----------------------------
    # 5. POST join (next stop)
    # -----------------------------
    df_stops_post = (
        df_stops_filtered[['cuebiq_id', 'stop_zoned_datetime', 'lat','lng']]
        .rename(columns={
            'stop_zoned_datetime': 'ts_post',
            'lat': 'lat_post',
            'lng': 'lng_post'
        })
    )

    evcs_session_stops = evcs_session_stops.sort_values(
        ['search_ts_post', 'cuebiq_id']
    ).reset_index(drop=True)

    df_stops_post = df_stops_post.sort_values(
        ['ts_post', 'cuebiq_id']
    ).reset_index(drop=True)

    evcs_session_stops = pd.merge_asof(
        evcs_session_stops,
        df_stops_post,
        left_on='search_ts_post',
        right_on='ts_post',
        by='cuebiq_id',
        direction='forward'
    )

    # -----------------------------
    # 6. Reporting
    # -----------------------------
    print(f"Final Row Count: {len(evcs_session_stops)}")
    print(f"NaN ts_pre: {evcs_session_stops['ts_pre'].isna().sum()}")
    print(f"NaN ts_post: {evcs_session_stops['ts_post'].isna().sum()}")
    # pings_df = pd.read_parquet(
    #     f"{data_path}ping{min_daily_pings}/user_subset/"
    #     f"ping_data_{current_city}_{date_str}.parquet"
    # )

#     ping_df = (
#         pings_df
#         .loc[pings_df.cuebiq_id.isin(ev_driver_ls)]
#         .copy()
#     )

#     ping_df["event_zoned_datetime"] =  pd.to_datetime(ping_df["event_zoned_datetime"]).dt.tz_convert('US/Pacific')


#     # -------------------------------------------------------
#     # PING FALLBACK FOR MISSING PRE / POST STOPS
#     # -------------------------------------------------------

#     # Identify rows needing fallback
#     missing_pre = evcs_session_stops['ts_pre'].isna()
#     missing_post = evcs_session_stops['ts_post'].isna()

#     print(f"Missing PRE stops: {missing_pre.sum()}")
#     print(f"Missing POST stops: {missing_post.sum()}")

#     # -------------------------------------------------------
#     # Prepare ping data
#     # -------------------------------------------------------

#     ping_pre = (
#         ping_df[['cuebiq_id', 'event_zoned_datetime', 'lat','lng']]
#         .rename(columns={
#             'event_zoned_datetime': 'ts_pre_ping',
#             'lat': 'lat_pre_ping',
#             'lng': 'lng_pre_ping'
#         })
#         .sort_values(['ts_pre_ping', 'cuebiq_id'])
#         .reset_index(drop=True)
#     )

#     ping_post = (
#         ping_df[['cuebiq_id', 'event_zoned_datetime', 'lat','lng']]
#         .rename(columns={
#             'event_zoned_datetime': 'ts_post_ping',
#             'lat': 'lat_post_ping',
#             'lng': 'lng_post_ping'
#         })
#         .sort_values(['ts_post_ping', 'cuebiq_id'])
#         .reset_index(drop=True)
#     )

#     # -------------------------------------------------------
#     # PRE fallback (only rows missing ts_pre)
#     # -------------------------------------------------------

#     if missing_pre.sum() > 0:

#         pre_subset = (
#             evcs_session_stops.loc[missing_pre]
#             .sort_values(['search_ts_pre', 'cuebiq_id'])
#             .reset_index()
#         )

#         pre_filled = pd.merge_asof(
#             pre_subset,
#             ping_pre,
#             left_on='search_ts_pre',
#             right_on='ts_pre_ping',
#             by='cuebiq_id',
#             direction='backward'
#         )

#         evcs_session_stops.loc[pre_filled['index'], 'ts_pre'] = pre_filled['ts_pre_ping'].values
#         evcs_session_stops.loc[pre_filled['index'], 'lat_pre'] = pre_filled['lat_pre_ping'].values
#         evcs_session_stops.loc[pre_filled['index'], 'lng_pre'] = pre_filled['lng_pre_ping'].values

#     # -------------------------------------------------------
#     # POST fallback (only rows missing ts_post)
#     # -------------------------------------------------------

#     if missing_post.sum() > 0:

#         post_subset = (
#             evcs_session_stops.loc[missing_post]
#             .sort_values(['search_ts_post', 'cuebiq_id','lng'])
#             .reset_index()
#         )

#         post_filled = pd.merge_asof(
#             post_subset,
#             ping_post,
#             left_on='search_ts_post',
#             right_on='ts_post_ping',
#             by='cuebiq_id',
#             direction='forward'
#         )

#         evcs_session_stops.loc[post_filled['index'], 'ts_post'] = post_filled['ts_post_ping'].values
#         evcs_session_stops.loc[post_filled['index'], 'lat_post'] = post_filled['lat_post_ping'].values
#         evcs_session_stops.loc[post_filled['index'], 'lng_post'] = post_filled['lng_post_ping'].values


    # -------------------------------------------------------
    # Final reporting
    # -------------------------------------------------------

    print("After ping fallback:")
    print(f"Remaining NaN ts_pre: {evcs_session_stops['ts_pre'].isna().sum()}")
    print(f"Remaining NaN ts_post: {evcs_session_stops['ts_post'].isna().sum()}")

#     evcs_session_stops=evcs_session_stops[['session_id','cuebiq_id', 'stop_zoned_datetime', 'evcs_id',
#                                                   'lat','lng', 'duration', 'geometry', 'poi_visit','pre_ts', 'pre_lat', 
#                                                   'pre_lng','post_ts',  'post_lat', 'post_lng']].copy()
                         
    evcs_session_stops.rename(columns={'stop_zoned_datetime':'timestamp'},inplace=True)
    evcs_session_stops_diffs=add_info(evcs_session_stops,cbg_gdf)
    
    
    return evcs_session_stops_diffs

def add_info(evcs_sessions_filtered,cbg_gdf):
    
    #check lat/lng crs
    evcs_sessions_filtered['dist_OE'] = haversine_vectorized(evcs_sessions_filtered.lat.values, 
                                                             evcs_sessions_filtered.lng.values, 
                                                            evcs_sessions_filtered.lat_pre.values, 
                                                            evcs_sessions_filtered.lng_pre.values)

    evcs_sessions_filtered['dist_OD'] = haversine_vectorized(evcs_sessions_filtered.lat_post.values, 
                                                             evcs_sessions_filtered.lng_post.values, 
                                                            evcs_sessions_filtered.lat_pre.values, 
                                                            evcs_sessions_filtered.lng_pre.values)


    evcs_sessions_filtered['dist_ED'] = haversine_vectorized(evcs_sessions_filtered.lat.values, 
                                                             evcs_sessions_filtered.lng.values, 
                                                            evcs_sessions_filtered.lat_post.values, 
                                                            evcs_sessions_filtered.lng_post.values)

    # evcs_sessions_filtered.post_ts=pd.to_datetime(evcs_sessions_filtered.post_ts) 
    # evcs_sessions_filtered.pre_ts=pd.to_datetime(evcs_sessions_filtered.pre_ts) 
    # evcs_sessions_filtered['duration_OD']= (evcs_sessions_filtered['post_ts'] - evcs_sessions_filtered['pre_ts']).dt.total_seconds() / 60
    
    # 1. Parse timestamps with UTC as base
    evcs_sessions_filtered['ts_pre']  = pd.to_datetime(evcs_sessions_filtered['ts_pre'], utc=True)
    evcs_sessions_filtered['ts_post'] = pd.to_datetime(evcs_sessions_filtered['ts_post'], utc=True)

    # 2. Convert from UTC to Pacific Time (handles PST/PDT automatically)
    evcs_sessions_filtered['ts_pre']  = evcs_sessions_filtered['ts_pre'].dt.tz_convert('America/Los_Angeles')
    evcs_sessions_filtered['ts_post'] = evcs_sessions_filtered['ts_post'].dt.tz_convert('America/Los_Angeles')

    # 3. Compute duration in minutes
    evcs_sessions_filtered['duration_OD'] = (
        evcs_sessions_filtered['ts_post'] - evcs_sessions_filtered['ts_pre']
    ).dt.total_seconds() / 60
    

    # 1. Parse timestamp with UTC as base
    evcs_sessions_filtered.rename(columns={'GEOID':'GEOID_test'},inplace=True)
    #print(evcs_sessions_filtered.columns)
    evcs_sessions_filtered['timestamp'] = pd.to_datetime(evcs_sessions_filtered['timestamp'], utc=True)

    # 2. Convert from UTC to Pacific Time (auto-handles DST between PST and PDT)
    evcs_sessions_filtered['timestamp'] = evcs_sessions_filtered['timestamp'].dt.tz_convert('America/Los_Angeles')

    # evcs_sessions_filtered['timestamp'] = pd.to_datetime(evcs_sessions_filtered['timestamp'])
    # print(evcs_sessions_filtered['timestamp'])

    evcs_sessions_filtered['date']=evcs_sessions_filtered.timestamp.dt.date
    evcs_sessions_filtered['hour']=evcs_sessions_filtered.timestamp.dt.hour
    evcs_sessions_filtered['DOW']=evcs_sessions_filtered.timestamp.dt.dayofweek

    evcs_sessions_filtered=add_cbg_col(evcs_sessions_filtered,cbg_gdf,lat='lat_pre',lng='lng_pre',geo_col='GEOID_O')
    evcs_sessions_filtered=add_cbg_col(evcs_sessions_filtered,cbg_gdf,lat='lat_post',lng='lng_post',geo_col='GEOID_D')
    evcs_sessions_filtered=add_cbg_col(evcs_sessions_filtered,cbg_gdf,lat='lat',lng='lng',geo_col='GEOID_E')
    
    # evcs_sessions_filtered=evcs_sessions_filtered.reset_index().rename(columns={'index':'cuebiq_id'})
    evcs_sessions_filtered_out=evcs_sessions_filtered.copy()
    

    return evcs_sessions_filtered_out




def process_evcs_session_stops(start_date, end_date):
    """

    """
    ev_session_info=pd.DataFrame()
    start_dt = dt.strptime(start_date, '%Y%m%d')
    end_dt = dt.strptime(end_date, '%Y%m%d')

# loop through days to pull individually
    current_date = start_dt
    while current_date <= end_dt:

        # print alert of what day is being processed
        date_str = str(current_date.strftime('%Y%m%d'))
        date_start = time.time()

        print(f"-------------------------------------------------\nProcessing date: {date_str}")

        date_start = time.time()



        f_exists = os.path.isfile(root+f"data/choice_model/evcs_session_info_ping{min_daily_pings}/evcs_session_{date_str}_5min_4_7.csv")
        print(f_exists,root+f"data/choice_model/evcs_session_info_ping{min_daily_pings}/evcs_session_{date_str}_5min_4_7.csv")
        if f_exists and not overwrite: 
            print(f"\t * Date {date_str} has already been processed, reading in")
            ev_session_iter=pd.read_csv(root+f"data/choice_model/evcs_session_info_ping{min_daily_pings}/evcs_session_{date_str}_5min_4_7.csv",index_col=0)

            #pd.read_csv(root+f'data/choice_model/evcs_session_info_ping{min_daily_pings}/evcs_session_{date_str}.csv',index_col=0)

            #ev_session_iter.rename(columns={'Unnamed: 0':'cuebiq_id'},inplace=True)

        else:
            ev_session_iter=process_day(date_str,evcs_proj)
            #ev_session_iter=ev_session_iter[~(ev_session_iter.ts_post.isna()|ev_session_iter.ts_pre.isna()|(ev_session_iter.classification_type=='RECURRING_AREA'))]
            ev_session_iter=ev_session_iter[~(ev_session_iter.ts_post.isna()|ev_session_iter.ts_pre.isna())]

            #ev_session_iter=ev_session_iter.reset_index().rename(columns={'index':'cuebiq_id'})
            ev_session_iter=ev_session_iter[[ 'session_id', 'cuebiq_id', 'timestamp',
           'dwell_time_minutes', 'lat', 'lng', 'evcs_dist', 'T_since_charge',
           'classification_type', 'evcs_id', 
            'poi_visit', 'duration', 'ts_pre', 'lat_pre', 'lng_pre', 'ts_post', 'lat_post',
           'lng_post', 'dist_OE', 'dist_OD', 'dist_ED', 'duration_OD', 'date',
           'hour', 'DOW', 'GEOID_O', 'GEOID_D', 'GEOID_E']].copy()


        ev_session_iter.to_csv(root+f"data/choice_model/evcs_session_info_ping{min_daily_pings}/evcs_session_{date_str}_5min_4_7.csv")


        ev_session_info=pd.concat([ev_session_info,ev_session_iter])#combine all days

        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)

    
    ev_session_info=ev_session_info[ev_session_info.cuebiq_id.isin(ev_driver_ls)].copy()

    #evcs_sessions_filtered=evcs_sessions_filtered.reset_index().rename(columns={'index':'cuebiq_id'})
    #ev_session_formatted=add_info(ev_session_info,cbg_gdf)
    ev_session_info.to_csv(root+f'/data/choice_model/evcs_session_OED_{start_date}_{end_date}_ping{min_daily_pings}_model_5min_4_7.csv')



 ################################################################################
# run
################################################################################

current_city = "SF"
#overwrite=True

with open(current_session_path+f'ev_driver_info/user_set_{current_city}_model.txt', "r") as file:
    set_string = file.read() # Read the string from the file
ev_driver_ls = list(ast.literal_eval(set_string)) # Convert the string back to a set

evcs_df=pd.read_csv(data_path+f'evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv')
evcs_proj = prepare_gdf(evcs_df)

cbg_gdf = gpd.read_file(root+f'data/geo_files/{current_city}/studyarea_cbg.shp')
cbg_gdf=cbg_gdf.to_crs(4326)

#
process_evcs_session_stops(start_date, end_date)




 
