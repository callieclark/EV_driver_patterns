################################################################################
# Calculate distance for points not in stop data
# Created on: 01/31/2026
# Created by: Anne and Callie
################################################################################


################################################################################
# prep
################################################################################
exec(open('EV00_settings.py').read())



################################################################################
# functions
################################################################################

def normalize_timestamp(s, timezone):
    """
    Ensure tz-aware pandas datetime in the desired timezone.
    Safe for tz-naive, tz-aware, and mixed inputs.
    """
    s = pd.to_datetime(s, errors="coerce")

    if s.dt.tz is None:
        # tz-naive → localize
        s = s.dt.tz_localize(timezone)
    else:
        # tz-aware → convert
        s = s.dt.tz_convert(timezone)

    return s

################################################################################
# Run Loop
################################################################################

def process_day(date_str):
    evcs_session_df=pd.read_csv(current_session_path + f"evcs_sessions/evcs_session_duration_{current_city}_{date_str}.csv",index_col=0)
    evcs_stop_df=pd.read_csv(f'{current_session_path}evcs_session_stops/model/driver_EVCS_behavior_{current_city}_{date_str}.csv',index_col=0)
    df_grey=evcs_stop_df[evcs_stop_df.category=='no stop'][['cuebiq_id', 'evcs_id','stop_zoned_datetime','lat', 'lng','category','geometry', 'GEOID']].merge(evcs_session_df[['cuebiq_id','evcs_id','duration_LB']],on=['cuebiq_id','evcs_id'])
    df_grey=df_grey.drop_duplicates(subset=['cuebiq_id', 'evcs_id','stop_zoned_datetime']).copy()

    pings_df = pd.read_parquet(
        f"{data_path}ping{min_daily_pings}/user_subset/"
        f"ping_data_{current_city}_{date_str}.parquet"
    )


    df_grey["stop_zoned_datetime"] = pd.to_datetime(
        df_grey["stop_zoned_datetime"],
        utc=True
    ).dt.tz_convert(timezone)  
    df_grey["evcs_dist_75"] = np.nan
    df_grey["evcs_dist_mean"] = np.nan
    df_grey["evcs_dist_max"] = np.nan
    df_grey["num_points"] = np.nan
    df_grey['date']=date_str


    pings_df["event_zoned_datetime"] = pd.to_datetime(
        pings_df["event_zoned_datetime"],
        utc=True
    ).dt.tz_convert(timezone)


    for id_iter in df_grey.cuebiq_id.unique():
        row = df_grey.loc[df_grey.cuebiq_id == id_iter].iloc[0]

        t_start = row.stop_zoned_datetime
        t_end = t_start + pd.Timedelta(minutes=row.duration_LB)

        mask = (
            (pings_df.cuebiq_id == id_iter) &
            (pings_df.event_zoned_datetime >= t_start) &
            (pings_df.event_zoned_datetime <= t_end)
        )

        pings_subset = pings_df.loc[mask]

        # Skip if no pings in window
        if pings_subset.empty:
            continue

        # Compute distances (vectorized)
        distances = haversine_vectorized(
            row.lat,                     # df_grey latitude
            row.lng,                     # df_grey longitude
            pings_subset.lat.values,     # ping latitudes
            pings_subset.lng.values      # ping longitudes
        )

        # Assign minimum distance back to df_grey
        df_grey.loc[df_grey.cuebiq_id == id_iter, "evcs_dist_max"] = distances.max()
        df_grey.loc[df_grey.cuebiq_id == id_iter, "evcs_dist_75"] = np.percentile(distances, 75)
        df_grey.loc[df_grey.cuebiq_id == id_iter, "evcs_dist_mean"] = distances.mean()
        df_grey.loc[df_grey.cuebiq_id == id_iter, "num_points"] = len(distances)
    return df_grey


def calc_dist_for_ping():
    """
    Process the data that is saved by processing date to be by event date, for all counties. 
    start_date should be string of format "20220130"
    end_date should be string of format "20220130"
    """
    df_comb=pd.DataFrame()
    
    # convert inputs to datetime
    start_dt = dt.strptime(start_date, '%Y%m%d')
    end_dt = dt.strptime(end_date, '%Y%m%d')

    # loop through days to pull individually
    current_date = start_dt
    while current_date <= end_dt:
        
        # prep date times
        date_str = str(current_date.strftime('%Y%m%d'))
        date_start = time.time()

        
        # CENTRAL ACTION: filter daily data by ping frequency
        df_iter=process_day(date_str)
        df_comb=pd.concat([df_comb,df_iter])
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)

        
    df_comb.to_csv(f'{current_session_path}evcs_session_stops/unobserved_points/unobserved_points_dist_{current_city}.csv')

################################################################################
# run
################################################################################

# print some info before starting
print("\n\n-----------------------------------------------------------------------------\nSettings:")
print(f"\t * City  = {current_city}")
print(f"\t * overwrite = {overwrite}")

print(f"\t * Start date = {start_date}")
print(f"\t * End date  = {end_date}")
        
                

calc_dist_for_ping()