
 ################################################################################
# Create EVCS Sessions
# Created on: 12/27/2025
# Created by: Anne and Callie

#This Script identifies EVCS sessions and outputs a df of all identified 
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


def ensure_projected_crs(gdf, epsg=3857):
    """
    Force projected CRS in meters
    """
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS")

    if gdf.crs.to_epsg() != epsg:
        gdf = gdf.to_crs(epsg)

    return gdf


def find_k_nearest_points(df, evcs_gdf, k=1, max_distance=max_distance_ping_to_EVCS):
    """
    df + evcs_gdf must already:
    - have geometry
    - be projected in meters
    - share CRS
    """

    df = df.dropna(subset=["geometry", "event_zoned_datetime"])
    evcs_gdf = evcs_gdf.dropna(subset=["geometry"])

    # if len(df) == 0 or len(evcs_gdf) == 0:
    #     return pd.DataFrame()

    assert df.crs == evcs_gdf.crs, "CRS mismatch between pings and EVCS"

    df_coords = np.column_stack((df.geometry.x.values, df.geometry.y.values))
    evcs_coords = np.column_stack((evcs_gdf.geometry.x.values, evcs_gdf.geometry.y.values))

    tree = cKDTree(evcs_coords)
    distances, indices = tree.query(
        df_coords,
        k=k,
        distance_upper_bound=max_distance
    )

    rows = []
    for i, (dist, idx) in enumerate(zip(distances, indices)):
        if dist == np.inf:
            continue

        rows.append({
            "cuebiq_id": df.iloc[i]["cuebiq_id"],
            "timestamp": df.iloc[i]["event_zoned_datetime"],
            "nearest_evcs_ID": evcs_gdf.iloc[idx]["ID"],
            "distance": float(dist),
            "speed_kmh": df.iloc[i]["speed_kmh"],
            "dist_acc": df.iloc[i]["accuracy_meters"]
        })

    return pd.DataFrame(rows)





def find_nearest_within_distance_chunked(
    df,
    evcs_gdf,
    k=1,
    max_distance=max_distance_ping_to_EVCS,
    chunk_size=100_000
):
    results = []

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        out = find_k_nearest_points(chunk, evcs_gdf, k, max_distance)

        if not out.empty:
            results.append(out)

    if len(results) == 0:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)



def assign_evcs_groups_one_user(user_df, evcs_prox_dict):
    """
    Labels the EVCS groups for each user into (0,1,2...) referring to the EVCS prox dict created in 5B
    """
    evcs_ids = user_df["nearest_evcs_ID"].unique()

    group_dict = {}
    evcs_to_group = {}
    visited = set()
    group_num = 0

    for evcs_id in evcs_ids:
        if evcs_id in visited:
            continue

        group_members = set(evcs_prox_dict.get(evcs_id, [evcs_id]))

        changed = True
        while changed:
            changed = False
            for member in list(group_members):
                neighbors = set(evcs_prox_dict.get(member, [member]))
                if not neighbors.issubset(group_members):
                    group_members |= neighbors
                    changed = True

        # store group as flat list
        group_dict[group_num] = sorted(group_members)

        # map EVCS -> group
        for member in group_members:
            evcs_to_group[member] = group_num

        visited |= group_members
        group_num += 1

    user_df = user_df.copy()
    user_df["Group"] = user_df["nearest_evcs_ID"].map(evcs_to_group)

    return user_df, group_dict



def get_evcs_session_duration(pings_user_subset,date_str,ev_users_uids,evcs_df):
    # --- timestamps ---
    pings_user_subset = pings_user_subset.copy()
    # pings_user_subset["event_zoned_datetime"] = normalize_timestamp(
    #     pings_user_subset["event_zoned_datetime"]
    # )

    # --- geometry ---
    pings_user_subset = prepare_gdf(pings_user_subset)
    evcs_gdf = prepare_gdf(evcs_df)

    # --- CRS ---
    pings_user_subset = ensure_projected_crs(pings_user_subset)
    evcs_gdf = ensure_projected_crs(evcs_gdf)

    return find_nearest_within_distance_chunked(
        pings_user_subset,
        evcs_gdf,
        k=1,
        max_distance=max_distance_ping_to_EVCS,
        chunk_size=10_000
    )


def get_daily_evcs_users(evcs_prox_dict, date_str, sub_str=""):
        """
    Identifies users with a slow point near an EVCS for each day
    """
    df = pd.read_parquet(
        current_session_path + f"user_subset_slow_evcs/{sub_str}slow_points_{current_city}_{date_str}.parquet"
    )

    #df.loc[:,"timestamp"] = normalize_timestamp(df["timestamp"],timezone)
    df["timestamp"] = normalize_timestamp(df["timestamp"], timezone)
    df = df.sort_values(["cuebiq_id", "timestamp"])

    ev_users_uids = df["cuebiq_id"].unique().tolist()

    dfs = []
    all_group_evcs = {}

    for uid, sub_df in df.groupby("cuebiq_id", sort=False):
        sub_out, group_dict = assign_evcs_groups_one_user(sub_df, evcs_prox_dict)
        dfs.append(sub_out)
        all_group_evcs[uid] = group_dict

    if len(dfs) == 0:
        return pd.DataFrame(), [], {}

    return pd.concat(dfs), ev_users_uids, all_group_evcs



def process_day(date_str, sub_str, evcs_df, debug=False):
    """
    Process daily EVCS sessions with optimized performance.
    """
    t0 = time.time()
    if debug:
        print("\t ** starting **")

    # --- Load EVCS user info ---
    pings_evcs_slow_points, ev_users_uids, group_dict = (
        get_daily_evcs_users(evcs_prox_dict, date_str, sub_str)
    )

    # if not ev_users_uids or pings_evcs_slow_points.empty:
    #     return pd.DataFrame(columns=[
    #         'cuebiq_id', 'lat', 'lng', 'timestamp', 'evcs_id', 'duration_LB'
    #     ])

    # --- Load ping data ---
    pings_df = pd.read_parquet(
        f"{data_path}ping{min_daily_pings}/user_subset/"
        f"ping_data_{current_city}_{date_str}.parquet"
    )

    pings_user_subset = (
        pings_df
        .loc[pings_df.cuebiq_id.isin(ev_users_uids)]
        .copy()
    )

    pings_user_subset["event_zoned_datetime"] = (
        normalize_timestamp(
            pings_user_subset["event_zoned_datetime"], timezone
        )
    )
    del pings_df

    # --- Column sanity check ---
    required_cols = {
        "cuebiq_id", "lat", "lng", "speed_kmh", "event_zoned_datetime"
    }
    missing = required_cols - set(pings_user_subset.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # --- EVCS session candidates ---
    pings_near_evcs = get_evcs_session_duration(
        pings_user_subset, date_str, ev_users_uids, evcs_df
    )

    if debug:
        elapsed = round((time.time() - t0) / 60, 2)
        print(f"\t * Data loaded in {elapsed} minutes")

    # --- Pre-group ---
    slow_by_user = {
        uid: df.sort_values("timestamp").reset_index(drop=True)
        for uid, df in pings_evcs_slow_points.groupby("cuebiq_id", sort=False)
    }

    near_by_user = {
        uid: df.sort_values("timestamp").reset_index(drop=True)
        for uid, df in pings_near_evcs.groupby("cuebiq_id", sort=False)
    }

    raw_by_user = {
        uid: df.sort_values("event_zoned_datetime").reset_index(drop=True)
        for uid, df in pings_user_subset.groupby("cuebiq_id", sort=False)
    }

    results = []

    for user_id in ev_users_uids:

        user_slow = slow_by_user.get(user_id) #all points less than 10kmh + within 10m EVCS 
        user_raw = raw_by_user.get(user_id) #all raw points within 10m evcs
        user_near = near_by_user.get(user_id)#all raw points (no filter)

        if user_slow is None or user_raw is None or user_near is None:
            continue

        user_groups = group_dict.get(user_id, {}) #group_dict: maps group number → EVCS IDs in that group
        slow_idx = 0
        n_slow = len(user_slow)

        while slow_idx < n_slow:

            row = user_slow.iloc[slow_idx]

            user_ts = row["timestamp"]
            group_num = row["Group"]
            evcs_id = row["ID"]
            lat = row["Latitude"]
            lng = row["Longitude"]

            if pd.isna(lat) or pd.isna(lng):
                slow_idx += 1
                continue

            evcs_ids = user_groups.get(group_num)
            if not isinstance(evcs_ids, (list, set, tuple)):
                slow_idx += 1
                continue
                
            #Below we we look at all the raw points that are within the same evcs cluster as the evcs_id in row
            evcs_chunk = user_near[
                user_near.nearest_evcs_ID.isin(evcs_ids)
            ].copy()
            
            #evcs_chunk is the df of raw data points within 10 meters of relevant evcs

            if evcs_chunk.empty:
                slow_idx += 1
                continue

            t_start = evcs_chunk.timestamp.iloc[0]
            t_end = evcs_chunk.timestamp.iloc[-1]

            #raw chunk is all the raw datapoints between the evcs_chunk start and end
            raw_chunk = user_raw[
                (user_raw.event_zoned_datetime >= t_start) &
                (user_raw.event_zoned_datetime <= t_end)
            ]

            if len(raw_chunk) <= 1:
                slow_idx += 1
                continue

            distances = haversine_vectorized(
                lat, lng,
                raw_chunk["lat"].values,
                raw_chunk["lng"].values
            )

            filter_mask = (distances > 1000) | (raw_chunk.speed_kmh.values > 20)

            if filter_mask.any():
#                 filtered_times = raw_chunk.event_zoned_datetime.values[filter_mask]
#                 before = filtered_times < user_ts
                
                filtered_times = raw_chunk.loc[filter_mask, "event_zoned_datetime"]

                before = filtered_times < user_ts
                if before.any():
                    evcs_chunk = evcs_chunk[
                        evcs_chunk.timestamp > filtered_times[before].max() #grabs the latest violation of the filter times pre slow point occurence
                    ]

                after = filtered_times > user_ts
                if after.any():
                    evcs_chunk = evcs_chunk[
                        evcs_chunk.timestamp < filtered_times[after].min() #grabs the earliest violation of the filter times post idx slow point 
                    ]

            if evcs_chunk.empty:
                slow_idx += 1
                continue

            if (
                len(evcs_chunk) >= 2 and
                (evcs_chunk.speed_kmh <= 10).any()
            ):
                duration = (
                    evcs_chunk.timestamp.max() -
                    evcs_chunk.timestamp.min()
                ).total_seconds() / 60

                if duration >= 10:
                    results.append({
                        "cuebiq_id": user_id,
                        "lat": lat,
                        "lng": lng,
                        "timestamp": user_ts,
                        "evcs_id": evcs_id,
                        "duration_LB": duration
                    })

                    remaining = user_slow.timestamp > evcs_chunk.timestamp.max()
                    if remaining.any():
                        slow_idx = remaining.values.nonzero()[0][0]
                    else:
                        break
                    continue

            slow_idx += 1
            
            evcs_session_df=pd.DataFrame(results, columns=[ 'cuebiq_id', 'lat', 'lng', 'timestamp','evcs_id','duration_LB'])
            evcs_session_df.to_csv(current_session_path + f"evcs_sessions/{sub_str}evcs_session_duration_{current_city}_{date_str}.csv")


    return evcs_session_df
    
    
def process_evcs_session_duration(complete_info=False):
    """
    Process the data that is saved by processing date to be by event date, for all counties. 
    start_date should be string of format "20220130"
    end_date should be string of format "20220130"
    """
    if complete_info:
        print('Processing EVCS Complete Info Subset')
        sub_str='choice_model_subset/'
        evcs_df=pd.read_csv(data_path+f"evcs_locations/{current_city}/evcs_combined_complete_info_{max_distance_combine_stations}m_buffer.csv")

    else:
        sub_str=''
        evcs_df=pd.read_csv(data_path+f'evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv')

        
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

        f_exists = os.path.isfile(current_session_path + f"evcs_sessions/{sub_str}evcs_session_duration_{current_city}_{date_str}.csv")

        if f_exists and not overwrite: 
            print(f"---------- Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        
        # print alert of what day is being processed
        print(f"-----------------------------------------------------------------------------\nProcessing date: {date_str}")
        
        # CENTRAL ACTION: identify EVCS sessions
        timestamp_df = process_day(date_str, sub_str, evcs_df)
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)
        
        
################################################################################
# run
################################################################################
import pickle

with open(f"{data_path}evcs_locations/{current_city}/evcs_within_{max_distance_multiple_station_visits}m_dict.pkl", "rb") as f:
    evcs_prox_dict = pickle.load(f)


process_evcs_session_duration(complete_info=False)




       
        
