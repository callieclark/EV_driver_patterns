################################################################################
# Pull stop data from server
# Created on: 04/06/2025
# Created by: Callie
################################################################################


################################################################################
# this script calculates the number of visits to POIs from the whole subset
################################################################################
exec(open('EV00_settings.py').read())

from shapely import wkt
from scipy.spatial import cKDTree




################################################################################
# functions
################################################################################



def find_k_nearest_pois(df, poi_gdf, max_distance=50):

    gdf=prepare_gdf(df)
    
    gdf = gdf.dropna(subset=['geometry'])
    poi_gdf = poi_gdf.dropna(subset=['geometry'])
    
    # Ensure CRS match
    assert gdf.crs == poi_gdf.crs, "Coordinate reference systems do not match!"
    
    
    result_df = gdf.copy()
    df_coords = np.column_stack((gdf.geometry.x, gdf.geometry.y))
    poi_coords = np.column_stack((poi_gdf.geometry.x, poi_gdf.geometry.y))
    
    tree = cKDTree(poi_coords)
    distances, indices = tree.query(df_coords, k=1, distance_upper_bound=max_distance)
    placekeys, categories, names = [], [], []
    
    for dist, idx in zip(distances, indices):
        if dist != np.inf and idx < len(poi_gdf):
            poi = poi_gdf.iloc[idx]
            placekeys.append(poi['PLACEKEY'])
            categories.append(poi['Category'])
            names.append(poi['LOCATION_NAME'])
        else:
            placekeys.append(np.nan)
            categories.append('no poi')
            names.append(np.nan)
    
    result_df['placekey'] = placekeys
    result_df['category'] = categories
    result_df['name'] = names

    
    return result_df

def find_pois_kdtree_then_sjoin(df_points, poi_points_gdf, poi_polygons_gdf,
                                max_distance=50):
    """
    Two-tier POI mapping:
    1) KDTree nearest-neighbor to point POIs within max_distance
    2) For points with no match, spatially join to polygon POIs
    """
        # --- Defensive checks: POIs must be EVCS-filtered ---
    required_cols = {"ID", "PLACEKEY", "Category", "LOCATION_NAME"}
    assert required_cols.issubset(poi_points_gdf.columns), \
        "poi_points_gdf must be EVCS-filtered and include an 'ID' column"

    assert required_cols.issubset(poi_polygons_gdf.columns), \
        "poi_polygons_gdf must be EVCS-filtered and include an 'ID' column"
    
    df_points.reset_index(inplace=True)
    gdf=prepare_gdf(df_points,proj=True)
    gdf = gdf.dropna(subset=['geometry'])
    
    poi_points = poi_points_gdf.to_crs('EPSG:3857').dropna(subset=['geometry'])
    poi_polygons = poi_polygons_gdf.to_crs('EPSG:3857').dropna(subset=['geometry'])

    # Make sure CRS match
    assert gdf.crs == poi_points.crs == poi_polygons.crs, "CRS do not match!"

    # =====================================================
    # ===============  TIER 1: KDTree match  ==============
    # =====================================================

    df_xy = np.column_stack((gdf.geometry.x, gdf.geometry.y))
    poi_xy = np.column_stack((poi_points.geometry.x, poi_points.geometry.y))

    tree = cKDTree(poi_xy)
    distances, idxs = tree.query(df_xy, k=1, distance_upper_bound=max_distance)


    # Fill KDTree results
    gdf["placekey"]  = np.nan
    gdf["category"]  = "no poi"
    gdf["name"]      = np.nan
    gdf["evcs_id"] = np.nan

#     matched_mask = (distances != np.inf) & (idxs < len(poi_points))

#     gdf.loc[matched_mask, "placekey"] = poi_points.iloc[idxs[matched_mask]]["PLACEKEY"].values
#     gdf.loc[matched_mask, "category"] = poi_points.iloc[idxs[matched_mask]]["Category"].values
#     gdf.loc[matched_mask, "name"]     = poi_points.iloc[idxs[matched_mask]]["LOCATION_NAME"].values
    
    
    matched_mask = (distances != np.inf) & (idxs < len(poi_points))

    gdf.loc[matched_mask, "placekey"] = (
        poi_points.iloc[idxs[matched_mask]]["PLACEKEY"].values
    )
    gdf.loc[matched_mask, "category"] = (
        poi_points.iloc[idxs[matched_mask]]["Category"].values
    )
    gdf.loc[matched_mask, "name"] = (
        poi_points.iloc[idxs[matched_mask]]["LOCATION_NAME"].values
    )
    gdf.loc[matched_mask, "evcs_id"] = (
        poi_points.iloc[idxs[matched_mask]]["ID"].values
    )

    # =====================================================
    # =======  TIER 2: Spatial join for unmatched ==========
    # =====================================================

    unmatched = gdf[gdf["placekey"].isna()].copy()
    unmatched = unmatched.drop(columns=['evcs_id'])
    matched   = gdf[~gdf["placekey"].isna()].copy()


    if len(unmatched) > 0:
        
        unmatched_clean = unmatched.drop(columns=['placekey', 'category', 'name'])
        

        sjoined = gpd.sjoin(
            unmatched_clean,
            poi_polygons[['PLACEKEY', 'Category', 'LOCATION_NAME', 'ID', 'geometry']],
            how='left',
            predicate='within'
        )

        #sjoined = sjoined.groupby(level=0).first() #removing multiple parks within EVCS range
        sjoined = sjoined[~sjoined.index.duplicated(keep='first')]


        sjoined.rename(columns={
            'PLACEKEY': 'placekey',
            'Category': 'category',
            'LOCATION_NAME': 'name',
            'ID': 'evcs_id'
        }, inplace=True)
        


        sjoined = sjoined.drop(columns=[c for c in sjoined.columns if c.startswith('index_')], errors='ignore')

        # fill missing polygon matches
        sjoined['category'] = sjoined['category'].fillna('no poi')

        if(len(matched))==0:
            final=sjoined.copy()
        else:

            final = pd.concat(
                [matched.reset_index(drop=True),
                    sjoined.reset_index(drop=True)
                ],
                ignore_index=True
            )


    else:
        final = gdf.copy()
    
    return final

# def add_cbg_col(charging_activity_df,cbg_gdf):
#     df=charging_activity_df.copy()
#     gdf_points = gpd.GeoDataFrame(
#         df, 
#         geometry=gpd.points_from_xy(df.lng, df.lat),
#         crs=cbg_gdf.crs  # Make sure CRS matches
#     )
#     result = gpd.sjoin(gdf_points, cbg_gdf[['GEOID', 'geometry']], how='left', predicate='within')

#     charging_activity_df['GEOID'] = result['GEOID']
#     return charging_activity_df

def add_cbg_col(charging_activity_df, cbg_gdf):
    if debug:
        print("=== DEBUGGING SPATIAL JOIN ===")
        print(f"charging_activity_df shape: {charging_activity_df.shape}")
        print(f"charging_activity_df index: {charging_activity_df.index.tolist()}")
        print("First few rows of charging_activity_df:")
        print(charging_activity_df[['lng', 'lat']].head())

        print(f"\ncbg_gdf shape: {cbg_gdf.shape}")
        print(f"cbg_gdf columns: {cbg_gdf.columns.tolist()}")
    
    # Convert to GeoDataFrame
    charging_activity_gdf = gpd.GeoDataFrame(
        charging_activity_df, 
        geometry=gpd.points_from_xy(charging_activity_df.lng, charging_activity_df.lat),
        crs=cbg_gdf.crs)
    if debug:
        print(f"\ncharging_activity_gdf shape: {charging_activity_gdf.shape}")
        print(f"charging_activity_gdf index: {charging_activity_gdf.index.tolist()}")

    # Apply spatial join with left join to keep all original rows
    result = gpd.sjoin(charging_activity_gdf, cbg_gdf[['GEOID', 'geometry']], 
                       how='left', predicate='within')
    if debug:
        print(f"\nresult shape: {result.shape}")
        print(f"result index: {result.index.tolist()[:20]}...")  # First 20 indices

        # Check if the original dataframe has duplicate indices
        print(f"\nOriginal df has duplicate indices: {charging_activity_df.index.duplicated().any()}")
        print(f"Result has duplicate indices: {result.index.duplicated().any()}")

        # Look at what's happening with row 0 specifically
        print(f"\nRow 0 in original data:")
        print(charging_activity_df.iloc[0][['lng', 'lat']])

        print(f"\nAll 'Row 0' matches in result:")
    row_0_matches = result.loc[0]
    if isinstance(row_0_matches, pd.DataFrame):
        print(f"Number of matches: {len(row_0_matches)}")
        print("Unique lat/lng combinations:")
        print(row_0_matches[['lng', 'lat']].drop_duplicates())
        print("All GEOID values:")
        print(row_0_matches['GEOID'].tolist())
    
    return charging_activity_df  # Return early for debugging

    
def process_day(date_str,count,poi_evcs_df,evcs_gdf):
    
    
    df_stops = pd.read_csv(f"{data_path}raw/stop/stop_data_{current_city}_{date_str}.csv.gz",index_col=0)
    df_stops=df_stops[df_stops.transformation_type=='KEEP'] #remove home stops
    charging_activity_df=pd.DataFrame(columns=['cuebiq_id','stop_zoned_datetime', 'dwell_time_minutes','lat','lng','evcs_dist','T_since_charge','classification_type','evcs_id','placekey','category','name'])
    
   
    
    with open(f'{data_path}ping{min_daily_pings}/user_subset/user_set_{current_city}.txt', 'r') as file:
        set_string = file.read()
        subset_ids = list(ast.literal_eval(set_string))
        #uid_list = [line.strip() for line in file.readlines()]
    #subset_ids = [int(i) for i in uid_list]
    
               
    
    df_stops=df_stops[df_stops.index.isin(subset_ids)].copy()
                    
    df_stops.stop_zoned_datetime=pd.to_datetime(df_stops.stop_zoned_datetime, utc=True).dt.tz_convert(cities_tz[current_city])
    #df_stops=df_stops[(df_stops.stop_zoned_datetime.dt.hour >=5)&(df_stops.stop_zoned_datetime.dt.hour <23)]
     
    poi_evcs_proj=prepare_gdf(poi_evcs_df)
    df_stops_proj=prepare_gdf(df_stops)
    
    #here we need to pass in point and poly gdf but subset by the poi_evcs_proj
    #df_stops_poi=find_k_nearest_pois(df_stops_proj, poi_evcs_proj, max_distance=max_distance_POI_visit)
    df_stops_poi=find_pois_kdtree_then_sjoin(df_stops_proj, point_poi_geo, poly_poi_geo,
                                max_distance=max_distance_POI_visit)
    df_stops_poi=df_stops_poi[df_stops_poi.classification_type!='RECURRING_AREA'].copy()


    
  #check that this is better than 13.. paths are redundant  
    df_stops_poi.to_csv(current_session_path+f'all_driver_info/poi_visits_{current_city}_{date_str}.csv')
    return charging_activity_df


def process_evcs_session_stops(start_date, end_date, save_path, min_daily_pings, max_distance_ping_to_EVCS):
    """
    Process the data that is saved by processing date to be by event date, for all counties. 
    start_date should be string of format "20220130"
    end_date should be string of format "20220130"
    """
    count=0
    evcs_df=pd.read_csv(root+f'data/evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv')
    evcs_gdf=prepare_gdf(evcs_df,proj=False)

    
    # convert inputs to datetime
    start_dt = dt.strptime(start_date, '%Y%m%d')
    end_dt = dt.strptime(end_date, '%Y%m%d')
    
    
    #check if file is generated
    #add print statement
    poi_f_exists = os.path.isfile(root+f'data/POI_data/{current_city}_POIs_by_EVCS_250m.csv')
    if poi_f_exists:
        poi_evcs_df=pd.read_csv(root+f'data/POI_data/{current_city}_POIs_by_EVCS_250m.csv')
        poi_evcs_df=prepare_gdf(poi_evcs_df)
    else: 
        print('CSV not generated')
    
    
    # loop through days to pull individually
    current_date = start_dt
    while current_date <= end_dt:
        
        # print alert of what day is being processed
        date_str = str(current_date.strftime('%Y%m%d'))
        date_start = time.time()
        
        print(f"-------------------------------------------------\nProcessing date: {date_str}")

        date_start = time.time()
        
        # check if date already exists, in which case, next date
        f_exists = os.path.isfile(current_session_path+f'all_driver_info/poi_visits_{current_city}_{date_str}.csv')

        if f_exists and not overwrite: 
            print(f"\t * Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        

        # CENTRAL ACTION: identify sessions
        process_day(date_str, count, poi_evcs_df, evcs_gdf)
        count+=1
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)
       


 ################################################################################
# run
################################################################################
#point_poi_df and poly_poi_df are subsets or whole?
point_poi_df=pd.read_csv(root+f'data/POI_data/POI_{current_city}_points.csv')
point_poi_df['geometry'] = point_poi_df['geometry'].apply(wkt.loads)
point_poi_geo = gpd.GeoDataFrame(point_poi_df, geometry='geometry', crs="EPSG:4326") 

poly_poi_df=pd.read_csv(root+f'data/POI_data/POI_{current_city}_polygons.csv')  
poly_poi_df['geometry'] = poly_poi_df['geometry'].apply(wkt.loads)
poly_poi_geo = gpd.GeoDataFrame(poly_poi_df, geometry='geometry', crs="EPSG:4326") 



process_evcs_session_stops(start_date, end_date, data_path, min_daily_pings, max_distance_ping_to_EVCS)
#process_evcs_session_stops('20220101','20220103', data_path, min_daily_pings, max_distance_ping_to_EVCS)
