
################################################################################
# Pull stop data from server
# Created on: 04/06/2025
# Created by: Callie 
#Script categorizes activities during charging session into staying at the car, leaving the car or visiting a POI
################################################################################


################################################################################
# prep
################################################################################
exec(open('EV00_settings.py').read())

from shapely import wkt
from scipy.spatial import cKDTree

fitness_wellness = ['Fitness and Recreational Sports Centers']
healthcare_pharmacies = ['Pharmacies and Drug Stores', 'Health and Personal Care Stores']
beauty = ['Personal Care Services', 'Hair Salons']
restaurants_cafes = ['Restaurants and Other Eating Places', 'Drinking Places (Alcoholic Beverages)', 'Food Services and Drinking Places', 'Special Food Services', 'Bakeries and Tortilla Manufacturing']
retail = ['Clothing Stores', 'Shoe Stores', 'Department Stores', 'Clothing and Clothing Accessories Stores', 'Sporting Goods, Hobby, and Musical Instrument Stores', 'Jewelry, Luggage, and Leather Goods Stores', 'Book Stores and News Dealers', 'Electronics and Appliance Stores', 'Furniture Stores', 'Home Furnishings Stores', 'Used Merchandise Stores', 'Office Supplies, Stationery, and Gift Stores', 'General Merchandise Stores, including Warehouse Clubs and Supercenters', #this could be in groceries? 
          'Other Miscellaneous Store Retailers', 'Florists']
grocery = ['Grocery Stores', 'Grocery and Food Stores', 'Supermarkets', 'Specialty Food Stores', 'Food and Beverage Stores', 'Beer, Wine, and Liquor Stores']
other_errands = ['Drycleaning and Laundry Services', 'Automotive Repair and Maintenance', 'Consumer Goods Rental', 'Personal and Household Goods Repair and Maintenance', 'Postal Service']
entertainment = ['Museums, Historical Sites, and Similar Institutions', 'Amusement Parks and Arcades', 'Other Amusement and Recreation Industries', 'Motion Picture and Video Industries', 'Performing Arts Companies', 'Arts, Entertainment, and Recreation']



################################################################################
# functions
################################################################################

def categorize_top_category(row):
    top_category = row['TOP_CATEGORY']
    sub_category = row['SUB_CATEGORY']

    if sub_category in healthcare_pharmacies:
        return 'Pharmacies'
    elif sub_category in fitness_wellness:
        return 'Fitness'
    elif top_category in healthcare_pharmacies:
        return 'Pharmacies'
    elif top_category in beauty:
        return 'Beauty'
    elif top_category in restaurants_cafes:
        return 'Restaurants'
    elif top_category in retail:
        return 'Retail'
    elif top_category in grocery:
        return 'Grocery'
    elif top_category in entertainment:
        return 'Entertainment'
    elif top_category in other_errands:
        return 'Errands'

    else:
        return 'Other'
    



def format_poi_df(evcs_proj,sub_str):
    evcs_buffered=evcs_proj.copy()
    evcs_buffered["geometry"] = evcs_buffered.buffer(max_distance_POI_to_EVCS+50)
    
    
    #output one: poi points that intersect with evcs
    try:
        poi=pd.read_csv(root+f'data/POI_data/POI_{current_city}_approved.csv.gz',index_col=0)
    except:
        poi=pd.read_csv(root+f'data/POI_data/POI_{current_city}_approved.csv',index_col=0)
    
    poi.drop(columns=['geometry'],inplace=True)#'Unnamed: 0',
    poi.rename(columns={'LATITUDE':'Latitude','LONGITUDE':'Longitude'},inplace=True)
    poi=poi[~poi.TOP_CATEGORY.isin(['Museums, Historical Sites, and Similar Institutions','Amusement Parks and Arcades'])] #remove parks and museums
    poi.drop(columns=['SAFEGRAPH_BRAND_IDS','REGION',
       'POSTAL_CODE', 'ISO_COUNTRY_CODE', 'PHONE_NUMBER',
       'BRANDS', 'STORE_ID','CATEGORY_TAGS', 'OPENED_ON', 'CLOSED_ON', 'TRACKING_CLOSED_SINCE'],
                     inplace=True)
    
    poi_proj=prepare_gdf(poi) #poi_points 
    joined_point_df = gpd.sjoin(poi_proj, evcs_buffered[['ID','geometry']], how="inner", predicate="intersects")
    joined_point_df=joined_point_df.drop_duplicates(subset='PLACEKEY')
    joined_point_df['Category'] = joined_point_df.apply(categorize_top_category, axis=1)
    joined_point_df=joined_point_df.reset_index()
    joined_point_df = joined_point_df.drop(columns=[c for c in joined_point_df.columns if c.startswith('index')], errors='ignore')
    joined_point_df=joined_point_df.to_crs('EPSG:4326')
    

    joined_point_df.to_csv(root+f'data/POI_data/POI_{current_city}_points.csv')    #output standardized points poi here 
    

    #output two: poi points that intersect with evcs
    poly_poi=pd.read_csv(root+f'data/POI_data/{current_city}_entertainment_poi_polygon.csv',index_col=0)
    poly_poi['geometry'] = poly_poi['POLYGON_WKT'].apply(wkt.loads)
    poly_poi_geo = gpd.GeoDataFrame(poly_poi, geometry='geometry', crs=4326)
    poly_poi_gdf=poly_poi_geo.to_crs('EPSG:3857')
    
    joined_poly_df = gpd.sjoin(poly_poi_gdf, evcs_buffered[['ID','geometry']], how="inner", predicate="intersects")
    joined_poly_df=joined_poly_df.drop_duplicates(subset='PLACEKEY')
    joined_poly_df['Category'] = joined_poly_df.apply(categorize_top_category, axis=1)
    joined_poly_df=joined_poly_df.reset_index()
    joined_poly_df = joined_poly_df.drop(columns=[c for c in joined_poly_df.columns if c.startswith('index')],errors='ignore')
    joined_poly_df=joined_poly_df.to_crs('EPSG:4326')
    joined_poly_df.to_csv(root+f'data/POI_data/POI_{current_city}_polygons.csv')    #output standardized points poi here 
   

    
    #create combined gdf 

    poi_comb=pd.concat([joined_point_df,joined_poly_df],axis=0)
    poi_comb=poi_comb[['PLACEKEY', 'LOCATION_NAME',  'TOP_CATEGORY', 'SUB_CATEGORY','Category',
       'NAICS_CODE', 'Latitude', 'Longitude', 'STREET_ADDRESS', 'CITY',
       # 'REGION', 'POSTAL_CODE', 'OPEN_HOURS',  'OPENED_ON','WKT_AREA_SQ_METERS',
       # 'CLOSED_ON', 'TRACKING_CLOSED_SINCE',
        'MSA', 'geometry','ID']].copy()
    poi_comb=poi_comb[poi_comb.Category!='Other'].copy() #added 3/19
    poi_comb.to_csv(root+f'data/POI_data/{sub_str}{current_city}_POIs_by_EVCS_{max_distance_POI_to_EVCS+50}m.csv')

    

    evcs_buffered=evcs_buffered.to_crs('EPSG:4326')
    joined_df = gpd.sjoin(poi_comb[['PLACEKEY', 'LOCATION_NAME',  'TOP_CATEGORY', 'SUB_CATEGORY','Category',
       'NAICS_CODE', 'Latitude', 'Longitude','geometry']],evcs_buffered[['ID','geometry']], how="inner", predicate="intersects")
    # joined_df=joined_df.drop_duplicates(subset='PLACEKEY')
    #joined_df.drop(columns=['index_right','GEOMETRY_TYPE','DOMAINS','WEBSITE'],inplace=True)
    # joined_df=joined_df.to_crs('EPSG:4326')
    
    return joined_df

def create_evcs_poi_df(evcs_proj,sub_str):
    


    # evcs_buffered=evcs_proj.copy()
    # evcs_buffered["geometry"] = evcs_buffered.buffer(max_distance_POI_to_EVCS+50)
    joined_df=format_poi_df(evcs_proj,sub_str)

#     joined_df = gpd.sjoin(poi_proj, evcs_buffered[['ID','geometry']], how="inner", predicate="within")

#     joined_df.drop(columns=['index_right','GEOMETRY_TYPE','DOMAINS','WEBSITE'],inplace=True)

    # create the 'Category' column
    joined_df['Category'] = joined_df.apply(categorize_top_category, axis=1)
    joined_df['Fitness'] = (joined_df['Category'] == 'Fitness').astype(int)
    joined_df['Pharmacies'] = (joined_df['Category'] == 'Pharmacies').astype(int)
    joined_df['Restaurants'] = (joined_df['Category'] == 'Restaurants').astype(int)
    joined_df['Retail'] = (joined_df['Category'] == 'Retail').astype(int)
    joined_df['Grocery'] = (joined_df['Category'] == 'Grocery').astype(int)
    joined_df['Beauty'] = (joined_df['Category'] == 'Beauty').astype(int)
    joined_df['Errands'] = (joined_df['Category'] == 'Errands').astype(int)
    joined_df['Entertainment'] = (joined_df['Category'] == 'Entertainment').astype(int)
    
    
    evcs_category_counts=joined_df.groupby('ID').agg({'Fitness':'sum', 'Pharmacies':'sum', 'Restaurants':'sum', 'Retail':'sum', 'Grocery':'sum', 'Beauty':'sum', 'Errands':'sum', 'Entertainment':'sum'})
    # evcs_category_counts.to_csv(root+f'data/choice_model/evcs_poi_counts_{max_distance_POI_to_EVCS+50}m.csv')
    evcs_category_counts.to_csv(root+f'data/POI_data/{current_city}_evcs_poi_counts_{max_distance_POI_to_EVCS+50}m.csv')

    filtered_df = joined_df[(joined_df[['Fitness', 'Pharmacies', 'Restaurants', 'Retail', 'Grocery', 'Beauty', 'Errands', 'Entertainment']] > 0).any(axis=1)]
    filtered_df=filtered_df.reset_index()
    


    return filtered_df







def find_pois_kdtree_then_sjoin(df_points, poi_points_gdf, poi_polygons_gdf,
                                max_distance=50):
    """
    Two-tier POI mapping:
    1) KDTree nearest-neighbor to point POIs within max_distance
    2) For points with no match, spatially join to polygon POIs
    """
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

    matched_mask = (distances != np.inf) & (idxs < len(poi_points))

    gdf.loc[matched_mask, "placekey"] = poi_points.iloc[idxs[matched_mask]]["PLACEKEY"].values
    gdf.loc[matched_mask, "category"] = poi_points.iloc[idxs[matched_mask]]["Category"].values
    gdf.loc[matched_mask, "name"]     = poi_points.iloc[idxs[matched_mask]]["LOCATION_NAME"].values

    # =====================================================
    # =======  TIER 2: Spatial join for unmatched ==========
    # =====================================================

    unmatched = gdf[gdf["placekey"].isna()].copy()
    matched   = gdf[~gdf["placekey"].isna()].copy()


    if len(unmatched) > 0:
        
        unmatched_clean = unmatched.drop(columns=['placekey', 'category', 'name'])
        
        sjoined = gpd.sjoin(
            unmatched_clean,
            poi_polygons[['PLACEKEY', 'Category', 'LOCATION_NAME', 'geometry']],
            how='left',
            predicate='within'
        )

        #sjoined = sjoined.groupby(level=0).first() #removing multiple parks within EVCS range
        sjoined = sjoined[~sjoined.index.duplicated(keep='first')]


        sjoined.rename(columns={
            'PLACEKEY': 'placekey',
            'Category': 'category',
            'LOCATION_NAME': 'name'
        }, inplace=True)

        sjoined = sjoined.drop(columns=[c for c in sjoined.columns if c.startswith('index_')], errors='ignore')

        # fill missing polygon matches
        sjoined['category'] = sjoined['category'].fillna('no poi')



        if(len(matched))==0:
            final=sjoined.copy()
        else:
            final = pd.concat([matched,
                       sjoined],
                      axis=0,ignore_index=True)

    else:
        final = gdf.copy()


    
    
    return final





def add_cbg_col(charging_activity_df, cbg_gdf):
    # Reset index to ensure unique row identification
    df_reset = charging_activity_df.reset_index(drop=True)
    
    # Convert to GeoDataFrame
    charging_activity_gdf = gpd.GeoDataFrame(
        df_reset, 
        geometry=gpd.points_from_xy(df_reset.lng, df_reset.lat),
        crs=cbg_gdf.crs)
    
    # Apply spatial join with left join to keep all original rows
    result = gpd.sjoin(charging_activity_gdf, cbg_gdf[['GEOID', 'geometry']], 
                       how='left', predicate='within')
    
    print(f"Original rows: {len(df_reset)}, Result rows: {len(result)}")
    

    # Handle multiple matches by keeping first match
    if len(result) > len(df_reset):
        result = result.groupby(result.index).first()
    
    # Add GEOID to original dataframe
    df_reset['GEOID'] = result['GEOID'].values
    
    return df_reset

def process_day(date_str,count,point_poi_geo,poly_poi_geo,evcs_gdf,cbg_gdf,ev_driver_ls,sub_str):
    evcs_session_df=pd.read_csv(current_session_path + f"evcs_sessions/{sub_str}evcs_session_duration_{current_city}_{date_str}.csv",index_col=0)
    df_stops = pd.read_csv(f"{data_path}raw/stop/stop_data_{current_city}_{date_str}.csv.gz")


    charging_activity_df=pd.DataFrame(columns=['session_id','cuebiq_id','stop_zoned_datetime', 'dwell_time_minutes','lat', 'lng','evcs_dist','T_since_charge',
       'classification_type','evcs_id','placekey','category','name'])
    
    
    
    print('# EVCS sessions:',len(evcs_session_df))
    
    #evcs_session_df.set_index('cuebiq_id',inplace=True)
    evcs_stops_df = evcs_session_df[evcs_session_df.cuebiq_id.isin(ev_driver_ls)].copy()
    print('# EVCS sessions (with ev driver filter):', len(evcs_stops_df))

    evcs_stops_df['timestamp'] = pd.to_datetime(evcs_stops_df['timestamp'])

    df_stops['stop_zoned_datetime'] = (
        pd.to_datetime(df_stops.stop_zoned_datetime, utc=True)
          .dt.tz_convert(cities_tz[current_city])
    )

    evcs_stops_df = prepare_gdf(evcs_stops_df, proj=False)
    df_stops = prepare_gdf(df_stops, proj=False)
    evcs_proj = prepare_gdf(evcs_gdf)

    count = 0

    for _, row in evcs_stops_df.iterrows():

        uid = row['cuebiq_id']
        ts = row['timestamp']
        ts_lb = ts

        # duration lookup (now filtered by cuebiq_id)
        dur_series = evcs_session_df.loc[
            evcs_session_df['cuebiq_id'] == uid, 'duration_LB'
        ]

        if dur_series.empty or dur_series.isna().all():
            continue

        dur = int(dur_series.iloc[0])
        ts_ub = ts + timedelta(minutes=dur)

        # subset stops
        df_stops_iter = df_stops[
            (df_stops['cuebiq_id'] == uid) &
            (df_stops.stop_zoned_datetime >= ts_lb) &
            (df_stops.stop_zoned_datetime <= ts_ub)
        ].copy()

        evcs_lat = evcs_gdf.loc[
            evcs_gdf['ID'] == row['evcs_id'], 'Latitude'
        ].values[0]

        evcs_lng = evcs_gdf.loc[
            evcs_gdf['ID'] == row['evcs_id'], 'Longitude'
        ].values[0]

        curr_date_str = ts.strftime('%Y%m%d')

        if df_stops_iter.empty:
            new_row = pd.DataFrame({
                'cuebiq_id': [uid],
                'session_id': [f'{uid}_{curr_date_str}'],
                'stop_zoned_datetime': [ts],
                'evcs_dist': [np.nan],
                'lat': [evcs_lat],
                'lng': [evcs_lng],
                'evcs_id': [row['evcs_id']],
                'placekey': [np.nan],
                'category': ['no stop'],
                'name': [np.nan],
                'T_since_charge': [np.nan],
                'dwell_time_minutes': [np.nan],
                'classification_type': [np.nan],
            })

            new_row = new_row[charging_activity_df.columns]
            charging_activity_df = pd.concat(
                [charging_activity_df, new_row],
                ignore_index=True
            )

            count += 1
            continue

        # session metadata
        df_stops_iter['session_id'] = f'{uid}_{curr_date_str}'
        df_stops_iter['evcs_id'] = row['evcs_id']
        df_stops_iter['T_since_charge'] = (
            (df_stops_iter.stop_zoned_datetime - ts_lb)
            .dt.total_seconds() / 60
        )

        dist = haversine(
            df_stops_iter.lat,
            df_stops_iter.lng,
            evcs_lat,
            evcs_lng
        )

        df_stops_iter['evcs_dist'] = dist

        df_stops_iter = find_pois_kdtree_then_sjoin(
            df_stops_iter,
            point_poi_geo,
            poly_poi_geo,
            max_distance=50
        )

        df_stops_iter.loc[
            df_stops_iter.evcs_dist < max_distance_ping_to_EVCS,
            ['placekey', 'category', 'name']
        ] = [np.nan, 'evcs', row['evcs_id']]

        charging_activity_df = pd.concat(
            [charging_activity_df, df_stops_iter[charging_activity_df.columns]],
            ignore_index=True
        )

    print('Num stop records recorded:', len(charging_activity_df))
    print('Num EVCS sessions with no corresponding stop data:', count)

    charging_activity_df = add_cbg_col(charging_activity_df, cbg_gdf)
    charging_activity_df=charging_activity_df[charging_activity_df.classification_type!='RECURRING_AREA'].copy()
    #charging_activity_df.to_csv(current_session_path+f'evcs_session_stops/heuristics/{sub_str}driver_EVCS_behavior_{current_city}_{date_str}.csv')   
    charging_activity_df.to_csv(current_session_path+f'evcs_session_stops/model/{sub_str}driver_EVCS_behavior_{current_city}_{date_str}.csv')
    return charging_activity_df


def process_evcs_session_stops(start_date, end_date,cbg_gdf, save_path, min_daily_pings, max_distance_ping_to_EVCS,complete_info=False):
    """
    Process the data that is saved by processing date to be by event date, for all counties. 
    start_date should be string of format "20220130"
    end_date should be string of format "20220130"
    """
    count=0
    
    if complete_info:
        print('Processing EVCS Complete Info Subset')
        sub_str='choice_model_subset/'
        evcs_df=pd.read_csv(root+f"data/evcs_locations/{current_city}/evcs_combined_complete_info_{max_distance_combine_stations}m_buffer.csv")
        evcs_gdf=prepare_gdf(evcs_df,proj=False)

    else:
        sub_str=''
        evcs_df=pd.read_csv(root+f'data/evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv')
        evcs_gdf=prepare_gdf(evcs_df,proj=False)
        
    evcs_f_exists=os.path.isfile(root+f'data/POI_data/{sub_str}{current_city}_POIs_by_EVCS_{max_distance_POI_to_EVCS+50}m.csv')
    poi_f_exists=os.path.isfile(root+f'data/POI_data/POI_{current_city}_points.csv')

    if overwrite or not (poi_f_exists and evcs_f_exists):
        print('Generating POI files')
        evcs_proj = prepare_gdf(evcs_gdf)
        create_evcs_poi_df(evcs_proj,sub_str)#generates missing files 
        
        
    point_poi_df=pd.read_csv(root+f'data/POI_data/POI_{current_city}_points.csv')
    point_poi_df['geometry'] = point_poi_df['geometry'].apply(wkt.loads)
    point_poi_geo = gpd.GeoDataFrame(point_poi_df, geometry='geometry', crs="EPSG:4326") 
    
    poly_poi_df=pd.read_csv(root+f'data/POI_data/POI_{current_city}_polygons.csv')  
    poly_poi_df['geometry'] = poly_poi_df['geometry'].apply(wkt.loads)
    poly_poi_geo = gpd.GeoDataFrame(poly_poi_df, geometry='geometry', crs="EPSG:4326") 
                                   

    # convert inputs to datetime
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
        
        # check if date already exists, in which case, next date
        f_exists = os.path.isfile(current_session_path+f'evcs_session_stops/model/{sub_str}driver_EVCS_behavior_{current_city}_{date_str}.csv')
        if f_exists and not overwrite: 
            print(f"\t * Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        
        # get EV driver list
        with open(current_session_path+f'ev_driver_info/user_set_{current_city}_model.txt', "r") as file:
            set_string = file.read() # Read the string from the file
        ev_driver_ls = list(ast.literal_eval(set_string)) # Convert the string back to a set

        # CENTRAL ACTION: identify sessions
        process_day(date_str, count, point_poi_geo,poly_poi_geo, evcs_gdf, cbg_gdf, ev_driver_ls, sub_str)
        count+=1
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)
       


 ################################################################################
# run
################################################################################

cbg_gdf=gpd.read_file(root+f'data/geo_files/{current_city}/studyarea_cbg.shp')


crosswalk = pd.read_csv(root+'/data/census/df_bg2010_to_bg2020_maj_area.csv') #expanded this from CA to US 
crosswalk.loc[:,'bg2020ge'] = crosswalk.bg2020ge.astype(str).str.zfill(12)
crosswalk.loc[:,'bg2010ge'] = crosswalk.bg2010ge.astype(str).str.zfill(12)
mapping_dict = dict(zip(crosswalk['bg2010ge'], crosswalk['bg2020ge']))






process_evcs_session_stops(start_date, end_date,cbg_gdf, data_path, min_daily_pings, max_distance_ping_to_EVCS,complete_info=False)
