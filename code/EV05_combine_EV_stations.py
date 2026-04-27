################################################################################
# Pull stop data from server
# Created on: 03/27/2025
# Created by: Anne and Callie

#This script takes all EVCS geolocation sources and combines into one source of EVCS locations withhout duplicates, prioritizing the sources with more information if there are duplicates 
################################################################################


################################################################################
# prep
################################################################################

# bring in all settings
exec(open('EV00_settings.py').read())

from scipy.spatial import cKDTree


################################################################################
# functions
################################################################################

def make_gdf(df, geometry_col='geometry', crs="EPSG:4326"):
    """
    Convert a DataFrame to a GeoDataFrame based on geometry column or lat/long columns.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame
    geometry_col : str or list
        If str: Name of the geometry column
        If list: [longitude_col, latitude_col] names
    crs : str
        Coordinate reference system (default: "EPSG:4326" for WGS84)
        
    Returns:
    --------
    geopandas.GeoDataFrame
    """
    from shapely.geometry import Point
    import geopandas as gpd
    
    # Create a copy to avoid modifying the original dataframe
    df_copy = df.copy()
    
    if isinstance(geometry_col, list):  # Check if geometry_col is a list of [lon, lat]
        # Create Point geometries from longitude and latitude columns
        df_copy.loc[:, 'geometry'] = df_copy.apply(
            lambda row: Point(row[geometry_col[0]], row[geometry_col[1]]), 
            axis=1)
        # Create GeoDataFrame with the created geometry column
        gdf = gpd.GeoDataFrame(df_copy, geometry='geometry', crs=crs)
    else:
        try:
            # Try direct conversion to GeoDataFrame
            gdf = gpd.GeoDataFrame(df_copy, geometry=geometry_col, crs=crs)
        except TypeError:
            try:
                # If that fails, try parsing from WKT
                from shapely import wkt
                df_copy.loc[:, geometry_col] = df_copy[geometry_col].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df_copy, geometry=geometry_col, crs=crs)
            except Exception as e:
                raise ValueError(f"Failed to create GeoDataFrame: {str(e)}")
    
    return gdf

def format_afdc(df,gdf_study_area):
    afdc_gdf = gpd.GeoDataFrame(df, 
                            geometry=gpd.points_from_xy(afdc.Longitude, afdc.Latitude), 
                            crs="EPSG:4326")
    afdc_gdf=afdc_gdf.reset_index()
    afdc_gdf=afdc_gdf.sjoin(gdf_study_area,predicate='within')
    afdc_gdf.drop(columns=['index_right'],inplace=True)
    afdc_gdf.rename(columns={'ID':'ID_afdc'},inplace=True)
    afdc_gdf['Total_Ports']=afdc_gdf['EV Level1 EVSE Num'].fillna(0)+afdc_gdf['EV Level2 EVSE Num'].fillna(0)+afdc_gdf['EV DC Fast Count'].fillna(0)
    afdc_gdf.drop(columns=['Fuel Type Code'],inplace=True)
    afdc_proj=prepare_gdf(afdc_gdf)
    return afdc_gdf,afdc_proj

def format_open_charge(df,gdf_study_area):
    open_charge_gdf = gpd.GeoDataFrame(df,
                                   geometry=gpd.points_from_xy(open_charge['Longitude'], open_charge['Latitude']),crs='EPSG:4326')
    
    open_charge_gdf=open_charge_gdf.sjoin(gdf_study_area,predicate='within')
    open_charge_gdf.drop(columns=['index_right'],inplace=True)
    open_charge_gdf.rename(columns={'Station ID':'ID_oc','Earliest Open Date': 'Open Date',
                                    'EV Level 2 EVSE Num':'EV Level2 EVSE Num','EV Level 1 EVSE Num': 'EV Level1 EVSE Num','Station Names':'Station Name'},inplace=True)

    open_charge_gdf['Total_Ports']=open_charge_gdf['EV Level1 EVSE Num'].fillna(0)+open_charge_gdf['EV Level2 EVSE Num'].fillna(0)+open_charge_gdf['EV DC Fast Count'].fillna(0)
    open_charge_proj=prepare_gdf(open_charge_gdf)
    return open_charge_gdf,open_charge_proj


def format_safegraph_evcs(df):
    safegraph_df=df[df.SUB_CATEGORY=='Other Gasoline Stations']
    safegraph_df=safegraph_df.rename(columns={'PLACEKEY':'ID_safegraph','LOCATION_NAME':'Station Name','LATITUDE':'Latitude', 'LONGITUDE':'Longitude'})
    safegraph_evcs=safegraph_df[['ID_safegraph','Station Name','geometry','Latitude', 'Longitude']]
    safegraph_gdf=make_gdf(safegraph_evcs, geometry_col='geometry', crs="EPSG:4326")
    safegraph_proj=prepare_gdf(safegraph_gdf)
    return safegraph_gdf,safegraph_proj

def df_project_buffer(df_proj):
    gdf_buffer=df_proj.copy()
    gdf_buffer['geometry'] = gdf_buffer.geometry.buffer(max_distance_combine_stations)  # Buffer by 5 meters
    return gdf_buffer


def filter_complete_info(evcs_gdf):

    evcs_gdf_filtered=evcs_gdf.copy()
    evcs_gdf_filtered=evcs_gdf_filtered[~(evcs_gdf_filtered['Groups With Access Code'].isin(['Private', 'Public - Card key at all times', 'Private - Government only', 'Private - Credit card at all times']))]
    evcs_gdf_filtered=evcs_gdf_filtered[evcs_gdf_filtered.ID_safegraph.isna()]
    
    evcs_gdf_filtered.reset_index(inplace=True)

   
    evcs_gdf_filtered=evcs_gdf_filtered[['ID_afdc','ID_oc','ID_safegraph', 'Station Name', 'Status Code', 'Open Date','EV Level1 EVSE Num', 'EV Level2 EVSE Num', 
        'EV DC Fast Count', 'EV Other Count','Total_Ports',
       'Connector Type', 'EV Workplace Charging', 'Facility Type',
       'EV Other Info', 'EV Network', 'Groups With Access Code',
       'Access Days Time', 'Latitude', 'Longitude', 'geometry']]
    evcs_gdf_filtered.index.rename('ID',inplace=True)
    evcs_gdf_filtered.to_csv(f"{root}data/evcs_locations/{current_city}/evcs_combined_complete_info_{max_distance_combine_stations}m_buffer.csv")
    return evcs_gdf_filtered

def remove_upleveled_interactions(evcs_gdf):
    evcs_proj=prepare_gdf(evcs_gdf, proj=True)
    start_dt = dt.strptime(start_date, '%Y%m%d')
    end_dt = dt.strptime(end_date, '%Y%m%d')
    evcs_near_upleveled=pd.DataFrame()
    # loop through days to pull individually
    current_date = start_dt
    while current_date <= end_dt:
       


        # prep date times
        date_str = str(current_date.strftime('%Y%m%d'))
        df=pd.read_csv(data_path+f'raw/stop/stop_data_{current_city}_{date_str}.csv.gz')
        df=df[df.transformation_type=='UPLEVELED'] #create df of all uplevelled points 
        df=df[['lat', 'lng','transformation_type','block_group_id']]
        evcs_near_upleveled=pd.concat([evcs_near_upleveled,df])
        current_date = current_date + timedelta(days = 1)


    evcs_near_upleveled_deduped=evcs_near_upleveled.drop_duplicates()
    #note that the lat,lng for each cbg is not constant--> there is noise!!

    gdf = prepare_gdf(evcs_near_upleveled_deduped, proj=True)

    gdf_buffered = gdf.copy()
    gdf_buffered['geometry'] = gdf_buffered.geometry.buffer(max_distance_upleveled_to_evcs)
    evcs_within_buffer = gpd.sjoin(evcs_proj, gdf_buffered, how="inner", predicate="intersects")
    print(f'Found {len(evcs_within_buffer.index)} intersecting with uplevelled points out of {len(evcs_gdf)}')
    return evcs_within_buffer.index
    

def combine_sources(afdc,open_charge,safegraph, gdf_study_area):
    #format csvs 
    afdc_gdf,afdc_proj=format_afdc(afdc,gdf_study_area)
    open_charge_gdf,open_charge_proj=format_open_charge(open_charge, gdf_study_area)
    safegraph_gdf,safegraph_proj=format_safegraph_evcs(safegraph)
    
    #buffer and join afdc and open charge 
    afdc_buffered=df_project_buffer(afdc_proj)
    gdf_intersect=afdc_buffered.sjoin(open_charge_proj,how='inner',predicate='intersects')
    open_charge_new=open_charge_gdf[~open_charge_gdf.ID_oc.isin(gdf_intersect.ID_oc.values)]
    afdc_oc_gdf=pd.concat([afdc_gdf,open_charge_new])
    
    #buffer combined afdc and open charge and join with safegraph
    afdc_oc_proj=prepare_gdf(afdc_oc_gdf)
    afdc_oc_buffered=df_project_buffer(afdc_oc_proj)
    gdf_intersect_2=afdc_oc_buffered.sjoin(safegraph_proj,how='inner',predicate='intersects')
    safegraph_new=safegraph_gdf[~safegraph_gdf.ID_safegraph.isin(gdf_intersect_2.ID_safegraph.values)]
    evcs_gdf=pd.concat([afdc_oc_gdf,safegraph_new])
    
    #create complete info subset
    #filter_complete_info(evcs_gdf) #only use for the choice model subset

    
    #evcs_gdf.drop(columns=['level_0', 'level_1', 'index'],inplace=True)

    
   
    evcs_gdf_out=evcs_gdf[['ID_afdc','ID_oc','ID_safegraph', 'Station Name', 'Status Code', 'Open Date',
       'EV Level1 EVSE Num', 'EV Level2 EVSE Num', 'EV DC Fast Count','EV Other Count','Total_Ports',
       'Connector Type', 'EV Workplace Charging', 'Facility Type',
       'EV Other Info', 'EV Network', 'Groups With Access Code',
       'Access Days Time', 'Latitude', 'Longitude', 'geometry']]
    evcs_gdf_out.reset_index(inplace=True,drop=True)
    evcs_gdf_out.index.rename('ID',inplace=True)

    
    evcs_IDs=remove_upleveled_interactions(evcs_gdf_out)
    evcs_gdf_out_=evcs_gdf_out[~evcs_gdf_out.index.isin(evcs_IDs)]


    evcs_gdf_out_.to_csv(f"{root}data/evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv")
    
    print(f"File saved to: {root}data/evcs_locations/{current_city}/evcs_combined.csv")
    
    return evcs_gdf_out_

def create_gas_station_csv(safegraph):
    safegraph_df=safegraph[safegraph.SUB_CATEGORY=='Gasoline Stations with Convenience Stores']
    safegraph_df=safegraph_df.rename(columns={'PLACEKEY':'ID_safegraph','LOCATION_NAME':'Station Name', 'LATITUDE':'Latitude', 'LONGITUDE':'Longitude'})
    safegraph_evcs=safegraph_df[['ID_safegraph','Station Name','geometry','Latitude', 'Longitude']].copy()
    safegraph_gdf=make_gdf(safegraph_evcs, geometry_col='geometry', crs="EPSG:4326")
    safegraph_gdf.set_index('ID_safegraph', inplace=True)
    gas_poi_ids=remove_upleveled_interactions(safegraph_gdf)
    safegraph_gdf_=safegraph_gdf[~safegraph_gdf.index.isin(gas_poi_ids)]
    safegraph_gdf_.to_csv(root+f'data/evcs_locations/{current_city}/POI_gas_stations.csv')

################################################################################
# run
################################################################################

# read in data
open_charge = pd.read_csv(root+f"data/evcs_locations/{current_city}/opencharge_approved.csv",index_col=0)
afdc = pd.read_csv(root+f'data/evcs_locations/{current_city}/AFDC_evcs_approved.csv',index_col=0)
safegraph =pd.read_csv(root+f'data/evcs_locations/{current_city}/POI_{current_city}_EVCS_Gas_cleaned.csv',index_col=0)

#check for it if not create from union of shapefiles
# f_exists = os.path.isfile(root+f'data/geo_files/{current_city}/Study_Area_geo.shp')
# if f_exists and not overwrite: 
gdf_study_area=gpd.read_file(root+f'data/geo_files/{current_city}/Study_Area_geo.shp')
# else:
#     gdf_study_area=county_shp[county_shp.GEOID.isin(cities_fips[current_city])].dissolve()[['geometry']].to_crs(4326)
#     gdf_study_area.to_file(root+f'data/geo_files/{current_city}/Study_Area_geo.shp')

            
            
    

combine_sources(afdc, open_charge, safegraph, gdf_study_area)
create_gas_station_csv(safegraph)

