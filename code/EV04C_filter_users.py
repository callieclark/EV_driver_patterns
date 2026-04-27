################################################################################
# Create user subset  and pull home and work location data from server
# Created on: 04/01/2025
# Created by: Callie and Anne
#This script creates daily ping data filtered by users and a total user subset log
################################################################################


################################################################################
# prep
################################################################################

exec(open('EV00_settings.py').read())

#%load_ext cuebiqmagic.magics
#%init_cuebiq_data
#snow_engine = get_ipython().user_ns['instance']

from shapely.geometry import MultiPolygon

################################################################################
# functions
################################################################################
def add_time_diff(df, time_col='event_zoned_datetime', id_col='cuebiq_id'):
    """
    Add a 'time_diff' column (in minutes) per user.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain the user ID and timestamp columns.
    time_col : str
        Name of the timestamp column.
    id_col : str
        Name of the user ID column.

    Returns
    -------
    pd.DataFrame
        Original df with new 'time_diff' column (deconds).
    """

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors='coerce', utc=True)
    
    # Sort by user and time
    df = df.sort_values([id_col, time_col])

    # Compute time difference per user in minutes
    df['time_diff_s'] = df.groupby(id_col)[time_col].diff().dt.total_seconds() 

    # Optionally, fill first observation per user with 0
    df['time_diff_s'] = df['time_diff_s'].fillna(0)

    return df



def process_day(date_str,month):
    """        
    For a specific day, reads in all files, filters users by minimum daily pings, saves filtered df
    """
    
    # start timer
    date_start = time.time()
    
    # read in raw ping data for the date
    pings = pd.read_parquet(data_path+f'raw/ping/event_date/ping_data_{current_city}_{date_str}.parquet')
    print(f"\t * Read in data by {round((time.time()-date_start)/60, 2)} minutes")
    if debug: print(pings.head())
    
        
    home_work_locations = pd.read_csv(data_path + f"ping{min_daily_pings}/user_home_work/user_home_work_{current_city}.csv")


    # filter out users with home locations outside the study area
    pings_subset = pings[pings.cuebiq_id.isin(home_work_locations.cuebiq_id)]
    del pings
    del home_work_locations
    print(len(pings_subset))

    print(f"\t * Filtered users in study area by {round((time.time()-date_start)/60, 2)} minutes")
    
    # update or create the user subset
    subset_ids_day = list(pings_subset.cuebiq_id.values) 
    log_users(data_path+f'ping{min_daily_pings}/user_subset/user_set_{current_city}.txt', subset_ids_day)
    print(f"\t * Saved user IDs by {round((time.time()-date_start)/60, 2)} minutes")
    
    ## add time diff column 
    pings_subset=add_time_diff(pings_subset, time_col='event_zoned_datetime', id_col='cuebiq_id')
    
    #filter out data that has time diff <3s or speed 120kmh
    pings_subset_cleaned=pings_subset[(pings_subset['time_diff_s']>3)&(pings_subset['speed_kmh']<=120)]
    del pings_subset
    
    # save out filtered ping data
    file_name = data_path+f'ping{min_daily_pings}/user_subset/ping_data_{current_city}_{date_str}.parquet' 
    pings_subset_cleaned.to_parquet(file_name)
    
    # print info on date processing
    print(f"\t * Date {date_str} has {len(set(subset_ids_day)):,} unique users and {len(pings_subset_cleaned):,} user subset pings")
    
    

def filter_ping_data():
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
        month=current_date.month
        
        # prep date times
        date_str = str(current_date.strftime('%Y%m%d'))
        
        # loop through each of the counties to pull
        date_start = time.time()
        
        #check if date already exists, in which case, next date
        f_exists = os.path.isfile(data_path + f"ping{min_daily_pings}/user_subset/ping_data_{current_city}_{date_str}.parquet")
 
        if f_exists and not overwrite: 
            print(f"---------- Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
        
        # print alert of what day is being processed
        print(f"-----------------------------------------------------------------------------\nProcessing date: {date_str}")
        
        # CENTRAL ACTION: filter daily data by ping frequency
        process_day(date_str,month)
        
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
print(f"\t * Minimum number daily pings = {min_daily_pings}\n\n")
    

    
# read in study area
cbg_study_area = gpd.read_file(root+f'data/geo_files/{current_city}/studyarea_cbg.shp')


# create mapping dictionary for 2010 to 2020 block group mapping
crosswalk = pd.read_csv(root+'/data/census/df_bg2010_to_bg2020_maj_area.csv') #expanded this from CA to US 


crosswalk.loc[:,'bg2020ge'] = crosswalk.bg2020ge.astype(str).str.zfill(12)
crosswalk.loc[:,'bg2010ge'] = crosswalk.bg2010ge.astype(str).str.zfill(12)
mapping_dict = dict(zip(crosswalk['bg2010ge'], crosswalk['bg2020ge']))


# -------------------------------------------------------------------------------
# run this file
filter_ping_data()
