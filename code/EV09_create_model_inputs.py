
###############################################################################
# Created on: 04/07/2025
# Created by: Anne and Callie

# This script creates information to construct model features. ie Get information we can use to help identify which users are likely EV drivers

################################################################################


################################################################################
# prep
################################################################################

exec(open('EV00_settings.py').read())
import censusdata
from dateutil.rrule import rrule, MONTHLY, DAILY
import calendar
import math

################################################################################
# functions
################################################################################
import pandas as pd

   
    
def munge_columns(df, spatial_col):
    # race + ethnicity
    df['perc_white'] = df['race_white']/df['pop']
    df['perc_black'] = df['race_black']/df['pop']
    df['perc_asian'] = df['race_asian']/df['pop']
    df['perc_native'] = df['race_native']/df['pop']
    df['perc_hisp'] = df['hispanic']/df['pop']

    # housing
    df['perc_owners'] = df['tenure_own']/df['tenure_all']
    df['perc_renters'] = df['tenure_rent']/df['tenure_all']
    df['perc_sfh'] = df['structures_sfh']/df['structures_all']
    df['perc_mfh'] = (df['structures_2'] + df['structures_3_4'] + df['structures_5_9'] + df['structures_10_19'] + df['structures_20_49'] + df['structures_50'])/df['structures_all']
    df['perc_sfh_owners'] = df['tenure_structure_sfh_own']/df['tenure_structure_all']
    df['perc_sfh_renters'] = df['tenure_structure_sfh_rent']/df['tenure_structure_all']
    df['perc_mfh_owners'] = (df['tenure_structure_2_own'] + df['tenure_structure_3_4_own'] + df['tenure_structure_5_9_own'] + df['tenure_structure_10_19_own'] + df['tenure_structure_20_49_own'] + df['tenure_structure_50_own'])/df['tenure_structure_all']
    df['perc_mfh_renters'] =(df['tenure_structure_2_rent'] + df['tenure_structure_3_4_rent'] + df['tenure_structure_5_9_rent'] + df['tenure_structure_10_19_rent'] + df['tenure_structure_20_49_rent'] + df['tenure_structure_50_rent'])/df['tenure_structure_all']
    
    # mobility
    df['perc_same_house'] = df['mobility_same']/df['mobility_all']
    df['perc_same_metro'] = df['perc_same_house'] + df['mobility_metro']/df['mobility_all']

    # other
    df['perc_no_car'] = (df['owner_no_car'] + df['renter_no_car'])/df['tenure_all']
    df['perc_food_stamps'] = df['food_stamps']/df['tenure_all']
    df['perc_poverty'] = df['income_poverty']/df['tenure_all']
    df['perc_home_2mil'] = df['home_2mil']/df['tenure_own']
    
    # density
    df['pop_sqkm'] = df['pop']/df['area']


    df = df[[spatial_col, 'pop', 'pop_sqkm', 
             'perc_white', 'perc_black','perc_native', 'perc_hisp', 'perc_asian',
             'perc_owners', 'perc_renters', 'perc_sfh', 'perc_mfh', 
             'perc_sfh_owners', 'perc_sfh_renters', 'perc_mfh_owners', 'perc_mfh_renters',
             'perc_same_house', 'perc_same_metro', 'perc_no_car', 
             'perc_food_stamps', 'perc_poverty', 'income_median', 'perc_home_2mil']]
    
    return df


def get_first(l):
    if len(l)>0:
        return l[0]
    return np.nan


def get_user_home_work(string_days):
    
    # relic from when we were using daily data
    # file_names = [data_path + f"ping{min_daily_pings}/user_home_work/user_home_work_{current_city}_{d}.csv" for d in string_days]
    # df_list = [pd.read_csv(file, dtype=str).drop_duplicates() for file in file_names]
    
    # read in the monthly data generated in 4B
    df = pd.read_csv(data_path + f"ping{min_daily_pings}/user_home_work/user_home_work_{current_city}.csv")

    #month_df = df.sort_values('confidence_level_H').groupby('cuebiq_id').nth(0).reset_index() 
    #df.cuebiq_id = df.cuebiq_id.astype('string')
    #month_df = pd.concat(df_list, ignore_index=True)

    return df
    
    
def get_driver_stops(string_days):
    
    # read in all the EVCS stops for the month
    file_names = [current_session_path + f"evcs_sessions/evcs_session_duration_{current_city}_{d}.csv" for d in string_days]
    df_list = [pd.read_csv(file,index_col=0) for file in file_names]
    month_df = pd.concat(df_list, ignore_index=True)
    
    # count the EVCS stops for each user for the month
    EVCS_stops = month_df[['cuebiq_id', 'lat']]\
        .groupby('cuebiq_id')\
        .count()\
        .reset_index()\
        .rename(columns={'lat': 'EVCS_stops'})#, 'Unnamed: 0': 'cuebiq_id'})
    
    # count how many unique stations each user went to
    EVCS_stations = month_df[['cuebiq_id', 'evcs_id', 'lat']]\
        .groupby(['cuebiq_id', 'evcs_id'])\
        .count()\
        .reset_index()\
        .rename(columns={'lat': 'EVCS_stations'})\
        .drop('evcs_id', axis=1)\
        .groupby('cuebiq_id')\
        .count()\
        .reset_index()
    
    # read in all the gas station stops for the month
    file_names = [data_path + f"ping{min_daily_pings}/gas_station_visits/gas_station_stops_{current_city}_{d}_{max_distance_stop_to_gas}m_{gas_station_dwell_time_max}min.csv" for d in string_days]
    df_list = [pd.read_csv(file) for file in file_names]
    month_df = pd.concat(df_list, ignore_index=True)
   
    # count the gas station stops for each user for the month 
    gas_stops = month_df[['cuebiq_id', 'dwell_time']]\
        .groupby('cuebiq_id')\
        .count()\
        .rename(columns={'dwell_time': 'gas_stops'})\
        .reset_index()
    
    # count how many unique stations each user went to
    gas_stations = month_df[['cuebiq_id', 'nearest_SG_ID', 'distance']]\
        .groupby(['cuebiq_id', 'nearest_SG_ID'])\
        .count()\
        .reset_index()\
        .rename(columns={'distance': 'gas_stations'})\
        .drop('nearest_SG_ID', axis=1)\
        .groupby('cuebiq_id')\
        .count()\
        .reset_index()
    
    # combine all the monthly info
    stops = EVCS_stops.merge(EVCS_stations, on='cuebiq_id', how='outer')
    stops = stops.merge(gas_stops, on='cuebiq_id', how='outer')
    stops = stops.merge(gas_stations, on='cuebiq_id', how='outer')
    #add in other
    stops = stops.fillna(0)
    
    return stops


def calculate_radius_of_gyration(df):
    
    # prep to calculate gyration
    latitudes = df.lat
    longitudes = df.lng
    times = df.dwell_time_minutes

    if len(latitudes) != len(longitudes):
            raise ValueError("Latitudes and longitudes must be equal in length.")
    if len(latitudes) < 2:
            raise ValueError("Latitudes and longitudes must contain at least two points.")

    center_lat = np.average(latitudes, weights=times)
    center_lon = np.average(longitudes, weights=times)

    # calculate
    distances = [haversine(center_lat, center_lon, lat, lon) for lat, lon in zip(latitudes, longitudes)]
    squared_distances = [d**2 for d in distances]
    time_squared_distances = times*squared_distances
    summation = np.sum(squared_distances)/np.sum(times)
    radius_of_gyration = np.sqrt(summation)
    
    return radius_of_gyration


def calculate_k_radius_of_gyration(df, k=2): 
    
    # get the location groupings and take top 2
    trunc = lambda x: math.trunc(1000 * x) / 1000;
    k_locations = df.copy()
    k_locations['lat_trunc'] = k_locations.lat.apply(trunc)
    k_locations['lng_trunc'] = k_locations.lng.apply(trunc)
    k_locations = k_locations\
        .groupby(['lat_trunc', 'lng_trunc'])\
        .sum().reset_index()\
        .sort_values('dwell_time_minutes', ascending=False)[0:k]
    k_locations = k_locations[['lat_trunc', 'lng_trunc']]

    # filter df to just those groupings
    df['lat_trunc'] = df.lat.apply(trunc)
    df['lng_trunc'] = df.lng.apply(trunc)
    df = df.merge(k_locations, on=['lat_trunc', 'lng_trunc'])

    # prep to calculate gyration
    latitudes = df.lat
    longitudes = df.lng
    times = df.dwell_time_minutes

    center_lat = np.average(latitudes, weights=times)
    center_lon = np.average(longitudes, weights=times)

    # calculate
    distances = [haversine(center_lat, center_lon, lat, lon) for lat, lon in zip(latitudes, longitudes)]
    squared_distances = [d**2 for d in distances]
    time_squared_distances = times*squared_distances
    summation = np.sum(squared_distances)/np.sum(times)
    radius_of_gyration = np.sqrt(summation)
    
    return radius_of_gyration


def process_radius_gyration(df):
    
    # check if either file is already calculated
    f_path = current_session_path + f'all_driver_info/all_driver_gyration_{current_city}_{month.year}_{month.month}.csv'
    f_exists = os.path.isfile(f_path)
    f2_path = current_session_path + f'all_driver_info/all_driver_gyration_k2_{current_city}_{month.year}_{month.month}.csv'
    f2_exists = os.path.isfile(f2_path)
    
    if f_exists and f2_exists and not overwrite: 
        print(f"\t\t * Both gyration files already processed, skipping at {cur_time_string()}")
        return
    
    # drop the stops that are duplicates
    df = df.sort_values(by=['cuebiq_id'])
    df = df[['cuebiq_id', 'lat', 'lng', 'dwell_time_minutes']].drop_duplicates()
    
    # only keep ids with more than 1 obs
    df = df.groupby('cuebiq_id').filter(lambda x: len(x) > 1)
    
    #calculate gyration
    if f_exists and not overwrite: 
        print(f"\t\t * Gyration already processed, skipping at {cur_time_string()}")
    if not f_exists or overwrite: 
        gyration = df.groupby('cuebiq_id').apply(calculate_radius_of_gyration)
        gyration = gyration.reset_index().rename(columns={0: 'radius_gyration'})
        gyration.to_csv(f_path)
        del gyration
    
    # calculate gyration for top 2 locations
    if f2_exists and not overwrite: 
        print(f"\t\t * K2 gyration already processed, skipping at {cur_time_string()}")
    if not f2_exists or overwrite: 
        gyration = df.groupby('cuebiq_id').apply(calculate_k_radius_of_gyration)
        gyration = gyration.reset_index().rename(columns={0: 'radius_gyration'})
        gyration.to_csv(f2_path)
        del gyration
        

def calc_vmt():
    
    # loop over all days and calculate aggregate vmt by id, save in a temp folder
    j = len(string_days)
    for i in range(0, j): 
        d = string_days[i]
        temp_f = current_session_path + f'all_driver_info/temp/vmt_{current_city}_{d}.parquet'
        temp_f_exists = os.path.isfile(temp_f)
        if not temp_f_exists or overwrite: 
            temp = pd.read_parquet(data_path + 
                                   f"ping{min_daily_pings}/user_subset/ping_data_{current_city}_{d}.parquet")
            temp = temp[['cuebiq_id', 'distance_m']].groupby('cuebiq_id').sum().reset_index()
            temp.to_parquet(temp_f, index=False)
            del temp
     
        
    # start combining with first day in month
    d = string_days[0]
    df = pd.read_parquet(current_session_path + f'all_driver_info/temp/vmt_{current_city}_{d}.parquet')

    # loop through the pre-aggregated in the temp folder to compile all days
    for i in range(1, j): 
        d = string_days[i]
        temp = pd.read_parquet(current_session_path + f'all_driver_info/temp/vmt_{current_city}_{d}.parquet')
        df = df.merge(temp, on = 'cuebiq_id', how = 'outer').fillna(0) # add 0 if they are NA for the day

        # add the vmt for the new day, and pass the aggregate forward
        df['distance_m'] = df.distance_m_x + df.distance_m_y
        df = df[['cuebiq_id', 'distance_m']]
     
    # convert to miles
    df.distance_m = df.distance_m/1609 
    return df
    
    
    
################################################################################
# run - pull in and munge external data
################################################################################

#-------------------------------------------------------------------------------
# get EV ownership by zip code from CEC 
# (found at https://lab.data.ca.gov/dataset/vehicle-fuel-type-count-by-zip-code)
#-------------------------------------------------------------------------------

# check that ev data hasn't been calculated yet
f_path = root+f"data/census/ev_zip_percent_{current_city}.csv"
f_exists = os.path.isfile(f_path)

if f_exists and not overwrite: 
    print(f"--------------------------- ev ownership already processed, skipping at {cur_time_string()} ---------------------------")

# if not, process it 

if not f_exists:# or overwrite: 
    
    # load in data for CA ----------------------------------------------------------------------
    ev_ownership = pd.read_csv("https://data.ca.gov/datastore/dump/9aa5b4c5-252c-4d68-b1be-ffe19a2f1d26")

    # remove trucks that are included
    ev_ownership = ev_ownership[ev_ownership.Duty != "Heavy"]

    # aggregate cars and evs to zip level
    all_vehicles = ev_ownership\
        .groupby('Zip Code')\
        .agg({'Vehicles': 'sum'})\
        .reset_index()\
        .rename(columns={"Vehicles": "veh_count", "Zip Code": "zip"})\
        .reset_index()
    ev_vehicles = ev_ownership[ev_ownership.Fuel == "Battery Electric"] #add PHEV 
    ev_vehicles = ev_vehicles\
        .groupby('Zip Code')\
        .agg({'Vehicles': 'sum'})\
        .reset_index()\
        .rename(columns={"Vehicles": "ev_count", "Zip Code": "zip"})\
        .reset_index()

    # munge to be a % 
    ev_vehicles = ev_vehicles.merge(all_vehicles, on = "zip")
    ev_vehicles['perc_ev'] = ev_vehicles.ev_count/ev_vehicles.veh_count
    ca_ev_vehicles = ev_vehicles[['zip', 'perc_ev', "ev_count", "veh_count"]]
    ca_ev_vehicles = ca_ev_vehicles[~(ca_ev_vehicles.zip == 'OOS')]
    ca_ev_vehicles = ca_ev_vehicles[(ca_ev_vehicles.zip.astype(int) >= 90001) & (ca_ev_vehicles.zip.astype(int) <= 96162)]
    
    # export
    ca_ev_vehicles.to_csv(root+f"data/census/ev_zip_percent_Bay.csv", index=False)
    ca_ev_vehicles.to_csv(root+f"data/census/ev_zip_percent_SF.csv", index=False)
    print('CA finished')
    
    # read in data for seattle ----------------------------------------------------------------------
        # aggregate cars and evs to zip level
    all_vehicles = pd.read_json('https://data.wa.gov/resource/brw6-jymh.json?$query=SELECT%0A%20%20%60zip_code%60%2C%0A%20%20count(%60electrification_level%60)%20AS%20%60count_electrification_level%60%0AWHERE%0A%20%20%60start_of_month%60%0A%20%20%20%20BETWEEN%20%222021-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222022-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20AND%20caseless_one_of(%60county%60%2C%20%22King%22%2C%20%22Snohomish%22%2C%20%22Pierce%22)%0A%20%20AND%20caseless_one_of(%60vehicle_primary_use%60%2C%20%22Passenger%20Vehicle%22)%0AGROUP%20BY%20%60zip_code%60') 
    ev_vehicles = pd.read_json('https://data.wa.gov/resource/brw6-jymh.json?$query=SELECT%0A%20%20%60zip_code%60%2C%0A%20%20count(%60electrification_level%60)%20AS%20%60count_electrification_level%60%0AWHERE%0A%20%20%60start_of_month%60%0A%20%20%20%20BETWEEN%20%222021-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222022-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20AND%20caseless_one_of(%60county%60%2C%20%22King%22%2C%20%22Snohomish%22%2C%20%22Pierce%22)%0A%20%20AND%20caseless_one_of(%60vehicle_primary_use%60%2C%20%22Passenger%20Vehicle%22)%0A%20%20AND%20caseless_one_of(%0A%20%20%20%20%60electrification_level%60%2C%0A%20%20%20%20%22BEV%20(Battery%20Electric%20Vehicle)%22%0A%20%20)%0AGROUP%20BY%20%60zip_code%60')
    print('WA loaded')

    # merging
    all_vehicles.zip_code = all_vehicles.zip_code.astype('string')
    ev_vehicles.zip_code = ev_vehicles.zip_code.astype('string')
    ev_vehicles = all_vehicles.merge(ev_vehicles, on = "zip_code")
    
    # munge to be a % 
    ev_vehicles['perc_ev'] = ev_vehicles.count_electrification_level_y/ev_vehicles.count_electrification_level_x
    ev_vehicles = ev_vehicles.rename(columns = {'zip_code': 'zip'})
    ev_vehicles = ev_vehicles.rename(columns = {'zip_code': 'zip', 
                                                'count_electrification_level_y': 'ev_count', 
                                                'count_electrification_level_x': 'veh_count'})
    wa_ev_vehicles = ev_vehicles[['zip', 'perc_ev', "ev_count", "veh_count"]]
    
    # export
    wa_ev_vehicles.to_csv(root+f"data/census/ev_zip_percent_Seattle.csv", index=False)
    print('WA finished')


#-------------------------------------------------------------------------------
# get census demographics
# variable list may help: https://downloads.esri.com/esri_content_doc/dbl/us/Var_List_ACS_June_2022.pdf
#-------------------------------------------------------------------------------


# check that census data hasn't been calculated yet
f_path = root+f"data/census/demographics_zip_{current_city}.csv"

f_exists = os.path.isfile(f_path)
f_exists=False
if f_exists and not overwrite: 
    print(f"--------------------------- census data already processed, skipping at {cur_time_string()} ---------------------------")

# if not, process it 
if not f_exists or overwrite:    
    # get the areas for each cbg
    areas = gpd.read_file(root + f"data/geo_files/{current_city}/studyarea_cbg.shp")
    areas['area'] = areas['geometry'].to_crs({'proj':'cea'}) .area/(1.609344e6)
    areas.head()

    # pull data from census
    # TODO: need to set up so it pulls the right data based on the city you're looking at 
    demographics = censusdata.download('acs5', 2022,
                                 censusdata.censusgeo([('state', cities_fips[current_city][0][0:2]),#added to switch with state
                                                       ('county', '*'), ('block group', '*')]),
                                 ['B01003_001E', # total pop
                                  'B25003_001E', # tenure = all
                                  'B25003_002E', # tenure = own
                                  'B25003_003E', # tenure = rent 
                                  'B25044_003E', # owner, no vehicle available
                                  'B25044_010E', # renter, no vehicle available
                                  'B25024_001E', # structures = all
                                  'B25024_002E', # structures = SFH
                                  'B25024_003E', # structures = attached 1 unit
                                  'B25024_004E', # structures = 2 unit
                                  'B25024_005E', # structures = 3-4 unit
                                  'B25024_006E', # structures = 5-9 unit
                                  'B25024_007E', # structures = 10-19 unit
                                  'B25024_008E', # structures = 20-49 unit
                                  'B25024_009E', # structures = 50+ unit

                                  'B25032_001E', # tenure structures = all
                                  'B25032_003E', # tenure structures = sfh_own
                                  'B25032_014E', # tenure structures = sfh_rent

                                  'B25032_005E', # tenure structures = 2_own
                                  'B25032_006E', # tenure structures = 3_4_own
                                  'B25032_007E', # tenure structures = 5_9_own
                                  'B25032_008E', # tenure structures = 10_19_own
                                  'B25032_009E', # tenure structures = 20_49_own
                                  'B25032_010E', # tenure structures = 50_own

                                  'B25032_016E', # tenure structures = 2_rent
                                  'B25032_017E', # tenure structures = 3_4_rent
                                  'B25032_018E', # tenure structures = 5_9_rent
                                  'B25032_019E', # tenure structures = 10_19_rent
                                  'B25032_020E', # tenure structures = 20_49_rent
                                  'B25032_021E', # tenure structures = 50_rent

                                  'B02001_001E', # race = all
                                  'B02001_002E', # race = white
                                  'B02001_003E', # race = black
                                  'B02001_005E', # race = asian
                                  'B02001_004E', # race = native
                                  'B03002_012E', # hispanic

                                  'B07201_001E', # mobility = all
                                  'B07201_002E', # mobility = same house 1 year ago
                                  'B07201_004E', # mobility = diff house, same metro 1 year ago

                                  'B19013_001E', # median income
                                  'B22010_002E', # food stamps
                                  'B17017_002E', # households below poverty line
                                  'B25075_027E', # home > 2 mil
                                  'B25077_001E' # median home val
                                 ])
    demographics.rename(columns={'B01003_001E': 'pop', 

                                  'B25003_001E': 'tenure_all', 
                                  'B25003_002E': 'tenure_own', 
                                  'B25003_003E': 'tenure_rent', 

                                  'B25044_003E': 'owner_no_car', 
                                  'B25044_010E': 'renter_no_car', 

                                  'B25024_001E': 'structures_all', 
                                  'B25024_002E': 'structures_sfh', 
                                  'B25024_003E': 'structures_attached', 
                                  'B25024_004E': 'structures_2',
                                  'B25024_005E': 'structures_3_4',
                                  'B25024_006E': 'structures_5_9',
                                  'B25024_007E': 'structures_10_19',
                                  'B25024_008E': 'structures_20_49',
                                  'B25024_009E': 'structures_50',

                                  'B25032_001E': 'tenure_structure_all',
                                  'B25032_003E': 'tenure_structure_sfh_own',
                                  'B25032_014E': 'tenure_structure_sfh_rent',

                                  'B25032_005E': 'tenure_structure_2_own',
                                  'B25032_006E': 'tenure_structure_3_4_own',
                                  'B25032_007E': 'tenure_structure_5_9_own',
                                  'B25032_008E': 'tenure_structure_10_19_own',
                                  'B25032_009E': 'tenure_structure_20_49_own',
                                  'B25032_010E': 'tenure_structure_50_own',

                                  'B25032_016E': 'tenure_structure_2_rent',
                                  'B25032_017E': 'tenure_structure_3_4_rent',
                                  'B25032_018E': 'tenure_structure_5_9_rent',
                                  'B25032_019E': 'tenure_structure_10_19_rent',
                                  'B25032_020E': 'tenure_structure_20_49_rent',
                                  'B25032_021E': 'tenure_structure_50_rent',

                                  'B02001_001E': 'race_all', 
                                  'B02001_002E': 'race_white', 
                                  'B02001_003E': 'race_black', 
                                  'B02001_005E': 'race_asian', 
                                  'B02001_004E': 'race_native', 
                                  'B03002_012E': 'hispanic', 

                                  'B07201_001E': 'mobility_all',
                                  'B07201_002E': 'mobility_same', 
                                  'B07201_004E': 'mobility_metro', 

                                  'B19013_001E': 'income_median',
                                  'B17017_002E': 'income_poverty',
                                  'B22010_002E': 'food_stamps',
                                  'B25075_027E': 'home_2mil', 
                                  'B25077_001E': 'home_median'}, 
                       inplace = True)

    # deal with implicit NAs
    demographics = demographics.replace(-666666666, np.nan)
    demographics = demographics.replace(-999999999, np.nan)

    # add geo cols
    demographics['county_fips'] = [i.geo[0][1] + i.geo[1][1] for i in demographics.index.tolist()]
    demographics['bg_fips'] = [i.geo[0][1] + i.geo[1][1] + i.geo[2][1] + i.geo[3][1] for i in demographics.index.tolist()]

    # merge to area
    demographics = demographics.merge(areas[['GEOID', 'area']], left_on="bg_fips", right_on='GEOID').drop('GEOID', axis=1)

    demographics.reset_index(inplace = True)
    demographics = demographics.iloc[:, 1:] # drop column named index
    demographics.bg_fips = demographics.bg_fips.astype('string').str.zfill(12)
    demographics.to_csv(root+f"data/census/demographics_unmunged_test.csv", index=False)
    
    demographics=demographics[demographics['pop']>0]

    # normalize to percentages for cbg version
    cbg_demographics = munge_columns(demographics, 'bg_fips')
    cbg_demographics.to_csv(root+f"data/census/demographics_cbg_{current_city}.csv", index=False)

    
    crosswalk = pd.read_csv(zip_crosswalk, index_col=0)
    crosswalk.cbg=crosswalk.cbg.astype('string').str.zfill(12)
    crosswalk.GEOID_ZCTA5_20=crosswalk.GEOID_ZCTA5_20.astype('string').str.zfill(5)
    crosswalk=crosswalk.rename(columns={'GEOID_ZCTA5_20':'zcta','cbg':'bg_fips'})

    #prep was for previoius CS only version- confirm with Anne that my version is fine 
    # # prep crosswalk
    crosswalk = crosswalk[crosswalk['AREALAND_PART']>0] 

    # crosswalk to zip
    #zip_demographics = demographics.merge(crosswalk, on = 'bg_fips')
    zip_demographics = demographics.merge(crosswalk, on='bg_fips', how='left')

    # Check for missing ZIP mappings 
    if debug: 
        print(zip_demographics[zip_demographics['zcta'].isna()])
        missing_zips = zip_demographics['zcta'].isna().sum()
        print(f"CBGs without ZIP mappings: {missing_zips}")

    # Filter to only CBGs with ZIP mappings
    zip_demographics = zip_demographics[zip_demographics['zcta'].notna()]
    if debug: print(zip_demographics)

    # weight by how much of cbg is in zip
    cols = ['pop', 'tenure_all', 'tenure_own', 'tenure_rent', 'owner_no_car',
       'renter_no_car', 'structures_all', 'structures_sfh',
       'structures_attached', 'structures_2', 'structures_3_4',
       'structures_5_9', 'structures_10_19', 'structures_20_49',
       'structures_50', 'tenure_structure_all', 'tenure_structure_sfh_own',
       'tenure_structure_sfh_rent', 'tenure_structure_2_own',
       'tenure_structure_3_4_own', 'tenure_structure_5_9_own',
       'tenure_structure_10_19_own', 'tenure_structure_20_49_own',
       'tenure_structure_50_own', 'tenure_structure_2_rent',
       'tenure_structure_3_4_rent', 'tenure_structure_5_9_rent',
       'tenure_structure_10_19_rent', 'tenure_structure_20_49_rent',
       'tenure_structure_50_rent', 'race_all', 'race_white', 'race_black',
       'race_asian', 'race_native', 'hispanic', 'mobility_all',
       'mobility_same', 'mobility_metro', 'income_median', 'food_stamps',
       'income_poverty', 'home_2mil', 'home_median', 'area']
    zip_demographics[cols] = zip_demographics[cols].apply(pd.to_numeric)
    if debug: print(zip_demographics)

    # aggregate to zip
    wm_pop = lambda x: np.average(np.ma.masked_array(x, np.isnan(x)), weights = zip_demographics.loc[x.index, "pop"])
    zip_demographics_wm = zip_demographics.groupby('zcta').agg(income_median = ("income_median", wm_pop), 
                                                               home_median = ('home_median', wm_pop))
    if debug: print(zip_demographics_wm)

    zip_demographics_sum = zip_demographics.groupby('zcta').sum()
    if debug: print(zip_demographics_sum)
    
    zip_demographics = zip_demographics_wm.merge(zip_demographics_sum.drop(columns=['income_median', 'home_median']), on = 'zcta').reset_index()
    if debug: print(zip_demographics)

    # normalize to percentages and save
    zip_demographics_processed = munge_columns(zip_demographics, 'zcta')    
    if debug: print(zip_demographics_processed)
    
    zip_demographics_processed.to_csv(root+f"data/census/demographics_zip_{current_city}.csv", index=False) 


    

################################################################################
# munge the EV behavior info and combine all info
################################################################################

# read in data that's time independent, demographics and ev ownership
ev_ownership_data = pd.read_csv(root+f"data/census/ev_zip_percent_{current_city}.csv")
ev_ownership_data = ev_ownership_data[ev_ownership_data['zip'].astype('string').str.isdigit()]
ev_ownership_data['zip'] = ev_ownership_data['zip'].astype(str)
#ev_ownership_data['zip'] = ev_ownership_data['zip'].astype(int)

cbg_demographics = pd.read_csv(root+f"data/census/demographics_cbg_{current_city}.csv")
cbg_demographics.bg_fips = cbg_demographics.bg_fips.astype('string').str.zfill(12)
zip_demographics = pd.read_csv(root+f"data/census/demographics_zip_{current_city}.csv")
zip_demographics.zcta = zip_demographics.zcta.astype('string').str.zfill(12) #new

# create mapping dictionary for 2010 to 2020 block group mapping
crosswalk = pd.read_csv(root+'/data/census/df_bg2010_to_bg2020_maj_area.csv') #expanded this from CA to US 
crosswalk.loc[:,'bg2020ge'] = crosswalk.bg2020ge.astype('string').str.zfill(12)
crosswalk.loc[:,'bg2010ge'] = crosswalk.bg2010ge.astype('string').str.zfill(12)
mapping_dict = dict(zip(crosswalk['bg2010ge'], crosswalk['bg2020ge']))
                               
# read in crosswalk from cbg to zip and get most likely zip for each cbg
crosswalk = pd.read_csv(zip_crosswalk, index_col=0)
crosswalk.cbg=crosswalk.cbg.astype('string').str.zfill(12)
crosswalk.GEOID_ZCTA5_20=crosswalk.GEOID_ZCTA5_20.astype('string').str.zfill(5)
crosswalk=crosswalk.rename(columns={'GEOID_ZCTA5_20':'zcta','cbg':'bg_fips'})
crosswalk = crosswalk[['bg_fips', 'zcta']]

# get months in our study period
start_dt = dt.strptime(start_date, "%Y%m%d")
end_dt = dt.strptime(end_date, "%Y%m%d")
months = [dt for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt)]

# loop over each month
for month in months:
    
    month_start = time.time()
    print(f"-----------------------------------------------------------------------------\nProcessing month: {month}")

    # check that month hasn't already been processed 
    f_path = current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_cbg_demographics.csv'
    f1_exists = os.path.isfile(f_path)    
    f_path = current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_zip_demographics.csv'
    f2_exists = os.path.isfile(f_path)
    if f1_exists and f2_exists and not overwrite: 
        print(f"\t * month already processed, skipping at {cur_time_string()}")
        continue
    
    # get days in month
    num_days = calendar.monthrange(month.year, month.month)[1]
    days = [date(month.year, month.month, day) for day in range(1, num_days+1)]
    string_days = [str(day.strftime('%Y%m%d')) for day in days]
    
    # calculate vmt using stop data
    f_path = current_session_path + f'all_driver_info/all_driver_vmt_{current_city}_{month.year}_{month.month}.csv'
    f_exists = os.path.isfile(f_path)
    if f_exists and not overwrite: 
        print(f"\t\t * vmt already processed, skipping at {cur_time_string()}")
    if not f_exists or overwrite: 
        print(f"\t * Started calculating vmt at {cur_time_string()}, takes ~10 minutes")
        vmt = calc_vmt()
        vmt.to_csv(f_path)
        del vmt
        print(f"\t * Finished calculating vmt at {cur_time_string()} by {cur_time_diff(month_start)} minutes")
    
    # read in stop data (for gyration, total stop count, etc)
    file_names = [data_path + f"raw/stop/stop_data_{current_city}_{d}.csv.gz" for d in string_days]
    month_stops = [pd.read_csv(file) for file in file_names]
    month_stops = pd.concat(month_stops, ignore_index=True)
    stops_appeared = month_stops.groupby('cuebiq_id')\
                                .agg({'lng': 'count'})\
                                .rename(columns={'lng': 'n_stops'}).reset_index()
    print(f"\t * Finished reading all monthly stops at {cur_time_string()} by {cur_time_diff(month_start)} minutes")

    
    # calculate gyration using stop data
    f_path = current_session_path + f'all_driver_info/all_driver_gyration_{current_city}_{month.year}_{month.month}.csv'
    f_exists = os.path.isfile(f_path)
    if f_exists and not overwrite: 
        print(f"\t\t * gyration already processed, skipping at {cur_time_string()}")
    if not f_exists or overwrite: 
        # calculate radius of gyration using stop data
        print(f"\t * Started calculating radius of gyration at {cur_time_string()}, takes ~20 minutes")
        process_radius_gyration(month_stops)
        print(f"\t * Finished calculating radius of gyration at {cur_time_string()} by {cur_time_diff(month_start)} minutes")

    # get the number of EVCS and gas station stops for each user
    stops = get_driver_stops(string_days)
    print(f"\t * Finished pulling driver stops at {cur_time_string()} by {cur_time_diff(month_start)} minutes")
    if debug: print(stops)
    if debug: print('\t\t stops df:', stops)
    
    
    # get the home and work cbg
    ids = stops.cuebiq_id
    home_work_cbg = get_user_home_work(string_days)
    home_work_cbg = home_work_cbg.dropna(subset='GEOID_H')
    home_work_cbg.cuebiq_id = home_work_cbg.cuebiq_id.apply(lambda x: str(int(float(x))) if pd.notnull(x) else pd.NA).astype("string")
    home_work_cbg.GEOID_H = home_work_cbg.GEOID_H.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")
    # home_work_cbg = home_work_cbg.groupby('cuebiq_id')[['GEOID_H', 'GEOID_W']].agg(lambda x: get_first(pd.Series.mode(x))).reset_index() #first if there r mult modes
    if debug: print(home_work_cbg)
    print(f"\t * Finished pulling home and work cbgs at {cur_time_string()} by {cur_time_diff(month_start)} minutes")

    #read in the user stats generated in 04A
    user_stats = pd.read_csv(data_path + f'raw/user_freq_stats_{current_city}_{month.month}.csv')
    user_stats.cuebiq_id = user_stats.cuebiq_id.astype('string')
    user_stats.rename(columns = {'Days': 'n_days'}, inplace = True)
    if debug: print(user_stats)
    print(f"\t * Finished pulling user stats at {cur_time_string()} by {cur_time_diff(month_start)} minutes")

    # merge stops to everything
    print(f"\t * Starting to merge all data {cur_time_string()} by {cur_time_diff(month_start)} minutes")

     # number of days + stops in data
    stops_appeared['cuebiq_id'] = stops_appeared['cuebiq_id'].astype('string')
    combined = stops_appeared.merge(user_stats[['cuebiq_id', 'n_days']], on='cuebiq_id', how='right') #9/14: changed from inner
    
    combined = combined.fillna(0)
    if debug: print(combined)
    if debug: print('\t\t after left merging days appeared:', combined.columns, len(combined))
    
    # total stops
    stops.cuebiq_id = stops.cuebiq_id.astype('string')
    if debug: print('\t\t stops dtypes:',stops.dtypes)
    if debug: print('\t\t combined dtypes:',combined.dtypes)
    combined = combined.merge(stops, on='cuebiq_id', how='outer')
    if debug: print('after dropna', len(combined.dropna()))
    if debug: print(combined.dropna())
    
    if debug: print('\t\t after outer merging stops:', combined.columns, len(combined))
    
    # home and work cbg    
    combined = combined.merge(home_work_cbg, on='cuebiq_id', how='inner')
    if debug: print(combined)
    if debug: print('\t\t after inner merging home/work:', combined.columns, len(combined))
                               
    # calculate and combine VMT
    vmt = pd.read_csv(current_session_path + f'all_driver_info/all_driver_vmt_{current_city}_{month.year}_{month.month}.csv')
    vmt = vmt[['cuebiq_id', 'distance_m']]
    vmt['cuebiq_id'] = vmt['cuebiq_id'].astype('string')
    if debug: print(vmt)
    combined = combined.merge(vmt, on='cuebiq_id', how='left') #switched from outer to left 
    if debug: print(combined)
    combined.rename(columns={'vmt_tot': 'distance_m'})
    if debug: print(combined)
    if debug: print('\t\t after left merging vmt:', combined.columns, len(combined))
    
                               
    #calculated gyration
    gyration = pd.read_csv(current_session_path + f'all_driver_info/all_driver_gyration_{current_city}_{month.year}_{month.month}.csv')
    gyration_2 = pd.read_csv(current_session_path + f'all_driver_info/all_driver_gyration_k2_{current_city}_{month.year}_{month.month}.csv')
    gyration = gyration.merge(gyration_2, on="cuebiq_id", how="outer", suffixes=('','_k'))
    gyration['explorer'] = (gyration.radius_gyration_k > gyration.radius_gyration/2).astype(int)
    gyration.cuebiq_id = gyration.cuebiq_id.astype('string')
    combined = combined.merge(gyration, on='cuebiq_id', how='left')
    if debug: print('\t\t after left merging gyration:', combined.columns, len(combined))
    
    
    # munge format to merge to census data
    crosswalk.bg_fips = crosswalk.bg_fips.astype('string')
    
    #combined.GEOID_H = combined.GEOID_H.astype('string')
    #combined['GEOID_H'] = combined['GEOID_H'].astype(float).astype(int).astype('string')
    #combined['GEOID_H'] = combined['GEOID_H'].apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")
    combined = combined.merge(crosswalk, left_on='GEOID_H', right_on='bg_fips', how='left')
    if debug: print(combined)
    if debug: print('\t\t after left merging census crosswalk:', combined.columns, len(combined))

    # EV ownership
    combined = combined.merge(ev_ownership_data, left_on='zcta', right_on='zip', how='left')
    if debug: print(combined)
    if debug: print('\t\t after left merging ev ownership:', combined.columns, len(combined))
    
    # cbg_data
    combined_cbg = combined.merge(cbg_demographics, left_on='GEOID_H', right_on='bg_fips', how='left')
    
    combined.zcta = combined.zcta.astype('string').str.zfill(12) #new
    combined_zip = combined.merge(zip_demographics, on='zcta', how='left')
    if debug: print(combined_cbg)
    if debug: print(combined_cbg)
    
    # prep to save
    # print(combined_cbg.columns)
    # print(combined_zip.columns)
    combined_cbg = combined_cbg.drop(['bg_fips_x', 'bg_fips_y', 'zcta',
                                      #'Unnamed: 0', 'Unnamed: 0_k'
                                     ], axis=1)
    combined_cbg = combined_cbg.rename(columns={'GEOID_H': 'home_GEOID', 'GEOID_W': 'work_GEOID','distance_m':'vmt_tot'})
    
    combined_zip = combined_zip.drop(['zcta',
                                      #'Unnamed: 0', 'Unnamed: 0_k'
                                     ], axis=1)
    combined_zip = combined_zip.rename(columns={'GEOID_H': 'home_GEOID', 'GEOID_W': 'work_GEOID','distance_m':'vmt_tot'})
    combined_zip['vmt'] = combined_zip['vmt_tot']/combined_zip['n_days']
    combined_cbg['vmt'] = combined_cbg['vmt_tot']/combined_cbg['n_days']

    
    # save
    combined_cbg.to_csv(current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_cbg_demographics.csv')
    combined_zip.to_csv(current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_zip_demographics.csv')
    print(f"\t * Saved out the monthly id info at {cur_time_string()} by {cur_time_diff(month_start)} minutes")
    del combined_cbg
    del combined_zip
    