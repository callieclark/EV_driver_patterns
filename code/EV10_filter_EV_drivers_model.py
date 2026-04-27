################################################################################ Created on: 04/16/2025
# Created by: Anne and Callie
#
#Calculate all features needed and then apply the selected model
################################################################################


################################################################################
# prep
################################################################################

exec(open('EV00_settings.py').read())
from dateutil.rrule import rrule, MONTHLY, DAILY
import calendar
from statsmodels.api import OLS
from statsmodels.regression.linear_model import OLSResults

################################################################################
# functions
################################################################################
    
    
################################################################################
# run
################################################################################

# print some info before starting
print("\n\n-----------------------------------------------------------------------------\nSettings:")
print(f"\t * overwrite = {overwrite}")
print(f"\t * Start date = {start_date}")
print(f"\t * End date = {end_date}")
print(f"\t * # of days to include in query for when data is processed = {processing_days}")
print(f"\t * included counties = {counties}\n\n")

# get months in our study period
start_dt = dt.strptime(start_date, "%Y%m%d")
end_dt = dt.strptime(end_date, "%Y%m%d")
months = [dt for dt in rrule(MONTHLY, dtstart=start_dt, until=end_dt)]

# get days in our study period
days = [dt for dt in rrule(DAILY, dtstart=start_dt, until=end_dt)]
string_days = [str(day.strftime('%Y%m%d')) for day in days]


#------------------------------------------------------------------
# cec data
#------------------------------------------------------------------

# get the cec data
cec = pd.read_csv(root+f"data/census/ev_zip_percent_{current_city}.csv")
cec.perc_ev = cec.perc_ev * (1-never_public_charge_percent) 
cec.zip = cec.zip.astype(float)


#------------------------------------------------------------------
# gas stops
#------------------------------------------------------------------

# read in all gas stops for all study period
file_names = [data_path + f"ping{min_daily_pings}/gas_station_visits/gas_station_stops_{current_city}_{d}_{max_distance_stop_to_gas}m_{gas_station_dwell_time_max}min.csv" for d in string_days]
df_list = [pd.read_csv(file) for file in file_names]
gas_stops = pd.concat(df_list, ignore_index=True)

# aggregate to cuebiq_id
gas_stops = gas_stops.groupby('cuebiq_id')['nearest_SG_ID'].nunique().reset_index()
gas_stops = gas_stops.rename(columns={'nearest_SG_ID': 'gas_stations'})


#------------------------------------------------------------------
# read in driver info
#------------------------------------------------------------------

# read in ALL driver info to get zip code and stats for all months
m = months[0].month
y = months[0].year
driver_info = pd.read_csv(current_session_path + f"all_driver_info/all_driver_info_{current_city}_{y}_{m}_zip_demographics.csv", low_memory=False)
driver_info = driver_info[['cuebiq_id', 'zip', 'gas_stops', 'n_days', 'n_stops', 'vmt']]
driver_info['year'] = y
driver_info['month'] = m

for i in months[1:]: 
    y = i.year
    i = i.month
    d = pd.read_csv(current_session_path + f"all_driver_info/all_driver_info_{current_city}_{y}_{i}_zip_demographics.csv", low_memory=False)
    d = d[['cuebiq_id', 'zip', 'gas_stops', 'n_days', 'n_stops', 'vmt']]
    d['year'] = y
    d['month'] = i
    driver_info = pd.concat([driver_info, d], ignore_index=True)

driver_info = driver_info.groupby('cuebiq_id', dropna=False)\
    .agg({'gas_stops': 'sum', 'n_days': 'sum', 'n_stops': 'sum', 'month': 'count', 'vmt': 'mean'}).reset_index() 



#------------------------------------------------------------------
# ev sessions 
#------------------------------------------------------------------

# read in data across all months of EV stops 
file_names = [current_session_path + f"evcs_sessions/evcs_session_duration_{current_city}_{d}.csv" for d in string_days]
df_list = [pd.read_csv(file) for file in file_names]
ev_stops = pd.concat(df_list, ignore_index=True)
#ev_stops = ev_stops.rename(columns={'Unnamed: 0': 'cuebiq_id'}) # removed 1/19/26
ev_stops['date'] = ev_stops['timestamp'].str[0:10]



#------------------------------------------------------------------
# feature engineering 
#------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# distances between home/work and EV stops

# load in cbgs and get centroids
geo =  gpd.read_file(root+f'data/geo_files/{current_city}/studyarea_cbg.shp').to_crs(4326)
geo['lon'] = geo.geometry.centroid.x
geo['lat'] = geo.geometry.centroid.y
geo.GEOID = geo.GEOID.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")

# load in home work geoids and get the centroid of cbg
hw = pd.read_csv(data_path + f"ping{min_daily_pings}/user_home_work/user_home_work_{current_city}.csv")
hw.GEOID_H = hw.GEOID_H.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")
hw.GEOID_W = hw.GEOID_W.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")
hw = hw.merge(geo[['GEOID', 'lat', 'lon']], left_on="GEOID_H", right_on="GEOID", how="left")
hw = hw.rename(columns = {'lat': 'home_lat', 'lon': 'home_lon'})
hw = hw.merge(geo[['GEOID', 'lat', 'lon']], left_on="GEOID_W", right_on="GEOID", how="left")
hw = hw.rename(columns = {'lat': 'work_lat', 'lon': 'work_lon'})
hw = hw[['cuebiq_id', 'home_lat', 'home_lon', "work_lat", "work_lon"]]

# combine the ev stops w the home/work locations, calc distance
dist = ev_stops.merge(hw, on='cuebiq_id', how='left')
dist['dist_home'] = haversine_vectorized(dist.lat, dist.lng, dist.home_lat, dist.home_lon)
dist['dist_work'] = haversine_vectorized(dist.lat, dist.lng, dist.work_lat, dist.work_lon)
dist = dist.replace(0, np.nan)

# aggregate distance to cuebiq id
dist_agg = dist.groupby('cuebiq_id')\
    .agg({'lat': 'count', 'dist_home': 'sum', 'dist_work': 'sum'}).reset_index()
cols = ['dist_home', 'dist_work']
dist_agg[cols] = dist_agg[cols].div(dist_agg.lat, axis=0)
dist_agg = dist_agg.replace(0, np.nan)
dist_agg = dist_agg.drop(columns=['lat'])

# create distance bands to look at distribution of distances
def under_1000(x): return np.count_nonzero(x < 1000)
def bar_1000_10000(x): return np.count_nonzero((x > 1000) & (x < 10000))
def bar_10000_50000(x): return np.count_nonzero((x > 10000) & (x < 50000))
def over_50000(x): return np.count_nonzero(x > 50000)

dist_agg2 = dist\
    .groupby('cuebiq_id')\
    .agg({'lat': 'count', 
          'dist_home': [under_1000, bar_1000_10000, bar_10000_50000, over_50000, 'std'], 
          'dist_work': [under_1000, bar_1000_10000, bar_10000_50000, over_50000, 'std']}).reset_index()
dist_agg2.columns = [' '.join(col).strip() for col in dist_agg2.columns.values]
dist_agg2 = dist_agg2.rename(columns={'dist_home under_1000': 'p_dist_home_1k', 
                                      'dist_home bar_1000_10000': 'p_dist_home_1k_10k', 
                                      'dist_home bar_10000_50000': 'p_dist_home_10k_50k', 
                                      'dist_home over_50000': 'p_dist_home_50k', 
                                      'dist_home std': 'dist_home_std', 
                                      'dist_work under_1000': 'p_dist_work_1k', 
                                      'dist_work bar_1000_10000': 'p_dist_work_1k_10k', 
                                      'dist_work bar_10000_50000': 'p_dist_work_10k_50k', 
                                      'dist_work over_50000': 'p_dist_work_50k', 
                                      'dist_work std': 'dist_work_std'})
cols = ['p_dist_home_1k', 'p_dist_home_1k_10k', 'p_dist_home_10k_50k', 'p_dist_home_50k', 'dist_home_std',
        'p_dist_work_1k', 'p_dist_work_1k_10k', 'p_dist_work_10k_50k', 'p_dist_work_50k', 'dist_work_std']
dist_agg2[cols] = dist_agg2[cols].div(dist_agg2['lat count'], axis=0)

# create factors of the stds
dist_agg2['dist_home_std_factor'] = dist_agg2.dist_home_std.fillna(-1)
dist_agg2['dist_work_std_factor'] = dist_agg2.dist_work_std.fillna(-1)

dist_agg2.dist_home_std_factor = pd.cut(dist_agg2.dist_home_std_factor, 
                                           bins=[-2, -0.01, 0.01, 200, 3000, np.inf], 
                                           labels=['none', 'zero', 'low', 'med', 'high'])
dist_agg2.dist_work_std_factor = pd.cut(dist_agg2.dist_work_std_factor, 
                                           bins=[-2, -0.01, 0.01, 200, 3000, np.inf], 
                                           labels=['none', 'zero', 'low', 'med', 'high'])
dist_agg2 = dist_agg2.drop(columns=['lat count'])


# ------------------------------------------------------------------------------------------
# calculate percentage of time charging at different hours

timing = ev_stops
timing['datetime'] = pd.to_datetime(timing.timestamp, utc=True)
timing['datetime'] = timing['datetime'].dt.tz_convert(cities_tz[current_city])
timing['dow'] = timing.datetime.dt.weekday
timing['hour'] = timing.datetime.dt.hour
timing['date'] = timing.datetime.dt.date

timing['datetime_post'] = timing.datetime + pd.to_timedelta(timing['duration_LB'], unit='m')
timing['hour_post'] = timing.datetime_post.dt.hour

timing['weekday'] = timing.dow <= 4
timing['weekend'] = timing.dow > 4
timing['morning'] = (timing.hour >= 5) & (timing.hour < 12)
timing['afternoon'] = (timing.hour >= 12) & (timing.hour < 17)
timing['evening'] = (timing.hour >= 17) & (timing.hour < 21)
timing['night'] = (timing.hour >= 21) | (timing.hour < 5)
timing['midnight'] = timing.hour == 0
timing['workday'] = (timing.hour >= 7) & (timing.hour < 11) & (timing.hour_post >= 15) & (timing.hour_post < 19)
timing['weekday_morning'] = timing.weekday & timing.morning

# aggregate time of day info
time_day = timing\
    .groupby('cuebiq_id')\
    .agg({'lat': 'count', 'weekday': 'sum', 'weekend': 'sum', 'morning': 'sum', 
          'afternoon': 'sum', 'evening': 'sum', 'night': 'sum', 'workday': 'sum', 
          'weekday_morning': 'sum', 'midnight': 'sum'}).reset_index()
cols = ['weekday', 'morning', 'afternoon', 'evening', 'night', 'weekday_morning', 'workday', 'midnight']
time_day[cols] = timing[cols].div(timing.lat, axis=0)
time_day = time_day.drop(columns=['lat'])

# aggregate length of charge info
def above_180(x): return np.count_nonzero(x > 180)
def above_300(x): return np.count_nonzero(x > 300)
def above_420(x): return np.count_nonzero(x > 420)
def above_540(x): return np.count_nonzero(x > 540)
def above_600(x): return np.count_nonzero(x > 600)

time_charge = ev_stops\
    .groupby('cuebiq_id')\
    .agg({'lat': 'count', 
          'duration_LB': [above_180, above_300, above_420, above_540, above_600]}).reset_index()
time_charge.columns = [' '.join(col).strip() for col in time_charge.columns.values]
time_charge = time_charge.rename(columns={'duration_LB above_180': 'perc_3hr', 
                                          'duration_LB above_300': 'perc_5hr', 
                                          'duration_LB above_420': 'perc_7hr', 
                                          'duration_LB above_540': 'perc_9hr',
                                          'duration_LB above_600': 'perc_10hr'})
cols = ['perc_3hr', 'perc_5hr', 'perc_7hr', 'perc_9hr', 'perc_10hr']
time_charge[cols] = time_charge[cols].div(time_charge['lat count'], axis=0)
time_charge = time_charge.drop(columns=['lat count'])


# ------------------------------------------------------------------------------------------
# calculate how many stations are not visited multiple times in a single day
duped = timing[timing.duplicated(['cuebiq_id', 'date', 'evcs_id'], keep=False)]
duped_count = duped.groupby('cuebiq_id').agg({'evcs_id': pd.Series.nunique}).reset_index()
all_count = timing.groupby('cuebiq_id').agg({'evcs_id': pd.Series.nunique}).reset_index()
duped_perc = duped_count\
    .merge(all_count, on="cuebiq_id")\
    .rename(columns={'evcs_id_x': 'duplicate_stations', 'evcs_id_y': 'all_stations'})
duped_perc['perc_stations_multivisit'] = (duped_perc.duplicate_stations/duped_perc.all_stations)
duped_perc['single_visit_EVCS_stations'] = duped_perc.all_stations - duped_perc.duplicate_stations
duped_perc = duped_perc[['cuebiq_id', 'duplicate_stations', 'perc_stations_multivisit', 'single_visit_EVCS_stations']]


# ------------------------------------------------------------------------------------------
# calculate average time spent there
avg_time = ev_stops.groupby('cuebiq_id').agg({'duration_LB': ['mean', 'var']})
avg_time.columns = avg_time.columns.get_level_values(1)
avg_time = avg_time.rename(columns={'mean': 'avg_duration', 'var': 'var_duration'})


# ------------------------------------------------------------------------------------------
# calculate days between first and last EVCS stop
max_diff = ev_stops.groupby('cuebiq_id').agg({'date': ['min', 'max']}).reset_index()
max_diff.columns = [' '.join(col).strip() for col in max_diff.columns.values]
max_diff[['date min', 'date max']] = max_diff[['date min', 'date max']].apply(pd.to_datetime)
max_diff['max_date_diff'] = (max_diff['date max'] - max_diff['date min']).dt.days
max_diff = max_diff[['cuebiq_id', 'max_date_diff']]


# ------------------------------------------------------------------------------------------
# calculate number of stops at their most frequent station
max_stops = ev_stops.groupby(['cuebiq_id', 'evcs_id']).agg({'lat': 'count'}).reset_index().sort_values('lat', ascending=False)
max_stops = max_stops[['cuebiq_id', 'lat']].groupby('cuebiq_id').first().rename(columns={'lat': 'max_stops'}).reset_index()


# ------------------------------------------------------------------------------------------
# count total EVCS stops for each user
EVCS_stops = ev_stops[['cuebiq_id', 'lat']].groupby('cuebiq_id').count()\
        .reset_index().rename(columns={'lat': 'EVCS_stops'})
    
    
# ------------------------------------------------------------------------------------------
# count how many unique stations each user went to
EVCS_stations = ev_stops[['cuebiq_id', 'evcs_id', 'lat']]\
        .groupby(['cuebiq_id', 'evcs_id'])\
        .count().reset_index()\
        .rename(columns={'lat': 'EVCS_stations'})\
        .drop('evcs_id', axis=1).groupby('cuebiq_id').count().reset_index()


# ------------------------------------------------------------------------------------------
# daily stop counts by session and station
daily_stops = ev_stops.groupby(['cuebiq_id', 'date'])\
    .agg({'lat': 'count', 'evcs_id': pd.Series.nunique}).reset_index()\
    .groupby('cuebiq_id')\
    .agg({'lat': 'mean', 'evcs_id': 'mean'})\
    .rename(columns={'lat': 'avg_daily_sessions', 'evcs_id': 'avg_daily_stations'}).reset_index()

# merge everything
comb_stats = avg_time.merge(max_diff, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(time_day, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(time_charge, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(max_stops, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(EVCS_stops, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(EVCS_stations, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(gas_stops, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(daily_stops, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(duped_perc, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(dist_agg, on='cuebiq_id', how='outer')
comb_stats = comb_stats.merge(dist_agg2, on='cuebiq_id', how='outer')
driver_info = driver_info.merge(comb_stats, on='cuebiq_id', how='left')

# fill 0's
driver_info.gas_stations = driver_info.gas_stations.fillna(0)
driver_info.var_duration = driver_info.var_duration.fillna(0)

# calc ratios
driver_info['avg_days_btwn'] = driver_info.n_days/driver_info.EVCS_stops
driver_info['perc_one_EVCS'] = driver_info.max_stops/driver_info.EVCS_stops
driver_info['ev_stops_per_station'] = driver_info.EVCS_stops/driver_info.EVCS_stations
driver_info['perc_EVCS'] = driver_info.EVCS_stops/driver_info.n_stops
driver_info['avg_days_elapsed'] = driver_info.EVCS_stops/driver_info.max_date_diff
driver_info['ev_stops_per_month'] = driver_info.EVCS_stops/driver_info.n_days*30
driver_info['gas_stops_per_month'] = driver_info.gas_stops/driver_info.n_days*30
driver_info['stop_ratio'] = driver_info.gas_stops/driver_info.EVCS_stops
driver_info['tot_charge_per_vmt'] = (driver_info.EVCS_stops*driver_info.avg_duration)/(driver_info.n_days*driver_info.vmt)
driver_info['perc_timeblock'] = driver_info[['morning', 'afternoon', 'evening', 'night']].max(axis=1)/driver_info.EVCS_stops
driver_info['avg_sessions_per_station'] = driver_info.avg_daily_sessions/driver_info.avg_daily_stations

# calc specific boundaries
driver_info['ev_gt_0'] = driver_info.EVCS_stops > 0
driver_info['ev_gt_2'] = driver_info.EVCS_stops > 2
driver_info['ev_gt_30'] = driver_info.EVCS_stops > 30
driver_info['ev_gt_40'] = driver_info.EVCS_stops > 40
driver_info['ev_gt_50'] = driver_info.EVCS_stops > 50
driver_info['ev_gt_60'] = driver_info.EVCS_stops > 60
driver_info['ev_gt_70'] = driver_info.EVCS_stops > 70
driver_info['ev_gt_80'] = driver_info.EVCS_stops > 80
driver_info['gas_lt_2'] = driver_info.gas_stops < 2
driver_info['ev_pm_gt_1'] = driver_info.ev_stops_per_month > 1
driver_info['ev_pm_gt_2'] = driver_info.ev_stops_per_month > 2
driver_info['gas_pm_lt_1'] = driver_info.gas_stops_per_month < 1
driver_info['gas_pm_lt_2'] = driver_info.gas_stops_per_month < 2
driver_info['ev_gt_gas'] = driver_info.EVCS_stops > driver_info.gas_stops
driver_info['ev_coverage'] = driver_info.max_date_diff/driver_info.n_days

# make binned variables + quick fixes
driver_info['any_weekday_morning'] = driver_info.weekday_morning > 0
driver_info['any_workday'] = driver_info.workday > 0
driver_info['any_10hr'] = driver_info.perc_10hr > 0
driver_info['any_7hr'] = driver_info.perc_7hr > 0
driver_info['any_midnight'] = driver_info.midnight > 0
driver_info['ev_bins_per_month'] = pd.cut(driver_info.ev_stops_per_month, bins=[-1, 0, 4, 99999999], labels=['0', '1-4', '5+'])
driver_info.dist_home_std_factor[(driver_info.ev_gt_0) & (driver_info.dist_home_std_factor == "none")] = "zero"

#------------------------------------------------------------------
# get geographic (zip) info from other drivers 
#------------------------------------------------------------------

# read in home work cbg
home_work = pd.read_csv(data_path + f"ping{min_daily_pings}/user_home_work/user_home_work_{current_city}.csv")
home_work = home_work[['cuebiq_id', 'GEOID_H']]
home_work.GEOID_H = home_work.GEOID_H.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")
home_work.cuebiq_id = home_work.cuebiq_id.astype(int)
    
# read in crosswalk to zip and take zip with most area
crosswalk = pd.read_csv(zip_crosswalk)
#idx = crosswalk.groupby('bg_fips')['afact'].idxmax()
#crosswalk = crosswalk.loc[idx]
crosswalk = crosswalk[['cbg', 'GEOID_ZCTA5_20']].rename(columns = {'GEOID_ZCTA5_20': 'zip', 'cbg': 'bg_fips'})
crosswalk.bg_fips = crosswalk.bg_fips.astype('string')
crosswalk.bg_fips = crosswalk.bg_fips.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else pd.NA).astype("string")

# convert to zip and merge to driver info
home_work = home_work.merge(crosswalk, left_on='GEOID_H', right_on='bg_fips', how='left')
driver_info = driver_info.merge(home_work, on="cuebiq_id", how="left")
driver_info.cuebiq_id.nunique()


#------------------------------------------------------------------
# merge other data 
#------------------------------------------------------------------

# merge in demographics and cec
demographics = pd.read_csv(root + f"data/census/demographics_cbg_{current_city}.csv")
demographics = demographics.replace(r'^\s*$', np.nan, regex=True)
demographics = demographics.replace(r'--', np.nan, regex=True)
demographics.bg_fips = demographics.bg_fips.astype(str).str.pad(width=12, side='left', fillchar='0')
demographics = demographics.drop_duplicates()
#demographics.zcta = demographics.zcta.astype(float)

cec.zip = cec.zip.astype(float)

# deal with misingness in EV cols
cols_str = ['GEOID_H', 'GEOID_W', 'bg_fips']
cols_fac = ['dist_home_std_factor', 'dist_work_std_factor', 'ev_bins_per_month']
cols_bin = ['any_weekday_morning', 'any_workday', 'any_10hr', 'any_7hr', 'any_midnight']
cols_fill = [c for c in driver_info.columns if c not in cols_fac and c not in cols_bin and c not in cols_str]
full_df = driver_info
full_df[cols_bin] = full_df[cols_bin].fillna(False)
full_df[cols_fill] = full_df[cols_fill].fillna(0)
full_df[['dist_home_std_factor', 'dist_work_std_factor']] = full_df[['dist_home_std_factor', 'dist_work_std_factor']].fillna("none")
full_df.ev_bins_per_month = full_df.ev_bins_per_month.fillna("0")

# merge with demographics and cec
full_df = full_df.merge(cec, on="zip", how='left')
full_df = full_df.merge(demographics, left_on="bg_fips", right_on="bg_fips", how='left')
full_df.income_median = full_df.income_median.astype(float)
full_df[['perc_ev', 'gas_stops', 'gas_stations', 'gas_stops_per_month', 
                              'n_days', 'n_stops', 'avg_duration', 'stop_ratio', 'EVCS_stops', 
                              'EVCS_stations',  'vmt', 'ev_stops_per_month', 'perc_mfh', 'perc_one_EVCS', 
                              'weekday', 'morning', 'tot_charge_per_vmt', 'ev_stops_per_station', 
                              'ev_gt_2', 'ev_gt_30','gas_lt_2', 'ev_gt_gas']].isna().sum()

# make scaled variables
full_df['vmt_100'] = full_df.vmt/100
full_df['income_median_10000'] = full_df.income_median/10000

#------------------------------------------------------------------
# create linear model 
#------------------------------------------------------------------

if debug: print(full_df[['perc_ev', 'gas_stops_per_month', 'EVCS_stations', 'vmt', 'ev_gt_gas']].head())

df = full_df.dropna(subset = ['perc_ev', 'gas_stops_per_month', 'EVCS_stations', 'vmt', 'ev_gt_gas'])
if debug: print(df[['perc_ev', 'gas_stops_per_month', 'EVCS_stations', 'vmt', 'ev_gt_gas']].head())

model = OLSResults.load(f'{data_path}validation_output/{model_path}')

# if you want to run the model here you can change to your formula of choice
# OLS.from_formula("perc_ev ~ gas_stops_per_month + vmt + EVCS_stations + ev_gt_gas", data = df).fit()


#------------------------------------------------------------------
# use model to predict and select our "EV driver set"
#------------------------------------------------------------------

# get predictions
full_df['city'] = current_city
full_df['prediction'] = model.predict(full_df)

# identify id's in top 1.9% 
# Bay has 4.9% EV ownership and 39% don't exclusively charge at home or work, ie 0.049*(1-0.269)=0.035819
full_df['prediction_quantile'] = full_df['prediction'].rank(pct=True)
full_df['pred_EV_driver'] = full_df.prediction_quantile >= ev_percentage
filtered_drivers = full_df[(full_df.prediction_quantile >= ev_percentage)]
print(len(filtered_drivers),len(full_df))


#------------------------------------------------------------------
# outputs
#------------------------------------------------------------------


#reset this
log_users(current_session_path + f'ev_driver_info/user_set_{current_city}_model.txt', list(filtered_drivers.cuebiq_id))
if debug: print('Number of EV Drivers: ', len(list(filtered_drivers.cuebiq_id)))

for month in months:
    
    month_start = time.time()
    print(f"-----------------------------------------------------------------------------\nOutputting EV drivers for month: {month}")
    
    stops = pd.read_csv(current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_cbg_demographics.csv', low_memory=False)
    stops = stops[stops.cuebiq_id.isin(filtered_drivers.cuebiq_id)]
    stops.to_csv(current_session_path + f'ev_driver_info/ev_driver_info_model_{current_city}_{month.year}_{month.month}_cbg_demographics.csv', index=False)
    
    stops = pd.read_csv(current_session_path + f'all_driver_info/all_driver_info_{current_city}_{month.year}_{month.month}_zip_demographics.csv', low_memory=False)
    stops = stops[stops.cuebiq_id.isin(filtered_drivers.cuebiq_id)]
    stops.to_csv(current_session_path + f'ev_driver_info/ev_driver_info_model_{current_city}_{month.year}_{month.month}_zip_demographics.csv', index=False)