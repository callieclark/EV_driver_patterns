################################################################################
# Pull ping data from server
# Created on: 03/14/2025
# Created by: Callie and Anne
################################################################################


################################################################################
# prep
################################################################################

# bring in all settings
exec(open('EV00_settings.py').read())


################################################################################
# functions
################################################################################

def date_string_creation(date, day_delta=0):
    date = date + timedelta(days=day_delta)
    date_str = str(date.strftime('%Y%m%d'))
    return date_str
    

def process_day(date):
    """
    For a specific day, reads in all files, combines them, and saves out
    """
    
    date_str = date_string_creation(date)
    
    # check that all the files we need exist
    print(f"\t * Checking files exist for {date_str} at {cur_time_string()}")
    existing_files = []
    non_existent_files = []
    event_dates = [date_string_creation(date), date_string_creation(date, 1)]
    for e in event_dates:
        processing_dates = [date_string_creation(date, x) for x in range(processing_days+1)]
        for p in processing_dates:
            if e > p: continue #does this capture when e is processing date?
            for c in counties: 
                file = data_path + f'raw/ping/process_date/ping_data_{c}_processed{p}_event{e}.csv.gz'
                exists = os.path.exists(file)
                if not exists: 
                    non_existent_files.append(file)
                else: 
                    existing_files.append(file)
                
    # if they don't, throw an error           
    if len(non_existent_files) > 0:
        print(f"\t * Date {date_str} is missing the following files, skipping at {cur_time_string()}")
        print("\t\t * " + "\n\t\t * ".join(non_existent_files))
        return
    
    # read in all the relevant files, convert timezone and filter to relevant day
    print(f"\t * File check successful, now reading in data and converting time zones at {cur_time_string()}")
    data = []
    for f in existing_files: 
        try:
        
            day = pd.read_csv(f)
            day['event_zoned_datetime'] = pd.to_datetime(day['event_zoned_datetime'], errors='coerce', utc=True)
            day['event_zoned_datetime'] = day['event_zoned_datetime'].dt.tz_convert(timezone)
            day = day[day.event_zoned_datetime.dt.date == dt.date(date)]
            data.append(day)
        except:
            print('Error in file',f)
            del data
            print (f'SKIPPING {date}, re-generate files and run again')
            return
    
    # combine the files
    del day
    data = pd.concat(data, ignore_index=True)  
         
    # save out
    print(f"\t * Saving data at {cur_time_string()}")
    file_name = data_path+f'raw/ping/event_date/ping_data_{current_city}_{date_str}.parquet'
    data.to_parquet(file_name)
    
    # print info on date processing
    print(f"\t * Date {date_str} has {len(data):,} rows")
    del data
    

def process_and_save_data():
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
        f_exists = os.path.isfile(data_path + f"raw/ping/event_date/ping_data_{current_city}_{date_str}.parquet")
        if f_exists and not overwrite: 
            print(f"---------- Date {date_str} has already been processed, skipping at {cur_time_string()}")
            current_date = current_date + timedelta(days = 1)
            continue
            
        # print alert of what day is being processed
        print(f"-----------------------------------------------------------------------------\nProcessing date: {date_str}")
        
        # CENTRAL ACTION: pull data, calculate speed
        process_day(current_date)
        
        # iterate to next day 
        print(f"\t * Finished date {date_str} in {round((time.time()-date_start)/60, 2)} minutes")
        current_date = current_date + timedelta(days = 1)

        
################################################################################
# run
################################################################################

print("\n\n-----------------------------------------------------------------------------\nSettings:")
print(f"\t * overwrite = {overwrite}")
print(f"\t * Start date = {start_date}")
print(f"\t * End date (modified to 'end_date' - 'processing_days') = {end_date}")
print(f"\t * # of days to include in query for when data is processed = {processing_days}")
print(f"\t * included counties = {counties}\n\n")


# run this file
process_and_save_data()
