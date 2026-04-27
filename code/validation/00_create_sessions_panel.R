library(tidyverse)
library(viridis)
library(tigris)
library(sf)

########################################################################################
# Code by: Anne Driscoll
# Last edited on: 04/14/2026
# Gets info for ev watts behavior
########################################################################################

########################################################################################
# settings
########################################################################################

cities = c("Bay", "Seattle", "Boston", "Denver")
states = c("California", "Washington", "Massachusetts", "Colorado")

region_shapes = list(counties(state = "CA") %>% 
                       filter(GEOID %in% c('06013', '06041', '06055', '06075', 
                                           '06081', '06097', '06095', '06085', 
                                           '06001')), 
                     counties(state = "WA") %>% 
                       filter(GEOID %in% c('53061', '53033', '53053')), 
                     counties(state = "MA") %>% 
                       filter(GEOID %in% c('25009', '25017', '25021', '25023', 
                                           '25025', '25017', '33015', '33017')), 
                     counties(state = "CO") %>% 
                       filter(GEOID %in% c('08031', '08005', '08059', '08001', 
                                           '08035', '08014', '08039', '08093', 
                                           '08019', '08047')))

Mode <- function(x) {
  a <- table(x)
  as.numeric(names(a)[a == max(a)])
}


########################################################################################
# read in data
########################################################################################

session = read_csv("../data/input/evwatts/session.csv", 
                   col_names = c("id", "station_id", "port_id", "start_datetime", 
                                 "start_time_zone", "end_datetime", "end_time_zone", 
                                 "total_duration", "charge_duration", "energy_kwh", 
                                 "account_id", "charge_level", "fee", "currency", 
                                 "ended_by", "start_soc", "end_soc", "flag_id")) %>% 
  mutate(date = date(start_datetime), 
         week = week(start_datetime), 
         month = month(start_datetime), 
         year = year(start_datetime))

station = read_csv("../data/input/evwatts/station.csv") %>% 
  filter(venue != "Fleet", 
         venue != "Multi-Unit Dwelling", 
         venue != "Single Family Residential",
         access_type == "Public")


########################################################################################
# munge data
########################################################################################

for (i in 1:length(cities)) { 
  
  cur_city = cities[i]
  cur_state = states[i]
  cur_region = region_shapes[[i]]
  
  # filter to city data ----------------------------------------------------------
  station_city = station %>% 
    filter(state == cur_state, !is.na(lat)) %>%
    st_as_sf(coords = c("lon", "lat"), crs=4269) %>%
    st_intersection(cur_region)
  
  table(station_city$charge_level)/nrow(station_city)
  
  session_city = session %>% 
    merge(station_city %>% dplyr::select(id, station_location_id), by.x="station_id", by.y="id") %>% 
    filter(month >= 3,
           month <= 5, 
           year == 2022) 
  
  table(session_city$charge_level)/nrow(session_city)
  print(paste0(cur_city, " has ", nrow(session_city), " total sessions."))
  
  # get stats
  #View(table(session_wa$account_id))
  #View(table(session_wa$station_id))
  
  
  # group sessions by id and time ------------------------------------------------
  
  session_timings = session_city %>% 
    arrange(account_id, start_datetime) %>% 
    group_by(account_id) %>% 
    mutate(time_diff = start_datetime - lag(end_datetime), 
           num_diff = as.numeric(time_diff), 
           num_diff = ifelse(is.na(num_diff), 0, num_diff), 
           session = cumsum(num_diff > 15*60), 
           session_diff = time_diff*(session - lag(session)))
  
  # find user IDs that appear to be shared because sessions overlap
  # bad_user_ids = session_timings %>% 
  #   group_by(account_id) %>% 
  #   summarise(any_overlap = any(time_diff < 0)) %>% 
  #   filter(any_overlap)
  
  # find location IDs that are intensly overwhelming the dataset
  # bad_loc_ids = session_timings %>% 
  #   group_by(station_location_id) %>% 
  #   summarise(n = n(), 
  #             perc = n/nrow(session_timings)) %>% 
  #   filter(perc > .1)
  
  # filter out bad ids
  # session_timings = session_timings %>% 
  #   filter(!account_id %in% bad_ids$account_id) %>% 
  #   filter(!station_location_id %in% bad_loc_ids$station_location_id) 
  
  # group sessions
  grouped_sessions = session_timings %>% 
    merge(station_city %>% select(id, zip_code, venue, curbside, ev_pricing_category) %>% st_drop_geometry(), 
          by.x = "station_id", by.y="id", all.x = T) %>% 
    group_by(account_id, session) %>% 
    summarise(total_duration = sum(total_duration)*60, 
              hour = min(hour(start_datetime)),
              dow = min(wday(start_datetime)),
              fast = any(charge_level == "DCFC"),
              avg_session_diff = mean(session_diff, na.rm=T), 
              avg_session_diff = ifelse(is.infinite(avg_session_diff), NA, avg_session_diff), 
              n = n(), 
              venue = first(venue), 
              curbside = first(curbside), 
              ev_pricing_category = first(ev_pricing_category), 
              station_location_id = first(station_location_id)) %>% 
    mutate(dow = ifelse(dow == 1, 8, dow), 
           dow = dow-2) %>% 
    filter(total_duration > 10, total_duration < 60*24)
  print(paste0(cur_city, " has ", nrow(grouped_sessions), " grouped and filtered sessions."))
  
  saveRDS(session_timings, 
          paste0("../data/output/", tolower(cur_city), "_indiv_panel.RDS"))
  saveRDS(grouped_sessions, 
          paste0("../data/output/", tolower(cur_city), "_sessions_panel.RDS"))
  
   ggplot() + geom_density(data = grouped_sessions %>% filter(dow <= 4), 
                           aes(hour, color="all")) + ggtitle(cur_city)
  # 
  # ggplot() + geom_density(data = grouped_sessions %>% filter(dow <= 4), 
  #                         aes(hour, color="all")) + ggtitle(cur_city) + 
  #   facet_grid(vars(venue))
  # 
  ggplot(data = grouped_sessions %>% 
           mutate(dur_cut = cut(total_duration, c(-1, 10, 30, 60, 120, 240, 540, 999999)), weekend = dow > 4)) + 
    geom_density(aes(hour, color="all")) + ggtitle(cur_city) + 
    facet_grid(vars(dur_cut), vars(weekend))
  
}


########################################################################################
# see if we can do better at getting rid of fleets 
########################################################################################



ggplot() + geom_density(data = grouped_sessions_wa %>% filter(dow <= 4), 
                        aes(hour, color="all")) + ggtitle("Seattle") + 
  facet_grid(vars(venue))

ggplot() + geom_density(data = grouped_sessions_wa %>% filter(dow <= 4), 
                        aes(hour, color="all")) + ggtitle("Seattle") + 
  facet_grid(vars(curbside))

ggplot() + geom_density(data = grouped_sessions_wa %>% filter(dow <= 4), 
                        aes(hour, color="all")) + ggtitle("Seattle") + 
  facet_grid(vars(ev_pricing_category))

temp_wa = session_timings_wa %>% 
  group_by(account_id) %>% 
  summarise(avg_fee = mean(fee, na.rm=T), 
            avg_fee = ifelse(is.na(avg_fee), -9, avg_fee), 
            n_sessions = n(),
            total_days = max(start_datetime) - min(start_datetime), 
            total_days = as.numeric(total_days)/60/60/24, 
            max_hour = Mode(hour(start_datetime))) 
temp_wa = merge(grouped_sessions_wa, temp_wa, by="account_id", all.x=T)  %>%
  mutate(avg_fee_cut = cut(avg_fee, c(-10, -0.01, 0, 2, 6, 75)), 
         n_sessions_cut = cut(n_sessions, c(0, 2, 6, 50, 200, 315)), 
         sessions_day = n_sessions/(total_days+1), 
         sessions_day_cut = cut(sessions_day, c(0.0024,  0.0061, 0.093, 0.27, .0172, 1, 2.88))) #0, 1, 5, 33, 66, 90, 100
  
ggplot() + 
  geom_density(data = temp_wa %>% filter(dow <= 4), aes(hour)) +
  facet_grid(vars(avg_fee_cut)) + ggtitle("Seattle")


# 
ggplot() +
  geom_density(data = grouped_sessions_wa %>%
                 filter(dow <= 4), aes(hour, color="all")) +
  facet_grid(vars(n_sessions_fac), vars(n_hrs_between)) + ggtitle("Seattle") +
  geom_text(data=annotation, aes(x=1.8, y=1, label=n), parse=FALSE)

# 
ggplot() +
   geom_density(data = temp_wa %>% filter(dow <= 4, avg_fee > 0),
                aes(hour, color="fee > 0"))  +
  geom_density(data = temp_wa %>% filter(dow <= 4), 
               aes(hour, color="all")) +
  geom_density(data = temp_wa %>% filter(dow <= 4, avg_fee == 0),
               aes(hour, color="fee = 0"))  +
  geom_density(data = temp_wa %>% filter(dow <= 4, avg_fee == -9),
               aes(hour, color="fee = NA"))  +
  facet_grid(vars(n_sessions_cut)) + ggtitle("Seattle")

