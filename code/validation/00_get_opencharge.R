library(tidyverse)
library(viridis)
library(tigris)
library(jsonlite)
library(data.table)
source("../../tokens.R")

########################################################################################
# Code by: Anne Driscoll
# Last edited on: 02/06/2026
# Gets the opencharge api
########################################################################################

y = fromJSON(paste0("https://api.openchargemap.io/v3/poi/?output=json&", 
                    "countrycode=US&maxresults=999999&", 
                    "boundingbox=(29.6394,-96.6167),(30.9252,-98.7239)&", 
                    "compact=false&verbose=false&key=", opencharge_key))

stations = y %>% 
  unnest(AddressInfo, names_sep = ".") %>% 
  unnest(DataProvider, names_sep = ".") %>% 
  unnest(UsageType, names_sep = ".") %>%
  mutate(count_level1 = NA, count_level2 = NA, 
         count_fast = NA, count_other = NA) 

for (i in 1:nrow(stations)) {
  cons = stations$Connections[i][[1]]
  stations$count_level1[i] = sum(ifelse(cons$LevelID == 1, cons$Quantity, 0))
  stations$count_level2[i] = sum(ifelse(cons$LevelID == 2, cons$Quantity, 0))
  stations$count_fast[i] = sum(ifelse(cons$LevelID >= 3, cons$Quantity, 0))
  stations$count_other[i] = sum(cons$Quantity, na.rm=T) - stations$count_level1[i] - stations$count_level2[i] - stations$count_fast[i]
}

#Facility Type

s = stations %>% 
  mutate(`Connector Type` = NA, `EV Network` = NA, `Earliest Open Date` = DateCreated, 
         `Facility Type` = UsageType.Title, 
         City = AddressInfo.Town, Latitude = AddressInfo.Latitude, Longitude = AddressInfo.Latitude) %>% 
  rename(`Station ID` = ID, `Station Names` = AddressInfo.Title, 
         `EV Level 1 EVSE Num` = count_level1, `EV Level 2 EVSE Num` = count_level2, 
         `EV DC Fast Count` = count_fast, `EV Other Count` = count_other) %>% 
  select(`Station ID`, `Station Names`, `EV Level 1 EVSE Num`,
         `EV Level 2 EVSE Num`, `EV DC Fast Count`, `EV Other Count`,
         `Connector Type`, `Facility Type`, `EV Network`, Latitude,
         Longitude, City, `Earliest Open Date`) #, `geometry`)`Facility Type`, 


write_csv(s, "../data/output/opencharge_austin.csv")
