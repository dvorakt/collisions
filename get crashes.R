#this code generates data for Collapse in Collisions viz
#https://public.tableau.com/views/ACollapseinCollisionsv2/Dashboard2?:language=en&:display_count=y&publish=yes&:origin=viz_share_link
#the inputs are urls to open data sets of various cities
#the output are two datasets: 
#  daily_crashes.csv - daily collisions (and for some cities no of killed) for cities
#  map.csv - every crash from March 16 through the latest date in 2019 and 2020 (to compare geo distribution of crashes in 2020 vs 2019)


library(tidyverse)
library(lubridate)
library(stringr)
library(jsonlite)

#NYC
raw <- read_csv("https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD", guess_max = 100000)
nyc <- raw %>%
  mutate(date=as.Date(`CRASH DATE`, format="%m/%d/%Y"),
         city="New York") %>%
  group_by(city, date) %>%
  summarize(n=n(), 
            no_injured=sum(`NUMBER OF PERSONS INJURED`), 
            no_killed=sum(`NUMBER OF PERSONS KILLED`)) %>%
  filter(year(date)>2012) 

nyc2 <- raw %>%
  mutate(date=as.Date(`CRASH DATE`, format="%m/%d/%Y"),
         city="New York",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lat=LATITUDE,
         lon=LONGITUDE) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  filter(lat!=0) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)
  
#LA
la_raw <- read_csv("https://data.lacity.org/api/views/d5tf-ez2w/rows.csv?accessType=DOWNLOAD") 
la <- la_raw %>%
  mutate(date = as.Date(`Date Occurred`, format = "%m/%d/%Y"),
         city= "Los Angeles",
         killed = ifelse(str_detect(`MO Codes`,"3027"),1,0)) %>%
  group_by(city, date) %>%
  summarise(n=n(), no_killed = sum(killed))

la2 <- la_raw %>%
  mutate(date = as.Date(`Date Occurred`, format = "%m/%d/%Y"),
         city= "Los Angeles",
         Location=str_remove(Location, "\\("),
         Location=str_remove(Location, "\\)"),
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  separate(Location, into=c("lat","lon"), sep=",", convert = TRUE) %>%
  filter(year(date)>2018) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  filter(lat!=0) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)

#Denver
raw <- read_csv("https://www.denvergov.org/media/gis/DataCatalog/traffic_accidents/csv/traffic_accidents.csv")
denver <- raw %>%
  mutate(date=as.Date(`FIRST_OCCURRENCE_DATE`),
         city="Denver")%>%
  group_by(city, date) %>%
  summarise(n=n(), no_killed=sum(FATALITIES)) %>%
  filter(year(date)>2012)

denver2 <- raw %>%
  mutate(date=as.Date(`FIRST_OCCURRENCE_DATE`),
         city="Denver",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  rename(lat=GEO_LAT, lon=GEO_LON) %>%
  filter(lat!=0) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)


#Seattle
#http://data-seattlecitygis.opendata.arcgis.com/datasets/5b5c745e0f1f48e7a53acec63a0022ab_0
raw <- read_csv("http://data-seattlecitygis.opendata.arcgis.com/datasets/5b5c745e0f1f48e7a53acec63a0022ab_0.csv?outSR={%22latestWkid%22:2926,%22wkid%22:2926}")
seattle <- raw %>%
  mutate(date = as.Date(substr(INCDATE,1,10)),
         city="Seattle") %>%
  group_by(city, date) %>%
  summarise(n=n(),no_killed=sum(FATALITIES)) %>%
  filter(year(date)>2012)

raw3 <- fromJSON("http://data-seattlecitygis.opendata.arcgis.com/datasets/5b5c745e0f1f48e7a53acec63a0022ab_0.geojson?outSR={%22latestWkid%22:2926,%22wkid%22:2926}")
seattle2 <- raw3[[4]][[3]] %>%
  bind_cols(raw) %>%
  filter(!is.na(type)) %>%
  mutate(date = as.Date(substr(INCDATE,1,10)),
         city="Seattle",
         coordinates=str_remove(coordinates, "c\\("),
         coordinates=str_remove(coordinates, "\\)"),
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  separate(coordinates, into=c("lon","lat"), sep=",", convert = TRUE) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  filter(lat!=0) %>%
  filter(!is.na(lat)) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)
  
#Chicago
raw <- read_csv("https://data.cityofchicago.org/api/views/85ca-t3if/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*")
chicago <- raw %>%
  mutate(date=as.Date(substr(CRASH_DATE,1,10),format="%m/%d/%Y"),
         city="Chicago") %>%
  group_by(city, date) %>%
  summarise(n=n(),no_killed=sum(INJURIES_FATAL, na.rm = TRUE)) %>%
  filter(year(date)>2018) #earlier data seems off

chicago2 <- raw %>%
  mutate(date=as.Date(substr(CRASH_DATE,1,10),format="%m/%d/%Y"),
         city="Chicago",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lat=LATITUDE, lon=LONGITUDE) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  filter(lat!=0) %>%
  filter(!is.na(lat)) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)
  
#Austin
raw <- read_csv("https://data.austintexas.gov/api/views/dx9v-zd7x/rows.csv?accessType=DOWNLOAD")
austin <- raw %>%
  mutate(date=as.Date(substr(`Published Date`,1,10),format="%m/%d/%Y"),
         city = "Austin") %>%
  group_by(city, date) %>%
  summarise(n=n()) 

austin2 <- raw %>%
  mutate(date=as.Date(substr(`Published Date`,1,10),format="%m/%d/%Y"),
         city="Austin",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lat=Latitude, lon=Longitude) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date)) %>%
  filter(lat!=0) %>%
  filter(!is.na(lat)) %>%
  mutate(lat=ifelse(lat>40,lat/1000000,lat)) %>%
  filter(lat>30) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)

#Cincinnati
raw <- read_csv("https://data.cincinnati-oh.gov/api/views/rvmt-pkmq/rows.csv?accessType=DOWNLOAD", guess_max = 100000)
cincinnati <- raw %>%
  mutate(date = as.Date(substr(CRASHDATE,1,10),format="%m/%d/%Y"),
         city="Cincinnati") %>%
  group_by(city, date) %>%
  filter(year(date)>2014) %>%
  summarize(n=n())

cincinnati2 <- raw %>%
  mutate(date = as.Date(substr(CRASHDATE,1,10),format="%m/%d/%Y"),
         city="Cincinnati",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lat=LATITUDE_X, lon=LONGITUDE_X) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date, na.rm=TRUE)) %>%
  filter(lat!=0) %>%
  filter(lat > 30) %>%
  filter(lon < -80) %>%
  filter(!is.na(lat)) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)

#Boston
raw <- read_csv("https://data.boston.gov/dataset/7b29c1b2-7ec2-4023-8292-c24f5d8f0905/resource/e4bfe397-6bfc-49c5-9367-c879fac7401d/download/tmp532r_i6n.csv")
boston <- raw %>%
  mutate(date = as.Date(dispatch_ts),
         city = "Boston") %>%
  group_by(city, date) %>%
  summarise(n=n())

boston2 <- raw %>%
  mutate(date = as.Date(dispatch_ts),
         city = "Boston",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lon=long) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date, na.rm=TRUE)) %>%
  filter(lat!=0) %>%
  filter(!is.na(lat)) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)

#washington dc
raw <- read_csv("https://opendata.arcgis.com/datasets/70392a096a8e431381f1f692aaa06afd_24.csv")
dc <- raw %>%
  mutate(date = as.Date(substr(REPORTDATE,1,10),format="%Y/%m/%d"),
         city = "Washington, D.C.") %>%
  group_by(city, date) %>%
  summarise(n=n(), no_killed=sum(FATAL_BICYCLIST+FATAL_DRIVER+FATAL_PEDESTRIAN+FATALPASSENGER)) %>%
  filter(year(date)>2013) %>%
  filter(!is.na(date))

dc2 <- raw %>%
  mutate(date = as.Date(substr(REPORTDATE,1,10),format="%Y/%m/%d"),
         city = "Washington, D.C.",
         date2 = as.Date(ifelse(year(date)==2019, date+years(1),date), origin = "1970-01-01")) %>%
  rename(lon=LONGITUDE, lat=LATITUDE) %>%
  filter(date2>as.Date("2020-03-15") & date2<max(date, na.rm=TRUE)) %>%
  filter(lat!=0) %>%
  filter(!is.na(lat)) %>%
  mutate(id=row_number()) %>%
  select(date, lat, lon, city, id)
  
#dallas
#seems to have only very recent
r <- read_csv("https://www.dallasopendata.com/api/views/tqs9-xfzb/rows.csv?accessType=DOWNLOAD")
r <- read_csv("https://www.dallasopendata.com/api/views/9fxf-t2tr/rows.csv?accessType=DOWNLOAD")
r <- read_json("https://www.dallasopendata.com/resource/9fxf-t2tr.json")
r <- read_csv("https://www.dallasopendata.com/resource/rzdq-hjbp.csv")
raw <- read_csv("https://www.dallasopendata.com/api/views/tqs9-xfzb/rows.csv?accessType=DOWNLOAD")


#san diego
#does not have lon and lat
#r <- read_csv("http://seshat.datasd.org/pd/pd_collisions_datasd_v1.csv")
#g <- read_json("http://seshat.datasd.org/sde/pd/pd_beats_datasd.geojson")
#library(tidygeocoder)
#r2 <- r %>% mutate(address=paste(address_number_primary, address_road_primary, "San Diego", "CA"))
#r3 <- geocode(r2,address)
  
#####################################################################################################
#put all of the cities together
daily <- bind_rows(seattle, denver, la, nyc, austin, chicago, cincinnati, boston, dc)
table(daily$city)
write_csv(daily, "daily_crashes.csv", na="")
map <- bind_rows(la2,denver2,nyc2, austin2, chicago2, seattle2, cincinnati2, boston2, dc2) %>%
  group_by(city) %>%
  #for Austin Denver and Cincinnati some locations seem too far off so I eliminate them
  filter(!((city=="Austin" | city=="Denver" | city=="Cincinnati") &
             lat>quantile(lat,.03) & lat>quantile(lat,.97) & lon<quantile(lon,.03) & lon>quantile(lon,.97)))
table(map$city)
write_csv(map, "map.csv", na="")
