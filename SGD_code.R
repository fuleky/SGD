##################
# SETTING UP ENVIRONMENT
##################

rm(list = ls())
# Sys.getenv("TZ")
Sys.setenv(TZ = "Pacific/Honolulu")
# Sys.getenv()

# set path to working folder
setwd("~/Google Drive/SGD/")
library("xts")
library("tidyverse")
library("lubridate")

##################
# SNIFFER
##################

# load sniffer data
data.kb2014.df <-
  read_table2(file = "kb2014_sniffer.txt", na = c("", "NA", "-"))
# data.kb20141.df <- read_table2(file = "kb2014_sniffer1.txt", na = c("", "NA", "-"))
data.kb2015.df <-
  read_table2(file = "kb2015_sniffer.txt", na = c("", "NA", "-"))
data.kb2016.df <-
  read_table2(file = "kb2016_sniffer.txt", na = c("", "NA", "-"))
data.kb2017.df <-
  read_table2(file = "kb2017_sniffer.txt", na = c("", "NA", "-"))

# combine data into one data frame
dataAll.sniff.df <-
  bind_rows(data.kb2014.df,
            data.kb2015.df,
            data.kb2016.df,
            data.kb2017.df)

# remove data where Counting is <3000 and >4000 as those indicate measurement
# problem, i.e. electric spike in the system or existing file but missing
# measurement values (e.g. in July and Aug 2016)
dataAll.sniff.df <- dataAll.sniff.df %>%
  filter(Counting > 3000 & Counting < 4000)

# find the maximum value in column: max(dataAll.sniff.df$Counting)

# calculate cps of Bi-214 609 keV peak so divide AreaB by Counting
# calculate cps of K-40 1460 keV peak so divide AreaK by Counting
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(Rn = AreaB / Counting / 0.16746637*1000,
                                                cpsK = AreaK / Counting)

# replace NA in Rn to 0.001116442
# replace NA in cpsK to 0.2 but decided that this is not needed
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(Rn = if_else(is.na(Rn), 0.001116442, Rn))
# cpsK = if_else(is.na(cpsK), 0.2, cpsK)

# get sniffer datetimes
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(
  sniff.td = paste(Year, "-", match(Month, month.abb), "-", Date, sep = ""),
  # sniff.td = as.Date(sniff.td, "%Y-%m-%d", tz="Pacific/Honolulu"),
  sniff.td = ymd(sniff.td),
  sniff.td = paste(sniff.td, Time),
  # sniff.td = as.POSIXct(sniff.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
  sniff.td = ymd_hms(sniff.td),
  sniff.td = force_tz(sniff.td, "Pacific/Honolulu")
)

# create hourly time stamps
time.index.60 <- seq(from = ymd_hms(paste(date(first(dataAll.sniff.df$sniff.td)), hms::as.hms(0))), 
                     to = ymd_hms(paste(date(last(dataAll.sniff.df$sniff.td)), hms::as.hms(60 * 60 * 24))), 
                     by = "1 hour") %>% force_tz("Pacific/Honolulu")

# combine Rn data with hourly time stamps
dataAll.sniff60.df <- dataAll.sniff.df %>%
  select(sniff.td, Rn) %>%
  full_join(as_tibble(x = list(sniff.td = time.index.60))) %>%
  arrange(sniff.td)

# find 3 consequtive NAs and find the range of the NAs
cons.na <-
  is.na(lag(dataAll.sniff60.df$Rn)) &
  is.na(dataAll.sniff60.df$Rn) &
  is.na(lead(dataAll.sniff60.df$Rn))
cons.na <- lag(cons.na) | lead(cons.na)

# convert data to xts
dataAll.sniff60.xts <- xts(x = select(dataAll.sniff60.df, Rn), 
                         order.by = ymd_hms(dataAll.sniff60.df$sniff.td, tz = "Pacific/Honolulu"))

# interpolate xts to hourly series
#dataAll.sniff60.xts <- na.spline(dataAll.sniff60.xts, na.rm = FALSE)
dataAll.sniff60.xts <- na.approx(dataAll.sniff60.xts, na.rm = FALSE)

# set consequtive NAs to NAs
dataAll.sniff60.xts[cons.na] <- NA

# combine time with data from xts
dataAll.sniff60.df <-
  as_tibble(x = list(interv60.td = time.index.60)) %>%
  inner_join(as.tibble(list(
    interv60.td = index(dataAll.sniff60.xts),
    Rn60 = drop(coredata(dataAll.sniff60.xts))))) %>%
  arrange(interv60.td)

# # aggregate observations within an hour using dplyr
# dataAll.sniff60.df <- dataAll.sniff.df %>%
#   # create factor levels for each hour
#   mutate(interv60.td = cut(sniff.td, "1 hour", right = TRUE)) %>%
#   # group and summarize data based on factor levels
#   group_by(interv60.td) %>%
#   summarise(
#     Rn60 = mean(Rn)
#   ) %>%
#   # convert factor levels to datetime
#   mutate(
#     interv60.td = ymd_hms(interv60.td) + 60 * 60,
#     interv60.td = force_tz(interv60.td, "Pacific/Honolulu")
#   ) %>%
#   # order data based on time
#   arrange(interv60.td)
# 
# # fill in skipped hours in Rn variable by averaging over the previous and next observation
# # create a variable containing regular hourly time stamps
# time.index.60 <- seq(
#   from = first(dataAll.sniff60.df$interv60.td),
#   to = last(dataAll.sniff60.df$interv60.td),
#   by = "1 hour"
# ) %>% force_tz("Pacific/Honolulu")
# # merge the data with regular time stamps
# dataAll.sniff60.df <- dataAll.sniff60.df %>%
#   full_join(as_tibble(x = list(interv60.td = time.index.60))) %>%
#   arrange(interv60.td)
# # calculate the average pre-and-post time t in a temporary variable Rn60.fill
# Rn60.fill = (lag(dataAll.sniff60.df$Rn60) + lead(dataAll.sniff60.df$Rn60)) / 2
# # fill in the values for the NA
# dataAll.sniff60.df$Rn60[!complete.cases(dataAll.sniff60.df$Rn60)] <-
#   Rn60.fill[!complete.cases(dataAll.sniff60.df$Rn60)]
# # remove the temporary variable Rn60.fill from the data set
# rm(Rn60.fill)

##################
# CTD OCEAN
##################

# load CTD data from sniffer location, CTD attached to floating sniffer frame
data.ctd2014.df <- read_csv(
  file = "CTD2014.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)
data.ctd2015.df <- read_csv(
  file = "CTD2015.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)
data.ctd2016.df <- read_csv(
  file = "CTD2016.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)
data.ctd2017.df <- read_csv(
  file = "CTD2017.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)

# combine data into one data frame
dataAll.ctd.df <-
  bind_rows(data.ctd2014.df,
            data.ctd2015.df,
            data.ctd2016.df,
            data.ctd2017.df)

# get rid of incomplete cases
dataAll.ctd.df <- dataAll.ctd.df %>%
  filter(complete.cases(dataAll.ctd.df))

# set column names
dataAll.ctd.df <-
  rename(dataAll.ctd.df, temp.ctd = Temperature, sal.ctd = Salinity)

# aggregate observations within an hour using dplyr
dataAll.ctd60.df <- dataAll.ctd.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(Date, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(temp.ctd60 = mean(temp.ctd),
            sal.ctd60 = mean(sal.ctd)) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu")
  ) %>%
  # order data based on time
  arrange(interv60.td)

##################
# OCEAN WATER LEVEL
##################

# load owl data: start with first file and append the rest
dataAll.owl.df <-
  read_csv(
    file = "OceWL/CO-OPS__1617433__wl-1.csv",
    col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")),
    locale = locale(tz = "Pacific/Honolulu")
  )
for (owl.i in 2:41) {
  # assign(paste0("data.", owl.i, ".df"),
  #        read_csv(file = paste0("OceWL/CO-OPS__1617433__wl-", owl.i, ".csv"),
  #                 col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")),
  #                 locale = locale(tz="Pacific/Honolulu")))
  dataAll.owl.df <-
    bind_rows(dataAll.owl.df,
              read_csv(
                file = paste0("OceWL/CO-OPS__1617433__wl-", owl.i, ".csv"),
                col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")),
                locale = locale(tz = "Pacific/Honolulu")
              ))
}

# set column names
dataAll.owl.df <-
  rename(
    dataAll.owl.df,
    owl.td = `Date Time`,
    water.owl = `Water Level`,
    sigma.owl = Sigma
  )

# aggregate observations within an hour using dplyr
dataAll.owl60.df <- dataAll.owl.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(owl.td, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(water.owl60 = mean(water.owl),
            sigma.owl60 = mean(sigma.owl)) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu"),
    # cludge fix: the first observation has NA time: replace it with first obs.
    interv60.td =  ifelse(
      is.na(interv60.td),
      ymd_hms(paste(ymd(first(dataAll.owl.df$owl.td)), hms::hms(0)), tz = "Pacific/Honolulu"),
      interv60.td),
    interv60.td = as_datetime(interv60.td, tz = "Pacific/Honolulu")
  ) %>%
  # order data based on time
  arrange(interv60.td)

##################
# CTD POND
##################

# load CTD data from pond location
# pond data collected in coastal pond to represEnt coastal aquifer response to tides and terrestrial hydrology
data.pond2015.df <- read_csv(
  file = "pond2015.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)
data.pond2016.df <- read_csv(
  file = "pond2016.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)
data.pond2017.df <- read_csv(
  file = "pond2017.csv",
  col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")),
  locale = locale(tz = "Pacific/Honolulu")
)

# combine data into one data frame
dataAll.pond.df <-
  bind_rows(data.pond2015.df, data.pond2016.df, data.pond2017.df)

# set column names
dataAll.pond.df <-
  rename(
    dataAll.pond.df,
    temp.pond = Temperature,
    sal.pond = Salinity,
    depth.pond = Depth
  )

# aggregate observations within an hour using dplyr
dataAll.pond60.df <- dataAll.pond.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(Date, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(
    temp.pond60 = mean(temp.pond),
    sal.pond60 = mean(sal.pond),
    depth.pond60 = mean(depth.pond)
  ) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu")
  ) %>%
  # order gwlevel data based on time
  arrange(interv60.td)

##################
# GROUND WATER
##################

# load groundwater level data - data from USGS Kalaoa N. Kona well
# the code below includes diagnostics (some of it is commented out)
# in the next few steps analyze/compare the various gw data sets and plot them

# can grab column names from txt file
# dataAll.gw10.df <- read_table2(file = "GW10.txt",
#                             col_names = c("agency,cd", "site.no", "date", "time", "tz.cd", "gwlevel",
#                                           "gwlevel.cd", "wtemp", "wtemp.cd", "cond", "cond.cd"),
#                             na = c("", "NA", "--"), skip = 31)
# dataAll.gw15old.df <- read_table2(file = "GW15old.txt",
#                                col_names = c("agency.cd", "site.no", "date", "time", "tz.cd", "gwlevel",
#                                              "gwlevel.cd"),
#                                na = c("", "NA", "--"), skip = 29)
dataAll.gw15.df <- read_table2(
  file = "GW15.txt",
  col_names = c(
    "agency.cd",
    "site.no",
    "date",
    "time",
    "tz.cd",
    "gwlevel",
    "gwlevel.cd"
  ),
  na = c("", "NA", "--"),
  skip = 29
)

# get gwlevel datetimes
# dataAll.gw10.df <- dataAll.gw10.df %>% mutate(
#   gw10.td = paste(date, time),
#   # gw10.td = as.POSIXct(gw10.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
#   gw10.td = ymd_hms(gw10.td),
#   gw10.td = force_tz(gw10.td, "Pacific/Honolulu")
# )
# dataAll.gw15old.df <- dataAll.gw15old.df %>% mutate(
#   gw15old.td = paste(date, time),
#   # gw15old.td = as.POSIXct(gw15old.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
#   gw15old.td = ymd_hms(gw15old.td),
#   gw15old.td = force_tz(gw15old.td, "Pacific/Honolulu")
# )
dataAll.gw15.df <- dataAll.gw15.df %>% mutate(
  gw15.td = paste(date, time),
  # gw15.td = as.POSIXct(gw15.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
  gw15.td = ymd_hms(gw15.td),
  gw15.td = force_tz(gw15.td, "Pacific/Honolulu")
)

# create timeline at the given frequency
# time.index.10 <- seq(from=ymd_hms(first(dataAll.gw10.df$gw10.td)),
#                      to=ymd_hms(last(dataAll.gw10.df$gw10.td)), by='10 min') %>% force_tz("Pacific/Honolulu")
# time.index.15old <- seq(from=first(dataAll.gw15old.df$gw15old.td),
#                         to=last(dataAll.gw15old.df$gw15old.td), by='15 min') %>% force_tz("Pacific/Honolulu")
# time.index.15 <- as.POSIXct(seq(from=unclass(first(dataAll.gw15.df$gw15.td)),
#                                 to=unclass(last(dataAll.gw15.df$gw15.td)), by=15*60),
#                             origin="1970-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
time.index.15 <- seq(
  from = first(dataAll.gw15.df$gw15.td),
  to = last(dataAll.gw15.df$gw15.td),
  by = "15 min"
) %>% force_tz("Pacific/Honolulu")

# merge the data with regular time stamps
# dataAll.gw10.df <- dataAll.gw10.df %>% full_join(as_tibble(x=list(gw10.td=time.index.10)))
# dataAll.gw15old.df <- dataAll.gw15old.df %>% full_join(as_tibble(x=list(gw15old.td=time.index.15old)))
dataAll.gw15.df <- dataAll.gw15.df %>%
  full_join(as_tibble(x = list(gw15.td = time.index.15))) %>%
  arrange(gw15.td)

# find times with no data
# dataAll.gw10.na <- dataAll.gw10.df %>%
#   filter(is.na(dataAll.gw10.df$gwlevel))
# dataAll.gw15old.na <- dataAll.gw15old.df %>%
#   filter(is.na(dataAll.gw15old.df$gwlevel))
# dataAll.gw15.na <- dataAll.gw15.df %>%
#   filter(is.na(dataAll.gw15.df$gwlevel))

# plot times with no data
# plot(x=dataAll.gw10.df$gw10.td, y=as.numeric(is.na(dataAll.gw10.df$gwlevel)))
# points(x=dataAll.gw15old.df$gw15old.td, y=as.numeric(is.na(dataAll.gw15old.df$gwlevel)), col="red")
# plot(x=dataAll.gw15.df$gw15.td, y=as.numeric(is.na(dataAll.gw15.df$gwlevel)))

# based on the comparison of gw data sets decided to work with gw15

# make sure data is ordered by time
dataAll.gw15.df <- arrange(dataAll.gw15.df, gw15.td)

# create a 4-period (hourly) moving average
# dataAll.gw15.df <- dataAll.gw15.df %>% mutate(
#   gw15.ma = rollapplyr(data=gwlevel, width=4, mean, fill = NA)
# )

# aggregate observations within an hour using baseR
# dataAll.gw60.df <- aggregate(list(gwlevel = dataAll.gw15.df$gwlevel),
#                              list(interv60.td = cut(dataAll.gw15.df$gw15.td, "1 hour")), mean)

# aggregate observations within an hour using dplyr
dataAll.gw60.df <- dataAll.gw15.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(gw15.td, "1 hour", right = TRUE)) %>%
  # group and summarize gwlevel data based on factor levels
  group_by(interv60.td) %>%
  summarise(gw60 = mean(gwlevel)) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu"),
    # cludge fix: the first observation has NA time: replace it with first obs.
    interv60.td =  ifelse(
      is.na(interv60.td),
      ymd_hms(paste(ymd(first(dataAll.gw15.df$gw15.td)), hms::hms(0)), tz = "Pacific/Honolulu"),
      interv60.td),
    interv60.td = as_datetime(interv60.td, tz = "Pacific/Honolulu")
    ) %>%
  # order gwlevel data based on time
  arrange(interv60.td)

##################
# METEOROLOGY
##################

# load meteorogical data - data from Puu Waawaa Hawaii
# can grab column names from txt file
dataAll.meteo.df <-
  read_table2(
    file = "MeteoPWW.txt",
    col_names = c(
      "date",
      "year",
      "day",
      "run",
      "solrad",
      "windspavg",
      "winddir",
      "windspgust",
      "airtempavg",
      "airtempmax",
      "airtempmin",
      "relhumavg",
      "relhummax",
      "relhummin",
      "precip"
    ),
    na = c("", "NA", "--", -9999),
    skip = 6
  )

dataAll.meteo.df <- dataAll.meteo.df %>%
  mutate(date = mdy(date),
         date = ymd_hms(paste(date, hms::as.hms(0)), tz = "Pacific/Honolulu"))

#create hourly time stamps
time.index.60 <- seq(from = ymd_hms(paste(first(dataAll.meteo.df$date), hms::as.hms(0))), 
                     to = ymd_hms(paste(last(dataAll.meteo.df$date), hms::as.hms(60 * 60 * 24))), 
                     by = "1 hour") %>% force_tz("Pacific/Honolulu")

# combine daily data with hourly time stamps
dataAll.meteo60.df <- dataAll.meteo.df %>%
  full_join(as_tibble(x = list(date = time.index.60))) %>%
  arrange(date)

# convert data to xts
dataAll.meteo60.xts <-
  xts(x = select(dataAll.meteo60.df,-(date:run)), 
      order.by = ymd_hms(dataAll.meteo60.df$date, tz = "Pacific/Honolulu"))

# interpolate xts to hourly series
#dataAll.meteo60int.xts <- na.spline(dataAll.meteo60.xts)
dataAll.meteo60int.xts <- na.approx(dataAll.meteo60.xts)

# combine time with data from xts
dataAll.meteo60.df <-
  bind_cols(as.tibble(x = list(interv60.td = index(dataAll.meteo60int.xts))), 
            as.tibble(dataAll.meteo60int.xts))

##################
# CREATE COMBINED DATA SET OF HOURLY OBSERVATIONS
##################

# combine all relevant variables at the hourly frequency
# (gw60 has a time stamp at every hour in the sample due to time.index.15)
dataAll.60.df <- dataAll.gw60.df %>%
  full_join(dataAll.pond60.df) %>%
  full_join(dataAll.owl60.df) %>%
  full_join(dataAll.ctd60.df) %>%
  full_join(dataAll.sniff60.df) %>%
  full_join(dataAll.meteo60.df) %>%
  arrange(interv60.td)

##################
# ADD TRANSFORMATIONS OF THE DATA
##################

# define a (24 hour) backward moving average function
ma <- function(x, q = 24) {
  stats::filter(x, rep(1 / q, q), sides = 1)
}

# calculate transformations and save them in the same variable
dataAll.60.df <- dataAll.60.df %>% mutate(
  # change/slope of owl (backward and centered)
  chg.b.owl = water.owl60 - lag(water.owl60),
  chg.c.owl = (lead(water.owl60) - lag(water.owl60)) / 2,
  # change^2/curvature of owl (backward and centered)
  chg2.b.owl =
    water.owl60 - 2 * lag(water.owl60) + lag(water.owl60, 2),
  chg2.c.owl =
    lead(water.owl60) - 2 * water.owl60 + lag(water.owl60),
  # slope/relative change of owl
  slop.b6.owl = (water.owl60 - (4 * lag(water.owl60, 6) + 1 * lag(water.owl60, 7)) / 5) / 
    ((water.owl60 + (4 * lag(water.owl60, 6) + 1 * lag(water.owl60, 7)) / 5) / 2),
  # curvature/relative change^2 of owl
  curv.b6.owl = ((water.owl60 - (4 * lag(water.owl60, 6) + 1 * lag(water.owl60, 7)) / 5) -
                   (lag(water.owl60, 1) - (4 * lag(water.owl60, 7) + 1 * lag(water.owl60, 8)) / 5)) / 
    ((((water.owl60 + (4 * lag(water.owl60, 6) + 1 * lag(water.owl60, 7)) / 5) / 2) + 
        ((lag(water.owl60, 1) + (4 * lag(water.owl60, 7) + 1 * lag(water.owl60, 8)) / 5) / 2)) / 2),
  # slope/relative change of Rn
  slop.b6.Rn = (Rn60 - (4 * lag(Rn60, 6) + 1 * lag(Rn60, 7)) / 5) / 
    ((Rn60 + (4 * lag(Rn60, 6) + 1 * lag(Rn60, 7)) / 5) / 2),
  slop.b.Rn = (Rn60 - lag(Rn60)) / ((Rn60 + lag(Rn60)) / 2),
  # relative change of gw
  slop.b6.gw = (gw60 - (4 * lag(gw60, 6) + 1 * lag(gw60, 7)) / 5) / 
    ((gw60 + (4 * lag(gw60, 6) + 1 * lag(gw60, 7)) / 5) / 2),
  # gradient gw - water.owl60
  grad = gw60-water.owl60
)

# monthly averages
dataAll.mo.df <- dataAll.60.df %>% 
  mutate(calmon = month(interv60.td),
         calyear = year(interv60.td)) %>%
  # filter(complete.cases(.)) %>%
  group_by(calyear, calmon) %>%
  summarize(Rn.mo = mean(Rn60, na.rm = TRUE), gw.mo = mean(gw60, na.rm = TRUE), owl.mo = mean(water.owl60, na.rm = TRUE), precip.mo = mean(precip, na.rm = TRUE), grad.mo = mean(grad, na.rm = TRUE))
  
# remove Rn = NA from the dataset
dataAll.60.Rn.df <- dataAll.60.df %>% filter(!is.na(Rn60))

# get the longest sequence of continuous observations
dataAll.60.Rn.MaxLen.df <- dataAll.60.df %>%
  select(interv60.td, Rn60, water.owl60) %>%
  filter(with(rle(complete.cases(.)), 
              rep(lengths == max(lengths[values]) & values, lengths)))

##################
# SPECTRAL ANALYSIS
##################

# spectral analysis of dataAll.60.Rn.MaxLen.df 
Rn60.spec <- spectrum(as.ts(xts(x = dataAll.60.Rn.MaxLen.df$Rn60, 
                                order.by = ymd_hms(dataAll.60.Rn.MaxLen.df$interv60.td, 
                                                   tz = "Pacific/Honolulu"))),
                      log="no", span=3, demean = TRUE, detrend = TRUE, plot=FALSE)
# sampling interval in terms of days
samp.int <- 1 / (24*3600)
# cycles per day
Rn60.spec$scaledfreq <- Rn60.spec$freq / samp.int
# get variance under the curve
Rn60.spec$scaledspec <- 2 * Rn60.spec$spec
# combine columns in a new data frame
Rn60.scaledspec <- bind_cols(scaledfreq = Rn60.spec$scaledfreq, scaledper = 1/Rn60.spec$scaledfreq, scaledspec = Rn60.spec$scaledspec)

# spectral analysis of dataAll.60.Rn.MaxLen.df 
owl.spec <- spectrum(as.ts(xts(x = dataAll.60.Rn.MaxLen.df$water.owl60, 
                                order.by = ymd_hms(dataAll.60.Rn.MaxLen.df$interv60.td, 
                                                   tz = "Pacific/Honolulu"))),
                      log="no", span=3, demean = TRUE, detrend = TRUE, plot=FALSE)
# sampling interval in terms of days
# samp.int <- 1 / (24*3600)
# cycles per day
owl.spec$scaledfreq <- owl.spec$freq / samp.int
# get variance under the curve
owl.spec$scaledspec <- 2 * owl.spec$spec
# combine columns in a new data frame
owl.scaledspec <- bind_cols(scaledfreq = owl.spec$scaledfreq, scaledper = 1/owl.spec$scaledfreq, scaledspec = owl.spec$scaledspec)

##################
# EXPORT THE DATA
##################

# write_csv(dataAll.60.df, "dataAll60.csv")
# write_csv(select(dataAll.60.df, interv60.td, Rn60), "dataRn60.csv")
# write_csv(select(dataAll.60.df, interv60.td, Rn60, water.owl60), "dataRn60OWL.csv")

##################
# DIAGNOSTIC PLOT OF DATA
##################

# save the plots in a single pdf file
pdf(
  "plots.pdf",
  onefile = TRUE,
  width = 16 / 9 * 5,
  height = 5
)

# plot data in combined data set, check what is missing
for (var.i in 2:31) {
  #var.i = 12
  
  # plot the data (grey), 24 hour moving average (blue), and missing values (red)
  data.plot <-
    ggplot(
      data = dataAll.60.df,
      mapping = aes(x = dataAll.60.df$interv60.td,
                    y = dataAll.60.df[[var.i]])
    ) +
    geom_line(color = "grey70", 
              alpha = 0.5) +
    geom_line(y = ma(dataAll.60.df[[var.i]]),
              color = "blue",
              alpha = 0.5) +
    geom_linerange(mapping = aes(
      # ymin = 0,
      ymin = ifelse(
        is.na(dataAll.60.df[[var.i]]),
        mean(dataAll.60.df[[var.i]], na.rm = TRUE) -
          3 * sd(dataAll.60.df[[var.i]], na.rm = TRUE),
        NA
      ),
      ymax = ifelse(
        is.na(dataAll.60.df[[var.i]]),
        mean(dataAll.60.df[[var.i]], na.rm = TRUE) +
          3 * sd(dataAll.60.df[[var.i]], na.rm = TRUE),
        NA
      )
    ),
    color = "red", alpha = 0.2) +
    theme_minimal() + 
    ggtitle(names(dataAll.60.df)[var.i]) + xlab("time") + ylab("units")
  
  # save the plot with the name of the variable and then render it
  # assign(paste0(names(dataAll.60.df)[var.i], ".plot"), data.plot)
  # print(eval(parse(text = paste0(names(dataAll.60.df)[var.i], ".plot"))))
  
  # render the plot
  print(data.plot)
}

dev.off()

##################
# SAMPLE PLOTS OF DATA
##################

# save the plots in a single pdf file
pdf(
  "plots_poster.pdf",
  onefile = TRUE,
  # width = 16 / 9 * 5,
  # height = 5
  width = 6,
  height = 5
)

#dev.off()

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df) +
  geom_point(mapping = aes(
    x = interv60.td,
    y = Rn60,
    # color = ifelse(slop.b6.owl > -3.2, slop.b6.owl, -3.2)
    color = scale(water.owl60)
  ),
  alpha = 0.9) +
  geom_line(mapping = aes(
    x = interv60.td,
    y = ma(Rn60, 24)
  ),
  color = "red",
  alpha = 0.9) +
  geom_point(mapping = aes(
    x = interv60.td,
    y = ifelse(is.na(Rn60), NA, sal.ctd60 * 50 + 1000),
    color = scale(water.owl60)
  ),
  alpha = 0.9) +
  geom_line(mapping = aes(
    x = interv60.td,
    y = ifelse(is.na(Rn60), NA, ma(sal.ctd60, 24) * 50 + 1000)
  ),
  color = "black",
  alpha = 0.9) +
  scale_x_datetime(limits = c(first(dataAll.60.Rn.df$interv60.td), last(dataAll.60.Rn.df$interv60.td))) +
  scale_y_continuous(limits = c(0, 2500), sec.axis = sec_axis( ~ (. - 1000) / 50, name = "Salinity")) +
  scale_color_gradientn(colors = rev(topo.colors(5)),
                        guide = guide_colorbar(title = "Water level")
  ) +
  theme_minimal() + 
  # labs(title = "Rn-water-salinity", x = "Time", y = "Rn") +
  labs(x = "Time", y = "Rn") +
  theme(axis.title.y = element_text(color="red"),
        axis.title.y.right = element_text(color="black"), 
        legend.key.size = unit(0.4, "cm"),
        legend.position = c(.8,0.5))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.Rn.df) +
  geom_point(mapping = aes(
    x = chg.c.owl,
    # y = ifelse(chg2.c.owl > -0.12, chg2.c.owl, -0.12),
    y = chg2.c.owl,
    color = slop.b6.Rn
  ),
  alpha = 0.3) +
  geom_spoke(aes(x = 0, y = 0, angle = 0, radius = 0.2), color = "blue", size = 1) +
  geom_spoke(aes(x = 0, y = 0, angle = pi/2, radius = 0.1), color = "blue", size = 1) +
  geom_spoke(aes(x = 0, y = 0, angle = pi, radius = 0.2), color = "blue", size = 1) +
  geom_spoke(aes(x = 0, y = 0, angle = 3*pi/2, radius = 0.1), color = "blue", size = 1) +
  scale_x_continuous(limits = c(-0.2, 0.2)) + 
  scale_y_reverse(limits = c(0.1, -0.1)) +
  scale_color_gradientn(colors = heat.colors(5),
                        guide = guide_colorbar(title = "Rn")) +
  theme_minimal() + 
  ggtitle("Rn-water") + xlab("slope") + ylab("curvature")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.Rn.df) +
  geom_point(mapping = aes(
    x = interv60.td,
    y = water.owl60,
    color = slop.b6.Rn
  ),
  alpha = 0.3) +
  scale_color_gradientn(colors = rev(heat.colors(5)),
                        guide = guide_colorbar(title = "std Rn")) +
  theme_minimal() + 
  ggtitle("Rn-water") + xlab("time") + ylab("owl")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.Rn.df) +
  geom_point(mapping = aes(
    x = slop.b6.owl,
    y = slop.b6.Rn,
    color = slop.b6.gw
  ),
  alpha = 0.5) + 
  scale_x_continuous(limits = c(-2, 2)) + 
  scale_color_gradientn(colors = rev(topo.colors(5)),
                        guide = guide_colorbar(title = "gw")) +
  geom_smooth(mapping = aes(x = slop.b6.owl,
                            y = slop.b6.Rn), color = "red") +
  theme_minimal() + 
  ggtitle("Rn-water") + xlab("owl 6 hour slope") + ylab("Rn 6 hour slope")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.Rn.df) +
  geom_point(mapping = aes(
    x = grad,
    y = slop.b6.owl,
    color = slop.b6.Rn #log(Rn60)
  ),
  alpha = 0.3) + 
  scale_y_continuous(limits = c(-3, 3)) + 
  scale_color_gradientn(colors = rev(heat.colors(5)),
                        guide = guide_colorbar(title = "Rn 6 hour slope")) +
  theme_minimal() + 
  ggtitle("Rn-water") + xlab("grad") + ylab("owl 6 hour slope")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df, 
         mapping = aes(
           x = dataAll.60.df$interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$precip, q = 24)
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$gw60, q = 24) * 100 - 200
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. + 200) / 100, name = "Ground water level")) +
  theme_minimal() +
  labs(title = "Precipitation vs. Ground water level (24 hour ma)", x = "Time", y = "Precipitation") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df, 
         mapping = aes(
           x = dataAll.60.df$interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$precip, q = 24*30)
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$Rn60, q = 24*30) / 100
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. + 0) * 100, name = "Rn")) +
  theme_minimal() +
  labs(title = "Precipitation vs. Rn (30 day ma)", x = "Time", y = "Precipitation") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df, 
         mapping = aes(
           x = dataAll.60.df$interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$Rn60, q = 24 * 30)
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$sal.ctd60, q = 24 * 30) * 100 - 500
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. + 500) / 100, name = "Salinity")) +
  theme_minimal() +
  labs(title = "Rn vs. Salinity (30 day ma)", x = "Time", y = "Rn") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df, 
         mapping = aes(
           x = dataAll.60.df$interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$Rn60, q = 24 * 14)
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$water.owl60, q = 24 * 14) * 2000 + 1000
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. - 1000) / 2000, name = "Ocean water level")) +
  theme_minimal() +
  labs(title = "Rn vs. Ocean water level (14 day ma)", x = "Time", y = "Rn") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df, 
         mapping = aes(
           x = dataAll.60.df$interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$Rn60, q = 24 * 7)
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(dataAll.60.df$grad, q = 24 * 7) * 2000 - 4000
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. + 4000) / 2000, name = "Water gradient")) +
  theme_minimal() +
  labs(title = "Rn vs. Water gradient (7 day ma)", x = "Time", y = "Rn") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df %>% 
           filter(interv60.td >= "2014-04-01 00:00:00" & interv60.td < "2014-04-14 00:00:00"), 
         mapping = aes(
           x = interv60.td)) +
  geom_line(
    mapping = aes(
      y = Rn60
    ),
    color = "red", size = 1,
    alpha = 0.9
  ) +
  geom_line(
    mapping = aes(
      y = sal.ctd60 * 100
    ),
    color = "black", size = 1,
    alpha = 0.9
  ) +
  geom_line(
    mapping = aes(
      y = water.owl60 * 2000 + 1000
    ),
    color = "blue", size = 1,
    alpha = 0.9
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. - 1000) / 2000, name = "Ocean water level")) +
  theme_minimal() +
   labs(x = "Time", y = "Rn") +
  geom_text(label = "Salinity x 100", x = ymd_hms("2014-04-06 12:00:00"), y = 2800, colour = "black") +
  theme(axis.title.y = element_text(color="red"),
        axis.title.y.right = element_text(color="blue"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.Rn.MaxLen.df,
         mapping = aes(
           x = interv60.td)) +
  geom_line(
    mapping = aes(
      y = ma(Rn60, 48)
      # y = Rn60
    ),
    color = "blue",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      y = ma(water.owl60, 48) * 2000 + 500
      # y = water.owl60 * 2000 + 500
    ),
    color = "red",
    alpha = 0.5
  ) +
  scale_y_continuous(sec.axis = sec_axis( ~ (. - 500) / 2000, name = "Ocean water level")) +
  theme_minimal() +
  labs(title = "Rn vs. OWL (2 day ma)", x = "Time", y = "Rn") +
  theme(axis.title.y = element_text(color="blue"),
        axis.title.y.right = element_text(color="red"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = Rn60.scaledspec %>%
           filter(scaledfreq < 5) 
  ) +
  geom_line(
    mapping = aes(
      x = scaledfreq,
      y = scaledspec
    ),
    color = "blue",
    alpha = 0.5
  ) +
  theme_minimal() +
  labs(title = "Smoothed periodogram of Rn", x = "Frequency", y = "Spectral density")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = bind_cols(Rn60.scaledspec, owl.scaledspec) %>%
           filter(scaledfreq < 5) 
  ) +
  geom_line(
    mapping = aes(
      x = scaledfreq,
      y = scaledspec
    ),
    color = "red",
    size = 1,
    alpha = 0.9
  ) +
  geom_line(
    mapping = aes(
      x = scaledfreq1,
      y = -scaledspec1 * 200000
    ),
    color = "blue",
    size = 1,
    alpha = 0.9
  ) + 
  scale_y_continuous(sec.axis = sec_axis( ~ (. - 0) / (-200000), name = "- Spectral density ocean water level")) +
  theme_minimal() +
  # labs(title = "Smoothed periodogram of Rn and ocean water level", x = "Frequency (1/day)", y = "Spectral density Rn") +
  labs(x = "Frequency", y = "Spectral density Rn") +
  theme(axis.title.y = element_text(color="red"),
        axis.title.y.right = element_text(color="blue"))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = bind_cols(Rn60.scaledspec, owl.scaledspec) %>%
           filter(scaledper < 135) 
  ) +
  geom_line(
    mapping = aes(
      x = scaledper,
      y = scaledspec
    ),
    color = "red",
    alpha = 0.5
  ) +
  geom_line(
    mapping = aes(
      x = scaledper1,
      y = -scaledspec1 * 200000
    ),
    color = "blue",
    alpha = 0.5
  ) +
  theme_minimal() +
  labs(title = "Smoothed periodogram of Rn (red) and owl (-blue)", x = "Period in days", y = "Spectral density")

# render the plot
print(data.plot)

# cross-correlation function plot
plot.ccf <-
  function(var1,
           var2,
           lag.max = 100,
           ma = TRUE,
           q = 24,
           contseq = TRUE) {
    # get the variables of interest
    var1v <- eval(parse(text = paste0("dataAll.60.df$", var1)))
    var2v <- eval(parse(text = paste0("dataAll.60.df$", var2)))
    ccor.xts <- dataAll.60.df %>%
      # calculate n-period moving averages
      mutate(
        temp1 = stats::filter(var1v, rep(1 / q, q), sides = 1),
        temp2 = stats::filter(var2v, rep(1 / q, q), sides = 1)
      ) %>%
      # focus on the relevant data
      select(interv60.td, var1, var2, temp1, temp2) # %>% {
    if (contseq == TRUE) {
      # filter out the longest sequence of available observations
      ccor.xts <- ccor.xts %>% filter(with(rle(complete.cases(.)),
                                           rep(
                                             lengths == max(lengths[values]) & values, lengths
                                           )))
    } else {
      # filter out complete cases
      ccor.xts <- ccor.xts %>% filter(complete.cases(.))
    }
    #} %>%
    # make it an xts
    ccor.xts <-
      ccor.xts %>% xts(order.by = ymd_hms(.$interv60.td, tz = "Pacific/Honolulu"))
    # calculate the ccf for moving avereages or raw data
    if (ma) {
      ccf.out <- ccf(x = as.numeric(eval(parse(
        text = paste0("ccor.xts$", "temp1")
      ))),
      y = as.numeric(eval(parse(
        text = paste0("ccor.xts$", "temp2")
      ))),
      lag.max = lag.max)
    } else {
      ccf.out <- ccf(x = as.numeric(eval(parse(
        text = paste0("ccor.xts$", var1)
      ))),
      y = as.numeric(eval(parse(
        text = paste0("ccor.xts$", var2)
      ))),
      lag.max = lag.max)
    }
    # plot the data
    data.plot <-
      ggplot(data = bind_cols(list(lag = as.numeric(ccf.out$lag), acf = as.numeric(ccf.out$acf)))) +
      geom_col(mapping = aes(x = lag, y = acf), color = "blue", alpha = 0.5) +
      theme_minimal() +
      labs(title = "Cross-correlogram", x = paste("Lags of", var1), y = "Correlation")
      
      # render the plot
      print(data.plot)
    
    #return(ccf.out)
  }

plot.ccf(var1 = "water.owl60", var2 = "Rn60", lag.max = 100, ma = T, q = 24*30, contseq = TRUE)

# correlation plot
library(corrplot)
cor.mat <- dataAll.60.df %>%
  select(-interv60.td, -temp.pond60, -sal.pond60, -depth.pond60, -sigma.owl60, -(solrad:relhummin), -(chg.b.owl:slop.b6.gw)) %>%
  filter(complete.cases(.)) %>%
  cor()
colnames(cor.mat) <- c("gwl", "owl", "temp", "sal", "Rn", "precip", "grad")
corrplot.mixed(cor.mat, lower = "ellipse", upper = "number", tl.cex = 2, number.cex = 2)

# plot the data
data.plot <-
  ggplot(data = bind_cols(as.tibble(select(
    dataAll.mo.df, calmon
  )), as.tibble(scale(
    select(dataAll.mo.df,-calmon)
  ))),
  mapping = aes(x = month(calmon))) +
  geom_line(mapping = aes(y = gw.mo,
                          color = "1"),
            size = 1,
            alpha = 0.5) +
  geom_line(mapping = aes(y = owl.mo,
                          color = "2"),
            size = 1,
            alpha = 0.5) +
  geom_line(mapping = aes(y = precip.mo,
                          color = "3"),
            size = 1,
            alpha = 0.5) +
  geom_line(mapping = aes(y = grad.mo,
                          color = "4"),
            size = 1,
            alpha = 0.5) +
  geom_line(mapping = aes(y = Rn.mo,
                          color = "5"),
            size = 3,
            alpha = 0.5) +
  scale_color_discrete(name = "Averages",
                       labels = c("gw", "owl", "precip", "grad", "Rn")) +
  scale_x_continuous(breaks = 1:12) +
  # scale_x_discrete(breaks = 1:12, labels = as.character(month(dataAll.mo.df$calmon, label = TRUE))) +
  theme_minimal() +
  labs(title = "Seasonplot", x = "Month", y = "Average") +
  theme(legend.position = "bottom")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = bind_cols(as.tibble(select(
    dataAll.mo.df, calmon, calyear
  )), as.tibble(scale(
    select(dataAll.mo.df,-calmon)
  ))),
  mapping = aes(x = ymd(paste0(calyear, "-", calmon, "-1")))) +
  geom_line(mapping = aes(y = gw.mo,
                          color = "brown"),
            size = 1,
            alpha = 0.9) +
  geom_line(mapping = aes(y = owl.mo,
                          color = "darkblue"),
            size = 1.5,
            alpha = 0.9) +
  geom_line(mapping = aes(y = precip.mo,
                          color = "lightblue"),
            size = 1.5,
            alpha = 0.9) +
  geom_line(mapping = aes(y = grad.mo,
                          color = "green"),
            size = 1,
            alpha = 0.9) +
  geom_line(mapping = aes(y = Rn.mo,
                          color = "red"),
            size = 1.5,
            alpha = 0.9) +
  scale_color_manual(name = "Variables", values = c("brown", "darkblue", "lightblue", "green", "red"),
                       labels = c("Ground water level", "Ocean water level", "Precipitation", "Gradient", "Rn")) +
  theme_minimal() +
  labs(x = "Time", y = "Normalized monthly average") +
  theme(legend.position = c(0.25, 0.85))

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.mo.df,
  mapping = aes(x = precip.mo, y = Rn.mo)) +
  geom_point(color = "blue", alpha = 0.5) +
  geom_smooth(mapping = aes(x = precip.mo, y = Rn.mo), method = "lm", color = "red") +
  theme_minimal() +
  labs(title = "Rn vs precip", x = "Rainfall", y = "Rn")

# render the plot
print(data.plot)

# # save the plots in a single pdf file
pdf(
  "plots_poster.pdf",
  onefile = TRUE,
  # width = 16 / 9 * 5,
  # height = 5
  width = 6,
  height = 4
)

dev.off()

##################
# TEST CODE BELOW THIS LINE
##################

# spectal decomposition
# seasonal decomposition
# rising, falling tide
# correlogram lags, leads
# precip and gw in single time plot
# Rn and sal.ctd in single time plot
# Rn and water.owl in single time plot zoom into 1 week

library (EMD)
library (hht)

ee <- EEMD(dataAll.60.Rn.MaxLen.df$Rn60, 1:length(dataAll.60.Rn.MaxLen.df$interv60.td), 250, 100, 6, "trials")
eec <- EEMDCompile ("trials", 100, 6)
PlotIMFs(eec)
PlotIMFs(eec, c(1000,2000))

         