##################
# SETTING UP ENVIRONMENT
##################

# Sys.getenv("TZ")
Sys.setenv(TZ = "Pacific/Honolulu")
# Sys.getenv()

# set path to working folder
setwd("~/Google Drive/SGD/")
library("xts")
library("gridExtra")
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

#create hourly time stamps
time.index.60 <- seq(from = ymd_hms(paste(date(first(dataAll.sniff.df$sniff.td)), hms::as.hms(0))), 
                     to = ymd_hms(paste(date(last(dataAll.sniff.df$sniff.td)), hms::as.hms(60 * 60 * 24))), 
                     by = "1 hour") %>% force_tz("Pacific/Honolulu")

# combine data with hourly time stamps
dataAll.sniff.df <- dataAll.sniff.df %>%
  full_join(as_tibble(x = list(sniff.td = time.index.60))) %>%
  arrange(sniff.td)

# convert data to xts
dataAll.sniff.xts <- xts(x = select(dataAll.sniff.df, Rn), 
                         order.by = ymd_hms(dataAll.sniff.df$sniff.td, tz = "Pacific/Honolulu"))

# find 3 consequtive NAs and find the range of the NAs
cons.na <-
  is.na(lag(dataAll.sniff.xts)) &
  is.na(dataAll.sniff.xts) &
  is.na(lag(dataAll.sniff.xts, k = -1))
cons.na <- lag(cons.na) | lag(cons.na, k = -1)

# interpolate xts to hourly series
#dataAll.sniff.xts <- na.spline(dataAll.sniff.xts)
dataAll.sniff.xts <- na.approx(dataAll.sniff.xts)

# set consequtive NAs to NAs
dataAll.sniff.xts[cons.na] <- NA

# combine time with data from xts
dataAll.sniff60.df <-
  as_tibble(x = list(interv60.td = time.index.60)) %>%
  inner_join(as.tibble(list(
    interv60.td = ymd_hms(dataAll.sniff.df$sniff.td, tz = "Pacific/Honolulu"),
    Rn60 = drop(coredata(dataAll.sniff.xts))))) %>%
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

# convert to xts
# ctd.xts <- xts(x = select(dataAll.ctd.df, temp.ctd:sal.ctd), order.by = dataAll.ctd.df$Date)
# tzone(ctd.xts)

# aggregate observations within an hour using dplyr
dataAll.ctd60.df <- dataAll.ctd.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(Date, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(temp.ctd60.mean = mean(temp.ctd),
            sal.ctd60.mean = mean(sal.ctd)) %>%
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

# convert to xts
# owl.xts <- xts(x = select(dataAll.owl.df, water.owl:sigma.owl), order.by = dataAll.owl.df$owl.td)
# tzone(owl.xts)

# aggregate observations within an hour using dplyr
dataAll.owl60.df <- dataAll.owl.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(owl.td, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(water.owl60.mean = mean(water.owl),
            sigma.owl60.mean = mean(sigma.owl)) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu")
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

# convert to xts
# pond.xts <- xts(x = select(dataAll.pond.df, temp.pond:depth.pond), order.by = dataAll.pond.df$Date)
# tzone(pond.xts)

# aggregate observations within an hour using dplyr
dataAll.pond60.df <- dataAll.pond.df %>%
  # create factor levels for each hour
  mutate(interv60.td = cut(Date, "1 hour", right = TRUE)) %>%
  # group and summarize data based on factor levels
  group_by(interv60.td) %>%
  summarise(
    temp.pond60.mean = mean(temp.pond),
    sal.pond60.mean = mean(sal.pond),
    depth.pond60.mean = mean(depth.pond)
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
  summarise(gw60.mean = mean(gwlevel)) %>%
  # convert factor levels to datetime
  mutate(
    interv60.td = ymd_hms(interv60.td) + 60 * 60,
    interv60.td = force_tz(interv60.td, "Pacific/Honolulu")
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

# define a (24 hour) backward moving average function
ma <- function(x, n = 24) {
  stats::filter(x, rep(1 / n, n), sides = 1)
}

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
for (var.i in 2:21) {
  #var.i = 12
  
  # plot the data (grey), 24 hour moving average (blue), and missing values (red)
  data.plot <-
    ggplot(
      data = dataAll.60.df,
      mapping = aes(x = dataAll.60.df$interv60.td,
                    y = dataAll.60.df[[var.i]])
    ) +
    geom_line(color = "grey70") +
    geom_line(y = ma(dataAll.60.df[[var.i]]),
              color = "blue",
              alpha = 0.8) +
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
    color = "red") +
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

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df) +
  geom_point(mapping = aes(
    x = interv60.td,
    y = Rn60,
    #color = sal.ctd60.mean
    color = gw60.mean
  )) +
  scale_color_gradientn(colors = rainbow(10)) +
  ggtitle("Rn-water") + xlab("time") + ylab("Rn")

# render the plot
print(data.plot)

# plot the data
data.plot <-
  ggplot(data = dataAll.60.df) +
  geom_point(mapping = aes(x = water.owl60.mean-2*lag(water.owl60.mean)+lag(water.owl60.mean, 2),
                           y = Rn60,
                           #color = sal.ctd60.mean
                           color = water.owl60.mean)) +
  scale_color_gradientn(colors = rainbow(5)) +
  geom_smooth(mapping = aes(x = water.owl60.mean-lag(water.owl60.mean),
                            y = Rn60), color = "red") +
  ggtitle("Rn-water") + xlab("water") + ylab("Rn")

# render the plot
print(data.plot)

##################
# TEST CODE BELOW THIS LINE
##################

plot(dataAll.60.df$airtempavg)
plot(dataAll.meteo.df$airtempavg)
plot(dataAll.60.df$precip[dataAll.60.df$precip<0])

# spectal decomposition
# seasonal decomposition
# rising, falling tide
# correlogram lags, leads
# 
