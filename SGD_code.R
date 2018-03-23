# Sys.getenv("TZ")
Sys.setenv(TZ="Pacific/Honolulu")
# Sys.getenv()

# set path to working folder
setwd("~/Google Drive/SGD/")
library("xts")
library("tidyverse")

# load sniffer data
data.kb2014.df <- read_table2(file = "kb2014_sniffer.txt", na = c("", "NA", "-"))
# data.kb20141.df <- read_table2(file = "kb2014_sniffer1.txt", na = c("", "NA", "-"))
data.kb2015.df <- read_table2(file = "kb2015_sniffer.txt", na = c("", "NA", "-"))
data.kb2016.df <- read_table2(file = "kb2016_sniffer.txt", na = c("", "NA", "-"))
data.kb2017.df <- read_table2(file = "kb2017_sniffer.txt", na = c("", "NA", "-"))

# combine data into one data frame
dataAll.sniff.df <- bind_rows(data.kb2014.df, data.kb2015.df, data.kb2016.df, data.kb2017.df)

# remove data where Counting is <3000 and >4000 as those indicate measurement
# problem, i.e. electric spike in the system or existing file but missing
# measurement values (e.g. in July and Aug 2016)
dataAll.sniff.df <- dataAll.sniff.df %>%
  filter(Counting>3000 & Counting<4000)

# find the maximum value in column: max(dataAll.sniff.df$Counting)

# calculate cps of Bi-214 609 keV peak so divide AreaB by Counting
# calculate cps of K-40 1460 keV peak so divide AreaK by Counting
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(
  cpsB = AreaB / Counting,
  cpsK = AreaK / Counting
)

# replace NA in cpsB to 0.1
# replace NA in cpsK to 0.2
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(
  cpsB = if_else(is.na(cpsB), 0.1, cpsB),
  cpsK = if_else(is.na(cpsK), 0.2, cpsK)
)

# get sniffer datetimes
dataAll.sniff.df <- dataAll.sniff.df %>% mutate(
  sniff.td = paste(Year, "-", match(Month,month.abb), "-", Date, sep = ""),
  sniff.td = as.Date(sniff.td, "%Y-%m-%d", tz="Pacific/Honolulu"),
  sniff.td = paste(sniff.td, Time),
  sniff.td = as.POSIXct(sniff.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
)

# convert to xts
sniff.xts <- xts(x = select(dataAll.sniff.df,cpsB:cpsK), order.by = dataAll.sniff.df$sniff.td)
# tzone(sniff.xts)

# load CTD data from sniffer location
data.ctd2014.df <- read_csv(file = "CTD2014.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))
data.ctd2015.df <- read_csv(file = "CTD2015.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))
data.ctd2016.df <- read_csv(file = "CTD2016.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))
data.ctd2017.df <- read_csv(file = "CTD2017.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))

# combine data into one data frame
dataAll.ctd.df <- bind_rows(data.ctd2014.df, data.ctd2015.df, data.ctd2016.df, data.ctd2017.df)

# get rid of incomplete cases
dataAll.ctd.df <- dataAll.ctd.df %>%
  filter(complete.cases(dataAll.ctd.df))

# set column names
dataAll.ctd.df <- rename(dataAll.ctd.df, temp.ctd = Temperature, sal.ctd = Salinity)

# convert to xts
ctd.xts <- xts(x = select(dataAll.ctd.df, temp.ctd:sal.ctd), order.by = dataAll.ctd.df$Date)
# tzone(ctd.xts)

# load owl data: start with first file and append the rest
dataAll.owl.df <- read_csv(file = "OceWL/CO-OPS__1617433__wl-1.csv", col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")), locale = locale(tz="Pacific/Honolulu"))
for (owl.i in 2:41) {
#  assign(paste0("data.", owl.i, ".df"), read_csv(file = paste0("OceWL/CO-OPS__1617433__wl-", owl.i, ".csv"), col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")), locale = locale(tz="Pacific/Honolulu")))
  bind_rows(dataAll.owl.df, read_csv(file = paste0("OceWL/CO-OPS__1617433__wl-", owl.i, ".csv"), col_types = cols(`Date Time` = col_datetime(format = "%Y-%m-%d %H:%M")), locale = locale(tz="Pacific/Honolulu")))
}

# set column names
dataAll.owl.df <- rename(dataAll.owl.df, water.owl = `Water Level`, sigma.owl = Sigma)

# convert to xts
owl.xts <- xts(x = select(dataAll.owl.df, water.owl:sigma.owl), order.by = dataAll.owl.df$`Date Time`)
# tzone(owl.xts)

# load CTD data from pond location
# pond data - data collected in coastal pond to represnt coastal aquifer response to tides and terrestrial hydrology
data.pond2015.df <- read_csv(file = "pond2015.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))
data.pond2016.df <- read_csv(file = "pond2016.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))
data.pond2017.df <- read_csv(file = "pond2017.csv", col_types = cols(Date = col_datetime(format = "%m/%d/%Y %H:%M:%S")), locale = locale(tz="Pacific/Honolulu"))

# combine data into one data frame
dataAll.pond.df <- bind_rows(data.pond2015.df, data.pond2016.df, data.pond2017.df)

# set column names
dataAll.pond.df <- rename(dataAll.pond.df, temp.pond = Temperature, sal.pond = Salinity, depth.pond = Depth)

# convert to xts
pond.xts <- xts(x = select(dataAll.pond.df, temp.pond:depth.pond), order.by = dataAll.pond.df$Date)
# tzone(pond.xts)

# load groundwater level data - data from USGS Kalaoa N. Kona well
# can grab column names from txt file
data.gw10.df <- read_table2(file = "GW10.txt", col_names = c("agency,cd", "site.no", "date", "time", "tz.cd", "gwlevel", "gwlevel.cd", "wtemp", "wtemp.cd", "cond", "cond.cd"), na = c("", "NA", "--"), skip = 31)
data.gw15.df <- read_table2(file = "GW15.txt", col_names = c("agency.cd", "site.no", "date", "time", "tz.cd", "gwlevel", "gwlevel.cd"), na = c("", "NA", "--"), skip = 29)
data.gw15old.df <- read_table2(file = "GW15old.txt", col_names = c("agency.cd", "site.no", "date", "time", "tz.cd", "gwlevel", "gwlevel.cd"), na = c("", "NA", "--"), skip = 29)

# get gwlevel datetimes
data.gw10.df <- data.gw10.df %>% mutate(
  gw10.td = paste(date, time),
  gw10.td = as.POSIXct(gw10.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
)
data.gw15.df <- data.gw15.df %>% mutate(
  gw15.td = paste(date, time),
  gw15.td = as.POSIXct(gw15.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
)
data.gw15old.df <- data.gw15old.df %>% mutate(
  gw15old.td = paste(date, time),
  gw15old.td = as.POSIXct(gw15old.td, format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
)

# create continuous timeline at the given frequency
time.index.10 <- as.POSIXct(seq(from=unclass(first(data.gw10.df$gw10.td)), to=unclass(last(data.gw10.df$gw10.td)), by=10*60), origin="1970-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
time.index.15 <- as.POSIXct(seq(from=unclass(first(data.gw15.df$gw15.td)), to=unclass(last(data.gw15.df$gw15.td)), by=15*60), origin="1970-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")
time.index.15old <- as.POSIXct(seq(from=unclass(first(data.gw15old.df$gw15old.td)), to=unclass(last(data.gw15old.df$gw15old.td)), by=15*60), origin="1970-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S", tz="Pacific/Honolulu")

# merge the data with regular time stamps
data.gw10.df <- data.gw10.df %>% full_join(as_tibble(x=list(gw10.td=time.index.10)))
data.gw15.df <- data.gw15.df %>% full_join(as_tibble(x=list(gw15.td=time.index.15)))
data.gw15old.df <- data.gw15old.df %>% full_join(as_tibble(x=list(gw15old.td=time.index.15old)))

# find times with no data
data.gw10.na <- data.gw10.df %>% 
  filter(is.na(data.gw10.df$gwlevel))
data.gw15.na <- data.gw15.df %>% 
  filter(is.na(data.gw15.df$gwlevel))
data.gw15old.na <- data.gw15old.df %>% 
  filter(is.na(data.gw15old.df$gwlevel))

# plot times with no data
plot(x=data.gw10.df$gw10.td, y=as.numeric(is.na(data.gw10.df$gwlevel)))
plot(x=data.gw15.df$gw15.td, y=as.numeric(is.na(data.gw15.df$gwlevel)))
points(x=data.gw15old.df$gw15old.td, y=as.numeric(is.na(data.gw15old.df$gwlevel)), col="red")

# work with gw15; create a 4-period (hourly) moving average
data.gw15.df <- arrange(data.gw15.df, gw15.td)
data.gw15.df <- data.gw15.df %>% mutate(
  gw15.ma = rollapplyr(data=gwlevel, width=4, mean, fill = NA)
)
# 
#

###################

# convert to xts
gw10.xts <- xts(x = select(data.gw10.df, c(gwlevel, wtemp, cond)), order.by = data.gw10.df$gw10.td)
gw15.xts <- xts(x = select(data.gw15.df, gwlevel), order.by = data.gw15.df$gw15.td)
# tzone(gw10.xts)
# tzone(gw15.xts)
# indexTZ(gw15.xts)


#backward moving average of CTD data
temp_MA <- ma(ctd.df$Temperature_CTD,4,FALSE)


firstof()
firstof(year = 1970, month = 1, day = 1, hour = 0, min = 0, sec = 0, tz = "")

#plot(data.gw.df[,3], data.gw.df[,9])
#data.gw.df[,9]==NA
#as.numeric(data.gw.df[2,3]-data.gw.df[1,3])
#data.gw.df[1,3]+10

overlap.end <- tail(data.gw1.df[,3],1)
overlap.start <- head(data.gw.df[,3],1)
overlap1.df <- data.gw1.df[(data.gw1.df[,3]>=overlap.start & data.gw1.df[,3]<=overlap.end),]
overlap.df <- data.gw.df[(data.gw.df[,3]>=overlap.start & data.gw.df[,3]<=overlap.end),]
test <- (overlap.df[,3]-overlap1.df[,3])

#match(overlap.df[,3],overlap1.df[,3],)

intervals5.vec <- seq.POSIXt(from=head(data.gw.df[,3],1), to=tail(data.gw.df[,3],1), by = "5 min")
intervals10.vec <- seq.POSIXt(from=head(data.gw.df[,3],1), to=tail(data.gw.df[,3],1), by = "10 min")

missing.obs <- intervals10.vec[!(intervals10.vec %in% data.gw.df[,3])]

gwa.df <- as.data.frame(approx(x=data.gw.df[,3], y=data.gw.df[,5], method="linear", xout=intervals5.vec))
gwa.df[,1] <- as.POSIXct(gwa.df[,1], format="%m/%d/%Y %H:%M:%S", tz="Pacific/Honolulu", origin = "01/01/1970 00:00:00")
diff.vec <- gwa.df[gwa.df[,1] %in% overlap1.df[,3],2]-overlap1.df[overlap1.df[,3] %in% gwa.df[,1],5]
overlap.combined <- cbind(gwa.df[gwa.df[,1] %in% overlap1.df[,3],],overlap1.df[overlap1.df[,3] %in% gwa.df[,1],])


length(gwa.dt.df)
length(gwa.df[[1]])
2*length(data.gw.df[,3])-1
c(head(data.gw.df[,3],1), head(gwa.dt.df,1), tail(data.gw.df[,3],1), tail(gwa.dt.df,1))
  
#just testing:

as.POSIXct(Sys.time())

x <- c(1:10, 12,13)
y <- rnorm(12)
#par(mfrow = c(2,1))
plot(x, y, main = "approx(.) and approxfun(.)")
test.df <- points(approx(x, y, method="linear", n=length(x)*2-1), col = 2, pch = "*")
test.df <- approx(x, y, method="linear", n=length(x)*2-1)
points(approx(x, y, method = "constant"), col = 4, pch = "*")

plot(cpsB$Date_sniffer,cpsB$cpsB,"p")
#reverse aggregation

data(sample_matrix)
sample.xts <- as.xts(sample_matrix, descr='my new xts object')

