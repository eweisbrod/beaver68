# ------------------------------------------------------------------

#This script takes the code from Gow and Ding Chapter 12 and modifies
#it to partially replicate Dechow et al 2014 and do a figure by size

#Ch 12 code from:  
# https://iangow.github.io/far_book/beaver68.html

# ------------------------------------------------------------------
#load libraries
library(dplyr)
library(DBI)
library(ggplot2)
library(farr)
library(lubridate)  # For year()
library(tidyr)      # For fill()
library(dbplyr)     # For copy_inline()

# ------------------------------------------------------------------
#load helper scripts
#similar to "include" statement in SAS.

#this first one is not really needed but you can use it to see what
# your data directory is set as or if you need to set any params
source("src/-Global-Parameters.R")

#This one loads the helper functions in the utils script such as 
#industry assignment and winsorize, etc. 
#you can learn more about them in my example project
source("src/utils.R")


# ------------------------------------------------------------------
#Define local parameters

#event window parameters from Gow 
days_before <- 20L
days_after <- 20L

#I edited these to extend the dates from 
#1971 to 2024
first_date <- "1971-01-01"
last_date <- "2024-12-31"


# ------------------------------------------------------------------
#clever thing that Ian is doing to use the parquet data
#use duckDB to create a database in RAM that only opens
#columns that we need so that it wont use up as much RAM
#while we are processing the data.
#it behaves similar to logging into a remote server but it 
#actually is on your own computer and only temporary
db <- dbConnect(duckdb::duckdb())

#load references to the parquet files into your in-memory
#DuckDB database. The schema is the subfolder in your data
#directory and the "table" is the parquet file.
dsf <- load_parquet(db, schema = "crsp", table = "dsf")
dsi <- load_parquet(db, schema = "crsp", table = "dsi")
msf <- load_parquet(db, schema = "crsp", table = "msf")
msi <- load_parquet(db, schema = "crsp", table = "msi")

ccmxpf_lnkhist <- load_parquet(db, schema = "crsp", table = "ccmxpf_lnkhist")
stocknames <- load_parquet(db, schema = "crsp", table = "stocknames")

funda <- load_parquet(db, schema = "comp", table = "funda")
fundq <- load_parquet(db, schema = "comp", table = "fundq")


#make a table of earnings announcement dates by pulling from 
#the fundq dataset from Compustat
earn_annc_dates <-
  #start with fundq
  fundq |>
  #apply standard compustat filters
  filter(indfmt == "INDL", datafmt == "STD",
         consol == "C", popsrc == "D") |>
  #require announcement date (rdq) not missing
  #filter to only annual (4th quarter) announcements (fqtr ==4)
  #[NOTE: you could better replicate Dechow et al by removing the 4Q filter]
  filter(!is.na(rdq), fqtr == 4L) |>
  #select only the firm ID (gvkey), fiscal quarter end (datadate), and rdq
  select(gvkey, datadate, rdq) |>
  #filter fiscal quarters between the date parameters we set above
  filter(between(datadate, first_date, last_date)) |>
  #collect the data from the DuckDB file references into an R dataframe in RAM
  collect()

#create a linking table between CRSP Permnos and Compustat GVKEYS
#CRSP uses PERMNO as stock ID and Compustat uses GVKEY as firm/issuer ID
ccm_link <-
  ccmxpf_lnkhist |>
  filter(linktype %in% c("LC", "LU", "LS"),
         linkprim %in% c("C", "P")) |>
  rename(permno = lpermno) |>
  mutate(linkenddt = coalesce(linkenddt, max(linkenddt, na.rm = TRUE))) |>
  collect()

#creating a list/calendar of trading days
#to account for days when the market is closed 
trading_dates <-
  dsi |>
  select(date) |>
  distinct() |> 
  collect() |>
  #sort the dates 
  arrange(date) |>
  #create a unique index number for each data to use in lookups later
  mutate(td = row_number())

#make a single variable called min_date from the smallest date in the 
#trading_dates file
min_date <- 
  trading_dates |>
  summarize(min(date, na.rm = TRUE)) |>
  pull()

#repeat for max date
max_date <- 
  trading_dates |>
  summarize(max(date, na.rm = TRUE)) |>
  pull()

#for every possible calendar date between
#min date and max date, assign it a trading date (td) 
#ID number, such that if the market is closed on that 
#calendar date then it will be assigned to the next available trading date
annc_dates <-
  tibble(annc_date = seq(min_date, max_date, 1)) |>
  left_join(trading_dates, by = join_by(annc_date == date)) |>
  fill(td, .direction = "up")

#link firm-level returns to value-weighted index returns each day
mkt_rets <-
  #dsf is the CRSP daily stock file, with stock-day returns
  dsf |>
  #dsi is the daily index file, vwretd is the value weighted index
  #return 
  inner_join(dsi, by = "date") |>
  #compute market-adjusted abnormal return as firm return less index
  mutate(ret_mkt = ret - vwretd) |>
  #only keep the variables we need
  #I MODIFIED to add in shrout and price so that I could calculate
  #turnover and market cap (i.e., firm size)
  select(permno, date, ret, ret_mkt, vol,shrout,prc)

#[YOU COUD BETTER REPLICATE DECHOW ET AL BY REMOVING THIS]
#Create a list of only NYSE permnos where exchcd == 1
#will be applied later as a filter when linking
nyse <-
  stocknames |> 
  filter(exchcd == 1) |>
  select(permno, namedt, nameenddt) |>
  collect()

#link the list of earnings announcement dates to CRSP Permnos 
#using the GVKEY to PERMNO link you created above
earn_annc_links <-
  earn_annc_dates |>
  inner_join(ccm_link, join_by(gvkey, rdq >= linkdt, rdq <= linkenddt)) |>
  semi_join(nyse, join_by(permno, rdq >= namedt, rdq <= nameenddt)) |>
  #keep only the four variables we need
  select(gvkey, datadate, rdq, permno)

#expand each single announcement date 
#to the -20,+20 announcement date window
earn_annc_windows <-
  earn_annc_dates |>
  inner_join(annc_dates, by = join_by(rdq == annc_date)) |>
  mutate(start_td = td - days_before, 
         end_td = td + days_after) |>
  inner_join(trading_dates, by = join_by(start_td == td)) |>
  rename(start_date = date) |>
  inner_join(trading_dates, by = join_by(end_td == td)) |>
  rename(end_date = date,
         event_td = td) |>
  #drop a couple of temp vars that we don't need
  select(-start_td, -end_td)

#join each trading day in the window to its permno
earn_annc_window_permnos <-
  earn_annc_windows |>
  inner_join(earn_annc_links, by = c("gvkey", "datadate", "rdq"))

#merge in returns for each trading day in the window
earn_annc_crsp <-
  mkt_rets |>
  inner_join(copy_to(db, earn_annc_window_permnos, overwrite = TRUE), 
             join_by(permno, 
                     date >= start_date, date <= end_date)) |>
  #I modified to merge in shrout and prc as well
  select(gvkey, datadate, rdq, event_td, date, ret, ret_mkt, vol,shrout,prc) |>
  collect()

#not sure this was the most intuitive way to do this but 
#need to re-merge to td day index to computer relative TD
# with respect to the event. Converts from absolute TD index number
#to relative event day for the event study
earn_annc_rets <-
  earn_annc_crsp |>
  inner_join(trading_dates, by = "date") |>
  mutate(relative_td = td - event_td) 

#compute relative volume by taking mean volume over the event window
#then divide individual day volume by avg_vol
earn_annc_vols <-
  earn_annc_rets |>
  group_by(gvkey, datadate) |>
  mutate(avg_vol = mean(vol, na.rm = TRUE)) |>
  mutate(rel_vol = vol / avg_vol,
         #I modified this to also calculate share turnover
         #shares outstanding is in thousands in CRSP
         turn = vol / (shrout*1000),
         #this calculates market cap, price * shares outstanding
         #however, have to take absolute value in CRSP
         #prc is negative if the closing price was a midquote price
         #it doesnt matter that shrout is in thousands here because 
         #it will be the same for every stock, this is just used to sort
         #into size buckets
         size = abs(prc)*shrout,
         year = year(datadate)) |>
  ungroup() 

#removed the year grouping
#added some extra stats and some winsorization
earn_annc_summ <-
  earn_annc_vols |>
  #winsorize the abnormal returns and the relative volume
  #create new variables with _w
  #can learn more about this in my example project 
  mutate(ret_mkt_w = winsorize_x(ret_mkt),
         rel_vol_w = winsorize_x(rel_vol)) |> 
  #group by event date
  group_by(relative_td) |>
  #compute the means etc for every observation that is -20 days from event
  # then every observation that is -19 days from event and so on
  summarize(obs = n(),
            mean_ret = mean(ret, na.rm = TRUE),
            mean_ret_mkt = mean(ret_mkt, na.rm = TRUE),
            mean_rel_vol = mean(rel_vol, na.rm = TRUE),
            med_rel_vol = median(rel_vol, na.rm = TRUE),
            mean_rel_vol_w = mean(rel_vol_w, na.rm = TRUE),
            sd_ret = sd(ret, na.rm = TRUE),
            sd_ret_mkt = sd(ret_mkt, na.rm = TRUE),
            sd_ret_mkt_w = sd(ret_mkt_w, na.rm = TRUE),
            mad_ret = mean(abs(ret), na.rm = TRUE),
            mad_ret_mkt = mean(abs(ret_mkt), na.rm = TRUE),
            .groups = "drop")


#plot the mean relative volume by event day
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol)) +
  geom_line()

#plot the median relative volume
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = med_rel_vol)) +
  geom_line()

#plot the mean winsorized relative volume
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol_w)) +
  geom_line()

#plot the standard deviation of abnormal returns
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = sd_ret_mkt)) +
  geom_line()

#plot the winsorized standard deviation of abnormal returns
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = sd_ret_mkt_w)) +
  geom_line()



#change to decade grouping
#added some extra stats and some winsorization
earn_annc_summ <-
  earn_annc_vols |>
  mutate(ret_mkt_w = winsorize_x(ret_mkt),
         rel_vol_w = winsorize_x(rel_vol),
         turn_w = winsorize_x(turn)) |> 
  #add decade labels
  mutate(decade = case_when(
    between(year,1971,1980) ~ "1971 - 1980",
    between(year,1981,1990) ~ "1981 - 1990",
    between(year,1991,2000) ~ "1991 - 2000",
    between(year,2001,2010) ~ "2001 - 2010",
    between(year,2011,2024) ~ "2011 - 2024"
  )) |> 
  #now the group by for calculating means is within both 
  #event day and decade group
  group_by(relative_td,decade) |>
  summarize(obs = n(),
            mean_ret = mean(ret, na.rm = TRUE),
            mean_ret_mkt = mean(ret_mkt, na.rm = TRUE),
            mean_rel_vol = mean(rel_vol, na.rm = TRUE),
            med_rel_vol = median(rel_vol, na.rm = TRUE),
            mean_rel_vol_w = mean(rel_vol_w, na.rm = TRUE),
            mean_turn_w = mean(turn_w, na.rm = TRUE),
            med_turn_w = median(turn_w, na.rm = TRUE),
            sd_ret = sd(ret, na.rm = TRUE),
            sd_ret_mkt = sd(ret_mkt, na.rm = TRUE),
            sd_ret_mkt_w = sd(ret_mkt_w, na.rm = TRUE),
            mad_ret = mean(abs(ret), na.rm = TRUE),
            mad_ret_mkt = mean(abs(ret_mkt), na.rm = TRUE),
            .groups = "drop")


#relative volume by decade
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol_w,
             group = decade, color = decade,
             linetype = decade)) +
  geom_line()

#share turnover by decade...this is what dechow et al do
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_turn_w,
             group = decade, color = decade,
             linetype = decade)) +
  geom_line()

#median turnover, wanted to see if it was driven by small number of EA
# or not
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = med_turn_w,
             group = decade, color = decade,
             linetype = decade)) +
  geom_line()



#change to annual size grouping
#added some extra stats and some winsorization
earn_annc_summ <-
  earn_annc_vols |>
  filter(!is.na(size)) |> 
  mutate(ret_mkt_w = winsorize_x(ret_mkt),
         rel_vol_w = winsorize_x(rel_vol),
         turn_w = winsorize_x(turn)) |> 
  #I will sort on size within each year to make sure that 
  #my size sorts are not simply sorting older smaller firms into
  #the small ntiles. This way, size ntile will be relative to firms
  #in the same year
  group_by(year) |> 
    mutate(size_ntile = ntile(size,5)) |>
  #after creating annual size ntiles, regroup the data
  group_by(relative_td,size_ntile) |>
  summarize(obs = n(),
            mean_ret = mean(ret, na.rm = TRUE),
            mean_ret_mkt = mean(ret_mkt, na.rm = TRUE),
            mean_rel_vol = mean(rel_vol, na.rm = TRUE),
            med_rel_vol = median(rel_vol, na.rm = TRUE),
            mean_rel_vol_w = mean(rel_vol_w, na.rm = TRUE),
            mean_turn_w = mean(turn_w, na.rm = TRUE),
            med_turn_w = median(turn_w, na.rm = TRUE),
            sd_ret = sd(ret, na.rm = TRUE),
            sd_ret_mkt = sd(ret_mkt, na.rm = TRUE),
            sd_ret_mkt_w = sd(ret_mkt_w, na.rm = TRUE),
            mad_ret = mean(abs(ret), na.rm = TRUE),
            mad_ret_mkt = mean(abs(ret_mkt), na.rm = TRUE),
            .groups = "drop")

#check my size groups
  earn_annc_vols |>
  filter(!is.na(size)) |> 
  mutate(ret_mkt_w = winsorize_x(ret_mkt),
         rel_vol_w = winsorize_x(rel_vol),
         turn_w = winsorize_x(turn)) |> 
  group_by(year) |> 
  mutate(size_ntile = ntile(size,5)) |> 
  group_by(size_ntile) |> 
  count()
  
  earn_annc_vols |>
    filter(!is.na(size)) |> 
    mutate(ret_mkt_w = winsorize_x(ret_mkt),
           rel_vol_w = winsorize_x(rel_vol),
           turn_w = winsorize_x(turn)) |> 
    group_by(year) |> 
    mutate(size_ntile = ntile(size,5)) |> 
    group_by(size_ntile) |> 
    summarize(
      obs = n(),
      mean_size = mean(size, na.rm = TRUE)
    )


#plot median turnover by size group
earn_annc_summ |>
  #turn numeric variable into factor for chart labelling
  mutate(size_ntile = factor(size_ntile)) |> 
  ggplot(aes(x = relative_td, y = med_turn_w,
             group = size_ntile, color = size_ntile,
             linetype = size_ntile)) +
  geom_line()


#plot mean turnover by size group
earn_annc_summ |>
  mutate(size_ntile = factor(size_ntile)) |> 
  ggplot(aes(x = relative_td, y = mean_turn_w,
             group = size_ntile, color = size_ntile,
             linetype = size_ntile)) +
  geom_line()
