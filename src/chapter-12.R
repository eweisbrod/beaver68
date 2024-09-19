#load libraries
library(dplyr)
library(DBI)
library(ggplot2)
library(farr)
library(lubridate)  # For year()
library(tidyr)      # For fill()
library(dbplyr)     # For copy_inline()

#load helper scripts
#similar to "include" statement in SAS.
source("src/-Global-Parameters.R")
source("src/utils.R")


#parameters
days_before <- 20L
days_after <- 20L

first_date <- "1971-01-01"
last_date <- "2019-12-31"

db <- dbConnect(duckdb::duckdb())

dsf <- load_parquet(db, schema = "crsp", table = "dsf")
dsi <- load_parquet(db, schema = "crsp", table = "dsi")
msf <- load_parquet(db, schema = "crsp", table = "msf")
msi <- load_parquet(db, schema = "crsp", table = "msi")
ccmxpf_lnkhist <- load_parquet(db, schema = "crsp", table = "ccmxpf_lnkhist")
stocknames <- load_parquet(db, schema = "crsp", table = "stocknames")

funda <- load_parquet(db, schema = "comp", table = "funda")
fundq <- load_parquet(db, schema = "comp", table = "fundq")



earn_annc_dates <-
  fundq |>
  filter(indfmt == "INDL", datafmt == "STD",
         consol == "C", popsrc == "D") |>
  filter(!is.na(rdq), fqtr == 4L) |>
  select(gvkey, datadate, rdq) |>
  filter(between(datadate, first_date, last_date)) |>
  collect()

ccm_link <-
  ccmxpf_lnkhist |>
  filter(linktype %in% c("LC", "LU", "LS"),
         linkprim %in% c("C", "P")) |>
  rename(permno = lpermno) |>
  mutate(linkenddt = coalesce(linkenddt, max(linkenddt, na.rm = TRUE))) |>
  collect()

trading_dates <-
  dsi |>
  select(date) |>
  collect() |>
  arrange(date) |>
  mutate(td = row_number())

min_date <- 
  trading_dates |>
  summarize(min(date, na.rm = TRUE)) |>
  pull()

max_date <- 
  trading_dates |>
  summarize(max(date, na.rm = TRUE)) |>
  pull()

annc_dates <-
  tibble(annc_date = seq(min_date, max_date, 1)) |>
  left_join(trading_dates, by = join_by(annc_date == date)) |>
  fill(td, .direction = "up")

mkt_rets <-
  dsf |>
  inner_join(dsi, by = "date") |>
  mutate(ret_mkt = ret - vwretd) |>
  select(permno, date, ret, ret_mkt, vol,shrout,prc)

#we could consider removing this filter
nyse <-
  stocknames |> 
  filter(exchcd == 1) |>
  select(permno, namedt, nameenddt) |>
  collect()


earn_annc_links <-
  earn_annc_dates |>
  inner_join(ccm_link, join_by(gvkey, rdq >= linkdt, rdq <= linkenddt)) |>
  semi_join(nyse, join_by(permno, rdq >= namedt, rdq <= nameenddt)) |>
  select(gvkey, datadate, rdq, permno)

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
  select(-start_td, -end_td)

earn_annc_window_permnos <-
  earn_annc_windows |>
  inner_join(earn_annc_links, by = c("gvkey", "datadate", "rdq"))

earn_annc_crsp <-
  mkt_rets |>
  inner_join(copy_to(db, earn_annc_window_permnos, overwrite = TRUE), 
             join_by(permno, 
                     date >= start_date, date <= end_date)) |>
  select(gvkey, datadate, rdq, event_td, date, ret, ret_mkt, vol,shrout,prc) |>
  collect()

earn_annc_rets <-
  earn_annc_crsp |>
  inner_join(trading_dates, by = "date") |>
  mutate(relative_td = td - event_td) 

earn_annc_vols <-
  earn_annc_rets |>
  group_by(gvkey, datadate) |>
  mutate(avg_vol = mean(vol, na.rm = TRUE)) |>
  mutate(rel_vol = vol / avg_vol,
         turn = vol / (shrout*1000),
         size = abs(prc)*shrout,
         year = year(datadate)) |>
  ungroup() 

#removed the year grouping
#added some extra stats and some winsorization
earn_annc_summ <-
  earn_annc_vols |>
  mutate(ret_mkt_w = winsorize_x(ret_mkt),
         rel_vol_w = winsorize_x(rel_vol)) |> 
  group_by(relative_td) |>
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




earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol)) +
  geom_line()


earn_annc_summ |>
  ggplot(aes(x = relative_td, y = med_rel_vol)) +
  geom_line()


earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol_w)) +
  geom_line()

earn_annc_summ |>
  ggplot(aes(x = relative_td, y = sd_ret_mkt)) +
  geom_line()

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


#Gow plots relative volume
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_rel_vol_w,
             group = decade, color = decade,
             linetype = decade)) +
  geom_line()

#Dechow et al share plot turnover
earn_annc_summ |>
  ggplot(aes(x = relative_td, y = mean_turn_w,
             group = decade, color = decade,
             linetype = decade)) +
  geom_line()

#median turnover
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
  group_by(year) |> 
    mutate(size_ntile = ntile(size,5)) |>
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


#median turnover
earn_annc_summ |>
  mutate(size_ntile = factor(size_ntile)) |> 
  ggplot(aes(x = relative_td, y = med_turn_w,
             group = size_ntile, color = size_ntile,
             linetype = size_ntile)) +
  geom_line()


#mean turnover
earn_annc_summ |>
  mutate(size_ntile = factor(size_ntile)) |> 
  ggplot(aes(x = relative_td, y = mean_turn_w,
             group = size_ntile, color = size_ntile,
             linetype = size_ntile)) +
  geom_line()
