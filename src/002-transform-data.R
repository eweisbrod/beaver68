# ==============================================================================
# 002-transform-data.R
#
# Purpose:
#   Turn the raw WRDS pulls into the event-window panel that Beaver (1968)
#   analyzed: for each annual earnings announcement, one row per trading
#   day from 20 days before to 20 days after the announcement, carrying
#   the return, the market-adjusted return, and trading volume.
#
#   The logic of the design, in one sentence: if earnings announcements
#   convey information, then volume and return variability should be
#   ABNORMALLY HIGH on day 0 relative to the surrounding non-announcement
#   days -- and each announcement acts as its own control.
#
# Inputs (from RAW_DATA_DIR):
#   fundq-raw.parquet
#   ccm-link.parquet
#   crsp-dsf-v2.parquet
#   crsp-index.parquet
#
# Outputs (to DATA_DIR):
#   event-panel.parquet        One row per announcement per trading day
#   event-summary.parquet      Collapsed to relative_td x year x decade
#   trading-dates.parquet      CRSP trading-day calendar with td index
#   sample-selection.parquet   Step-by-step observation counts
#
# Notes:
#   - The heavy joins run in DuckDB directly against the parquet files,
#     so the ~100M-row CRSP daily file never enters R memory. Only the
#     small collapsed results get collect()ed.
#   - Every design choice Beaver made that we either match or deliberately
#     depart from is flagged with a "DESIGN CHOICE" comment. Those are
#     the raw material for the discussion questions in the write-up.
# ==============================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, lubridate, glue, arrow, duckdb, DBI, dbplyr,
               tictoc,
               tidyverse)

# The DuckDB progress bar floods the .Rout log with "DuckDB progress: 0%".
# Journals that require an execution log (see the JAR Data and Code Sharing
# Policy) do not want to read 4,000 lines of that.
options(duckdb.progress_display = FALSE)

load_dot_env(".env")
raw_data_dir <- Sys.getenv("RAW_DATA_DIR")
data_dir     <- Sys.getenv("DATA_DIR")
output_dir   <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")


# Design parameters ------------------------------------------------------------

# Everything you might reasonably want to change lives here, so you can run
# an alternative specification without hunting through the script. Several
# of the discussion questions ask you to change one of these and re-run.

DAYS_BEFORE   <- 20L      # trading days before the announcement
DAYS_AFTER    <- 20L      # trading days after

# DESIGN CHOICE (matches Beaver): annual earnings announcements only.
# Beaver's 1961-1965 sample period predates mandatory quarterly reporting,
# so his "earnings announcement" is an annual event. fqtr == 4 is the
# closest modern analogue: the Q4 report, which is when annual results are
# released. Set ANNUAL_ONLY <- FALSE to use all four quarters -- which is
# what Ball and Shivakumar (2008) do when they argue earnings
# announcements convey relatively little information.
ANNUAL_ONLY   <- TRUE

# DESIGN CHOICE (matches Beaver): NYSE-listed firms only. Beaver required
# NYSE listing partly for data availability and partly to get a set of
# firms with relatively few competing news events. Set to FALSE to include
# Nasdaq and NYSE American -- see the discussion question on how far
# Beaver's results generalize.
NYSE_ONLY     <- TRUE

# Minimum number of non-missing return observations in the window. Without
# this, a firm with three traded days in a 41-day window contributes a
# wildly noisy "average."
MIN_OBS       <- 30L


# Sample selection tracker -----------------------------------------------------

# Records the observation count after each screen. Script 4 formats this
# into the sample-selection table that every empirical paper reports.

sample_selection <- tibble(step = integer(),
                           description = character(),
                           obs = integer())

add_step <- function(tbl, step, desc, n) {
  bind_rows(tbl, tibble(step = step, description = desc, obs = as.integer(n)))
}


# Open DuckDB over the parquet files -------------------------------------------

# tbl(con, "read_parquet('...')") creates a LAZY reference. No data moves
# until you call collect(). DuckDB reads the parquet files directly off
# disk and only materializes what it needs.

con <- dbConnect(duckdb())

fundq_tbl <- tbl(con, glue("read_parquet('{raw_data_dir}/fundq-raw.parquet')"))
ccm_tbl   <- tbl(con, glue("read_parquet('{raw_data_dir}/ccm-link.parquet')"))
dsf_tbl   <- tbl(con, glue("read_parquet('{raw_data_dir}/crsp-dsf-v2.parquet')"))
index_tbl <- tbl(con, glue("read_parquet('{raw_data_dir}/crsp-index.parquet')"))


# Step 1: earnings announcement dates ------------------------------------------

# rdq is Compustat's "report date of quarterly earnings" -- the date the
# announcement was made public. This is our event date.

annc <- fundq_tbl |>
  filter(!is.na(rdq)) |>
  select(gvkey, datadate, fyearq, fqtr, rdq, conm, saleq, ibq, atq, prccq, cshoq)

sample_selection <- add_step(sample_selection, 1,
                             "Compustat firm-quarters with an announcement date (rdq)",
                             count_rows(annc))

if (ANNUAL_ONLY) {
  annc <- annc |> filter(fqtr == 4L)
  sample_selection <- add_step(sample_selection, 2,
                               "Annual (fourth-quarter) announcements only",
                               count_rows(annc))
} else {
  sample_selection <- add_step(sample_selection, 2,
                               "All four fiscal quarters retained",
                               count_rows(annc))
}


# Step 2: attach permno via the CCM link ---------------------------------------

# The CCM link is a HISTORY table -- one row per gvkey-permno pairing per
# validity period. The screens below are the standard ones:
#   linktype LC / LU / LS  research-grade links (drop dubious ones)
#   linkprim C / P         primary link (drop secondary share classes,
#                          which would otherwise duplicate every event)
# The date conditions keep only links valid ON the announcement date. A
# NULL linkenddt means "still valid today."

ccm <- ccm_tbl |>
  filter(linktype %in% c("LC", "LU", "LS"),
         linkprim %in% c("C", "P")) |>
  select(gvkey, permno = lpermno, linkdt, linkenddt)

annc_linked <- annc |>
  inner_join(ccm, by = "gvkey") |>
  filter(rdq >= linkdt,
         is.na(linkenddt) | rdq <= linkenddt) |>
  select(gvkey, permno, datadate, fyearq, rdq, conm)

sample_selection <- add_step(sample_selection, 3,
                             "Merged to CRSP permno via CCM link",
                             count_rows(annc_linked))

# Belt and braces: the link screens above should already guarantee one
# permno per gvkey-datadate, but a bad link table would silently duplicate
# every event and inflate the sample. Check rather than assume.
dupes <- annc_linked |>
  count(gvkey, datadate) |>
  filter(n > 1) |>
  count() |>
  pull(n)

if (dupes > 0) {
  warning("CCM link produced ", dupes,
          " duplicated gvkey-datadate pairs. Investigate before trusting results.")
} else {
  message("Link check passed: one permno per gvkey-datadate.")
}


# Step 3: build the trading-day calendar ---------------------------------------

# THIS IS THE KEY IDEA OF AN EVENT STUDY and it is worth pausing on.
#
# We do not want CALENDAR days -- "20 days before the announcement" would
# straddle weekends and holidays inconsistently. We want TRADING days. So
# we take every date on which the market was open, sort them, and number
# them sequentially: td = 1, 2, 3, ...
#
# Now "20 trading days before" is just event_td - 20. Arithmetic on a
# gap-free integer replaces messy date logic.
#
# The market index file gives us exactly the set of dates the market was
# open, which is why we use it rather than deriving the calendar from
# individual stocks (a thinly traded stock has no observation on days it
# did not trade, which would punch holes in the calendar).

trading_dates <- index_tbl |>
  select(date = dlycaldt) |>
  distinct() |>
  arrange(date) |>
  collect() |>
  mutate(td = row_number())

message("Trading-day calendar: ", nrow(trading_dates), " days from ",
        min(trading_dates$date), " to ", max(trading_dates$date))

write_parquet(trading_dates, glue("{data_dir}/trading-dates.parquet"))


# Step 4: map each announcement to its event trading day -----------------------

# An announcement can land on a Saturday, a holiday, or after the close.
# We roll FORWARD to the next day the market was open -- that is the first
# session in which the market could actually react.
#
# What we need is a "rolling join": for each rdq, find the smallest
# trading date that is >= rdq. data.table and polars have this as a
# built-in; in base R the tool is findInterval() on the sorted vector of
# trading dates.
#
# WORKED EXAMPLE. Suppose the trading calendar around a weekend is
#
#   index:  ...   4            5            6           ...
#   date:   ... 2019-02-01   2019-02-04   2019-02-05    ...
#             (Friday)      (Monday)     (Tuesday)
#
# findInterval(x, vec) returns the index of the LARGEST vec value <= x.
#
#   rdq = 2019-02-04 (Monday, a trading day)
#       findInterval -> 5, and td_vec[5] == rdq, so the answer is 5.
#
#   rdq = 2019-02-02 (Saturday, not a trading day)
#       findInterval -> 4 (Friday, the largest date <= Saturday).
#       td_vec[4] != rdq, so we add 1 and get 5 -- Monday. Correct:
#       a Saturday press release is first tradable on Monday.
#
# That "add 1 unless rdq is itself a trading day" is exactly what the
# three lines below do, vectorized over all announcements at once.

annc_r <- annc_linked |> collect()

td_vec <- trading_dates$date

idx <- findInterval(annc_r$rdq, td_vec)

# Is rdq itself a trading day? (idx > 0 guards the case where rdq falls
# before the calendar even starts, when findInterval returns 0.)
is_trading_day <- idx > 0 & td_vec[pmax(idx, 1L)] == annc_r$rdq

# Roll forward one position when it is not.
event_idx <- ifelse(is_trading_day, idx, idx + 1L)

# An announcement after the last trading date in our calendar has no
# tradable day. Mark those NA; the filter below drops them.
event_idx[event_idx < 1L | event_idx > length(td_vec)] <- NA_integer_

annc_r <- annc_r |>
  mutate(event_td   = trading_dates$td[event_idx],
         event_date = trading_dates$date[event_idx]) |>
  filter(!is.na(event_td))

sample_selection <- add_step(sample_selection, 4,
                             "Announcement mapped to a trading day",
                             nrow(annc_r))

# Drop events whose full window would run off either end of the calendar
# (e.g. an announcement in the last week of the sample has no +20 days).
annc_r <- annc_r |>
  filter(event_td - DAYS_BEFORE >= min(trading_dates$td),
         event_td + DAYS_AFTER  <= max(trading_dates$td))

sample_selection <- add_step(sample_selection, 5,
                             glue("Complete [-{DAYS_BEFORE}, +{DAYS_AFTER}] window available"),
                             nrow(annc_r))


# Step 5: join the daily CRSP data ---------------------------------------------

# Now push the (small) event table back into DuckDB and range-join it to
# the (huge) daily file. DuckDB does the work; R just holds the handle.

duckdb::duckdb_register(con, "events", annc_r)
events_tbl <- tbl(con, "events")

duckdb::duckdb_register(con, "tdates", trading_dates)
tdates_tbl <- tbl(con, "tdates")

# Daily stock data, tagged with the trading-day index and with the market
# return joined on. Note ret_mkt: the firm's return NET of the market.
# Beaver's return-variability test needs this -- otherwise a market-wide
# move during a firm's announcement week looks like an earnings reaction.
daily <- dsf_tbl |>
  inner_join(tdates_tbl, by = c("dlycaldt" = "date")) |>
  inner_join(index_tbl, by = "dlycaldt") |>
  mutate(ret_mkt = dlyret - dlytotret) |>
  select(permno, date = dlycaldt, td,
         ret = dlyret, ret_mkt, vol = dlyvol, prc = dlyprc,
         shrout, primaryexch)

if (NYSE_ONLY) {
  daily <- daily |> filter(primaryexch == "N")
}

# The range join. For each event, grab every daily observation for that
# permno whose trading day falls inside the window.
panel <- events_tbl |>
  inner_join(daily, by = "permno") |>
  filter(td >= event_td - DAYS_BEFORE,
         td <= event_td + DAYS_AFTER) |>
  mutate(relative_td = td - event_td) |>
  select(gvkey, permno, datadate, fyearq, rdq, event_td, event_date,
         date, relative_td, ret, ret_mkt, vol, prc, shrout)

# Step 6: build the Beaver measures --------------------------------------------

# THE TWO VARIABLES BEAVER LOOKED AT:
#
#   VOLUME. Beaver scaled each firm's announcement-week volume by that
#   firm's own average volume in non-announcement weeks -- so a large firm
#   that always trades heavily does not swamp a small one. We compute two
#   versions:
#     rel_vol  volume / that event's own mean daily volume  (Beaver's ratio)
#     turn     volume / shares outstanding                  (turnover)
#   Turnover is the modern convention and is comparable across firms and
#   across time in a way that raw volume is not. shrout is reported in
#   THOUSANDS of shares while vol is in shares, hence the 1000.
#
#   RETURN VARIABILITY. Beaver did not look at the LEVEL of returns -- an
#   announcement can be good news or bad news, so average returns wash out
#   to roughly zero. He looked at the DISPERSION of returns. We report the
#   standard deviation and the mean absolute return (the latter is far
#   less sensitive to a single outlier).

# Count events surviving the exchange screen BEFORE the minimum-
# observations screen, so the two appear as separate rows in the sample
# selection table. Collapsing them into one line would hide which screen
# actually costs you the observations -- and on this sample the exchange
# screen is by far the bigger of the two.
n_events_exch <- panel |> distinct(gvkey, datadate) |> count() |> pull(n)

sample_selection <- add_step(sample_selection, 6,
                             if (NYSE_ONLY) "NYSE-listed common shares"
                             else "All exchanges, common shares",
                             n_events_exch)

panel <- panel |>
  group_by(gvkey, datadate) |>
  mutate(avg_vol = mean(vol, na.rm = TRUE),
         n_obs   = sum(as.integer(!is.na(ret)), na.rm = TRUE)) |>
  ungroup() |>
  mutate(rel_vol = vol / avg_vol,
         turn    = vol / (shrout * 1000),
         mve     = abs(prc) * shrout / 1000,   # market value, $ millions
         year    = year(datadate)) |>
  # Label the decade here, while we are still inside DuckDB, so that BOTH
  # summaries below can be computed by the database. dbplyr translates
  # case_when() into SQL CASE WHEN -- nothing is pulled into R to do this.
  mutate(decade = case_when(
    between(year, 1970, 1979) ~ "1970s",
    between(year, 1980, 1989) ~ "1980s",
    between(year, 1990, 1999) ~ "1990s",
    between(year, 2000, 2009) ~ "2000s",
    between(year, 2010, 2019) ~ "2010s",
    year >= 2020              ~ "2020s"
  ))

# Apply the minimum-observations screen.
panel <- panel |> filter(n_obs >= MIN_OBS)

# Materialize. This is the one genuinely expensive step -- everything
# above was lazy. compute() keeps the result inside DuckDB rather than
# pulling ~16M rows into R.
tictoc::tic("Building event panel")
panel <- panel |> compute()
tictoc::toc()

n_panel  <- count_rows(panel)
n_events <- panel |> distinct(gvkey, datadate) |> count() |> pull(n)

sample_selection <- add_step(sample_selection, 7,
                             glue("At least {MIN_OBS} return observations in the window"),
                             n_events)

message("Event panel: ", format(n_panel, big.mark = ","), " firm-days across ",
        format(n_events, big.mark = ","), " announcements.")


# Step 7: collapse to the plot-level summary -----------------------------------

# Beaver's figures plot an average across all announcements for each day
# relative to the announcement. This is that collapse.
#
# We group by year so you can look at how the pattern has changed over
# time. `decade` is included in the grouping only so the label survives
# into the output -- it is a function of `year`, so it creates no
# additional groups.

event_summary <- panel |>
  group_by(relative_td, year, decade) |>
  summarize(obs          = n(),
            mean_ret     = mean(ret, na.rm = TRUE),
            mean_ret_mkt = mean(ret_mkt, na.rm = TRUE),
            sd_ret       = sd(ret, na.rm = TRUE),
            sd_ret_mkt   = sd(ret_mkt, na.rm = TRUE),
            mad_ret      = mean(abs(ret), na.rm = TRUE),
            mad_ret_mkt  = mean(abs(ret_mkt), na.rm = TRUE),
            mean_rel_vol = mean(rel_vol, na.rm = TRUE),
            med_rel_vol  = median(rel_vol, na.rm = TRUE),
            mean_turn    = mean(turn, na.rm = TRUE),
            med_turn     = median(turn, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  arrange(year, relative_td)

# The by-decade collapse. NOTE that this is computed from the PANEL, not
# by averaging the yearly averages -- averaging averages would silently
# weight a year with 800 announcements the same as one with 4,000.
#
# Like event_summary above, this runs inside DuckDB and only the small
# (41 x 6) result comes back to R.
decade_summary <- panel |>
  group_by(relative_td, decade) |>
  summarize(obs          = n(),
            sd_ret_mkt   = sd(ret_mkt, na.rm = TRUE),
            mad_ret_mkt  = mean(abs(ret_mkt), na.rm = TRUE),
            mean_rel_vol = mean(rel_vol, na.rm = TRUE),
            med_rel_vol  = median(rel_vol, na.rm = TRUE),
            med_turn     = median(turn, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  arrange(decade, relative_td)


# Step 8: write everything out -------------------------------------------------

dir.create(data_dir, showWarnings = FALSE, recursive = TRUE)

# The full panel, for anyone who wants to run their own tests on it.
# This is the file script 4 uses for the formal statistical tests.
tictoc::tic("Writing event panel")
panel |>
  collect() |>
  write_parquet(glue("{data_dir}/event-panel.parquet"))
tictoc::toc()

write_parquet(event_summary,  glue("{data_dir}/event-summary.parquet"))
write_parquet(decade_summary, glue("{data_dir}/decade-summary.parquet"))
write_parquet(sample_selection, glue("{data_dir}/sample-selection.parquet"))

print(sample_selection)

dbDisconnect(con, shutdown = TRUE)

cat("\nDerived files written to", data_dir, "\n")
cat("Next: src/003-figures.R\n")
