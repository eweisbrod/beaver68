# ==============================================================================
# 001-download-data.R
#
# Purpose:
#   Download the raw WRDS tables needed to replicate Beaver (1968), "The
#   Information Content of Annual Earnings Announcements," JAR 6, 67-92.
#
#   Beaver asked whether earnings announcements convey new information to
#   the market. His evidence: in the week of an annual earnings
#   announcement, both trading VOLUME and the VARIABILITY of stock
#   returns spike relative to their non-announcement levels. So we need
#   three ingredients:
#
#     1. Earnings announcement dates      -> Compustat fundq (rdq)
#     2. Daily returns and volume         -> CRSP daily stock file
#     3. A market return to adjust by     -> CRSP value-weighted index
#
#   plus the CCM link table to get from Compustat's gvkey to CRSP's permno.
#
# Inputs:
#   WRDS PostgreSQL endpoint. Credentials come from your OS keyring
#   (service `wrds`, keys `username` and `password`), stored once by
#   project_setup() the first time you run this script.
#
# Outputs (to RAW_DATA_DIR):
#   ccm-link.parquet     CCM link table (gvkey -> permno)
#   fundq-raw.parquet    Compustat quarterly fundamentals (incl. rdq)
#   crsp-dsf-v2.parquet  CRSP daily returns, volume, price, shares out
#   crsp-index.parquet   CRSP value-weighted market index returns
#
# Notes:
#   - Skip-if-exists guards mean re-running this script does NOT re-pull
#     anything already on disk. Delete a file in RAW_DATA_DIR to force a
#     refresh.
#   - Runtime: the CRSP daily pull is the long pole. Expect ~15-25
#     minutes back to 1970. See FIRST_DATE below if you want it faster.
#   - RAW vs DERIVED: raw pulls land in RAW_DATA_DIR and are never
#     modified. Everything scripts 2-4 compute lands in DATA_DIR. That
#     split is what makes a replication cheap -- someone else can rerun
#     scripts 2-5 against your preserved raw inputs without touching WRDS.
# ==============================================================================


# Setup ------------------------------------------------------------------------

# pacman installs anything missing, then loads everything.
if (!require("pacman")) install.packages("pacman")

pacman::p_load(dotenv, keyring, dbplyr, RPostgres, DBI, glue, arrow,
               tictoc,
               tidyverse)

# Helper functions: download_parquet(), write_parquet(), batch_run(),
# project_setup(), trading_day_window(), ...
source("src/utils.R")

# First-time setup: prompts for your data paths and WRDS credentials, then
# writes .env. Idempotent -- an instant no-op once .env exists, so you can
# leave this call here forever.
#
# IMPORTANT: run this script INTERACTIVELY in RStudio the first time
# (Ctrl+Enter through it, or Ctrl+A then Ctrl+Enter). project_setup()
# needs a live console to prompt you. After .env exists, run-all.R works.
project_setup()

load_dot_env(".env")
raw_data_dir <- Sys.getenv("RAW_DATA_DIR")
data_dir     <- Sys.getenv("DATA_DIR")
output_dir   <- Sys.getenv("OUTPUT_DIR")


# Sample period knob -----------------------------------------------------------

# How far back to pull CRSP daily data. This is the single biggest driver
# of download time and disk usage.
#
#   "1970-01-01"  full history; supports the by-decade plots. ~20 min.
#   "2000-01-01"  much faster (~5 min), still gives you two decades.
#
# Beaver's own sample was 1961-1965, which predates the CRSP daily file
# (daily CRSP starts in July 1962 and Compustat's rdq field is sparse
# before the 1970s). So we cannot replicate his exact sample -- we
# re-run his RESEARCH DESIGN on modern data. That gap is itself one of
# the discussion questions.
FIRST_DATE <- "1970-01-01"


# Connect to WRDS --------------------------------------------------------------

# Credentials live in your OS keyring (Windows Credential Manager, macOS
# Keychain), NOT in .env and NOT in this file. project_setup() stored them.
# To change them later:
#   keyring::key_set("wrds", "username")
#   keyring::key_set("wrds", "password")

wrds <- dbConnect(Postgres(),
                  host     = "wrds-pgdata.wharton.upenn.edu",
                  port     = 9737,
                  user     = keyring::key_get("wrds", "username"),
                  password = keyring::key_get("wrds", "password"),
                  sslmode  = "require",
                  dbname   = "wrds")

# Prints connection info if the handshake worked.
wrds


# Download the CCM link table --------------------------------------------------

# Compustat identifies firms by gvkey; CRSP by permno. The CCM link table
# maps between them, with validity date ranges (linkdt / linkenddt),
# because the mapping changes over time as firms merge, spin off, or
# re-list.
#
# It is small (~100K rows), so a plain collect() is fine -- that pulls the
# whole table into R memory in one shot.

ccm_path <- glue("{raw_data_dir}/ccm-link.parquet")

if (file.exists(ccm_path)) {
  message("Skipping CCM link download -- file exists: ", ccm_path)
} else {
  tictoc::tic()

  # Download the FULL link table unfiltered. We apply the linktype /
  # linkprim screens in script 2, so you can revisit those choices
  # without re-downloading.
  ccm_link <- tbl(wrds, in_schema("crsp", "ccmxpf_lnkhist")) |>
    collect()

  tictoc::toc()

  nrow(ccm_link)
  write_parquet(ccm_link, ccm_path)
}


# Download Compustat fundq -----------------------------------------------------

# fundq is Compustat's quarterly fundamentals file. The field we care most
# about is `rdq` -- the Report Date of Quarterly Earnings, i.e. the date
# the earnings announcement hit the wire. That is our event date.
#
# The four filters below are the standard Compustat screen and you should
# apply them on essentially every Compustat pull:
#   indfmt  = 'INDL'  industrial format (vs 'FS' financial services)
#   datafmt = 'STD'   standardized data (vs restated / preliminary)
#   popsrc  = 'D'     domestic population source
#   consol  = 'C'     consolidated statements
# Without them you get duplicate rows per firm-quarter.
#
# We keep a few extra fundamentals (saleq, ibq, atq, prccq, cshoq) beyond
# what the base replication strictly needs. They cost almost nothing to
# carry and give you raw material for an extension -- e.g. splitting the
# sample on firm size, or on whether earnings were good news or bad news.

fundq_path <- glue("{raw_data_dir}/fundq-raw.parquet")

fundq_sql <- glue("
  SELECT gvkey, datadate, fyearq, fqtr, rdq, conm, cusip, cik,
         saleq, ibq, epspiq, atq, cshoq, prccq, ajexq
  FROM comp.fundq
  WHERE indfmt = 'INDL' AND datafmt = 'STD'
    AND popsrc = 'D' AND consol = 'C'
    AND rdq IS NOT NULL
    AND datadate >= '{FIRST_DATE}'
")

if (file.exists(fundq_path)) {
  message("Skipping fundq download -- file exists: ", fundq_path)
} else {
  tictoc::tic()

  # This one fits in memory comfortably, so collect() again. For the
  # much larger CRSP daily file below we switch to a chunked approach.
  fundq_raw <- dbGetQuery(wrds, fundq_sql) |> as_tibble()

  tictoc::toc()

  nrow(fundq_raw)
  write_parquet(fundq_raw, fundq_path)
}


# Download CRSP daily stock file -----------------------------------------------

# This is the big one -- roughly 100 million rows for the full history.
# Rather than collect() it into R memory (which would likely blow up your
# session), download_parquet() streams it from the server in batches and
# appends each batch to the parquet file as a row group. Peak memory is
# one batch, not the whole table. See utils.R if you want the details.
#
# WHY THESE COLUMNS:
#   dlyret        daily return -- Beaver's return-variability measure
#   dlyvol        daily share volume -- Beaver's volume measure
#   dlyprc        price, for computing market value
#   shrout        shares outstanding, for turnover (volume / shares)
#   primaryexch   exchange, so we can impose Beaver's NYSE-only screen
#   sharetype /
#   securitytype  common-stock screens
#
# A NOTE ON CRSP "v2" (the CIZ format): WRDS migrated CRSP to a new schema.
# Column names changed (dlycaldt not date, dlyret not ret) and the old
# integer codes were replaced by strings. In particular the old `exchcd`
# (1 = NYSE) is now `primaryexch` ('N' = NYSE, 'A' = NYSE American,
# 'Q' = Nasdaq). Handily, these identifiers now live on the daily file
# itself, so we do NOT need the fiddly date-range join to `stocknames`
# that older code (including the textbook version of this exercise) uses.
#
# We apply the common-stock screen server-side because it discards funds,
# ADRs, and other non-common securities that Beaver's design never
# contemplated -- that is a big row reduction for free. We deliberately do
# NOT filter on exchange here, so you can relax the NYSE screen in script
# 2 without re-downloading.

tictoc::tic()

tbl(wrds, in_schema("crsp", "dsf_v2")) |>
  filter(dlycaldt >= FIRST_DATE,
         sharetype == "NS",      # common / ordinary shares
         securitytype == "EQTY") |>
  select(permno, dlycaldt, dlyret, dlyvol, dlyprc, shrout,
         primaryexch, siccd) |>
  download_parquet(glue("{raw_data_dir}/crsp-dsf-v2.parquet"))

tictoc::toc()


# Download the CRSP market index -----------------------------------------------

# Beaver's return-variability test needs returns measured net of
# market-wide movements -- otherwise a market-wide crash in a firm's
# announcement week would masquerade as an earnings reaction.
#
# indno 1000200 is the CRSP value-weighted index covering
# NYSE/NYSE American/Nasdaq/Arca. dlytotret is its total return.

tictoc::tic()

tbl(wrds, in_schema("crsp", "inddlyseriesdata")) |>
  filter(indno == 1000200, dlycaldt >= FIRST_DATE) |>
  select(dlycaldt, dlytotret) |>
  download_parquet(glue("{raw_data_dir}/crsp-index.parquet"))

tictoc::toc()


# Disconnect -------------------------------------------------------------------

dbDisconnect(wrds)

# You should now have four files in RAW_DATA_DIR:
#   ccm-link.parquet     gvkey -> permno crosswalk
#   fundq-raw.parquet    earnings announcement dates (rdq)
#   crsp-dsf-v2.parquet  daily returns and volume
#   crsp-index.parquet   daily market index returns
#
# Next: src/002-transform-data.R builds the event-window panel.

cat("\nRaw files in", raw_data_dir, ":\n")
print(list.files(raw_data_dir, pattern = "\\.parquet$"))
