# ==============================================================================
# 005-data-provenance.R
#
# Purpose:
#   Produce the JAR-style replication artifacts: sample identifiers and
#   a file inventory of RAW_DATA_DIR / DATA_DIR / OUTPUT_DIR with mtime,
#   size, and SHA256 hash for every file.
#
# Inputs (from DATA_DIR):
#   event-panel.parquet
#
# Outputs (to ./provenance/, a tracked folder in the repo):
#   sample-identifiers.parquet
#   sample-identifiers.csv
#
# Notes:
#   - sample-identifiers.* land in ./provenance/ (not DATA_DIR) because
#     JAR policy expects the sample-identifier artifacts to ship inside
#     the replication package. DATA_DIR sits outside the repo and is
#     gitignored; provenance/ is small, deterministic, and tracked.
#   - Intended to be run via batch_run() so the printed inventory lands
#     inside the script's .Rout file (R version banner at the top,
#     proc.time() block at the bottom). The .Rout itself becomes the
#     provenance log shipped with the rest of the JAR data package.
#   - Style: leans on R's top-level auto-print so the inventory reads
#     cleanly in the .Rout without the noise of cat() / sprintf().
# ==============================================================================


# Setup ------------------------------------------------------------------------

# Installed by renv at the versions pinned in renv.lock. If one of these is
# missing, run src/000-check-setup.R.
library(dotenv)
library(glue)
library(arrow)
library(dplyr)
library(digest)

options(scipen = 999)

load_dot_env(".env")
raw_data_dir <- Sys.getenv("RAW_DATA_DIR")
data_dir     <- Sys.getenv("DATA_DIR")
output_dir   <- Sys.getenv("OUTPUT_DIR")


# Run metadata -----------------------------------------------------------------

# Wall-clock start (R CMD BATCH appends proc.time() at the end for elapsed).
Sys.time()

# Environment configuration
raw_data_dir
data_dir
output_dir


# Export sample identifiers ----------------------------------------------------

# JAR: "whenever feasible, authors should provide the identifiers (e.g.,
# CIK, CUSIP) of all the observations that make up the final sample." A
# replicator with their own WRDS access can use these to verify the
# sample without rerunning scripts 1-2.

# The event panel holds one row per announcement PER TRADING DAY, so we
# take distinct events -- the sample is the set of announcements studied,
# not the set of firm-days.

panel <- read_parquet(glue("{data_dir}/event-panel.parquet"))

sample_ids <- panel |>
  distinct(gvkey, permno, rdq, datadate, fyearq) |>
  arrange(gvkey, rdq)

# Write into the repo's tracked provenance/ folder so the sample-ids
# ship with the code, not stranded outside the repo in DATA_DIR.
provenance_dir <- "provenance"
if (!dir.exists(provenance_dir)) dir.create(provenance_dir)
write_parquet(sample_ids,
              file.path(provenance_dir, "sample-identifiers.parquet"))
write.csv(sample_ids,
          file.path(provenance_dir, "sample-identifiers.csv"),
          row.names = FALSE)

# Sample summary
nrow(sample_ids)              # rows
n_distinct(sample_ids$gvkey)  # distinct gvkeys
range(sample_ids$rdq)         # rdq range


# File inventory ---------------------------------------------------------------

# Function header below uses roxygen2 syntax (the `#'` prefix). Roxygen2 is
# the de facto standard for documenting R functions — even in non-package
# code it's worth using, because RStudio renders it as in-editor help and
# any reader familiar with R recognizes `@param` / `@return` / `@examples`.

#' Print a directory listing with mtime, size, and SHA256 hash per file.
#'
#' Used to record what was on disk at the time the script ran. Bails out
#' cleanly if `dir` is unset or missing so the rest of the script can
#' still report what it can find.
#'
#' @param dir Path to the directory to inventory. `NULL`, empty string,
#'   or a non-existent path all produce a "skipping" message and an
#'   invisible NULL return.
#' @return Invisible `NULL`. Called for the printing side effect.
#' @examples
#'   list_dir(Sys.getenv("DATA_DIR"))
list_dir <- function(dir) {
  if (is.null(dir) || !nzchar(dir) || !dir.exists(dir)) {
    message("(directory not set or missing; skipping)")
    return(invisible())
  }

  # `no.. = TRUE` excludes "." and ".." from the listing. Sorting keeps
  # the inventory in a deterministic order across runs.
  files <- sort(list.files(dir, no.. = TRUE))

  for (f in files) {
    path <- file.path(dir, f)
    if (dir.exists(path)) next  # one-level only; skip subdirectories

    # `file.info` returns mtime + size in one call. `digest::digest` with
    # `file = path` streams the file from disk so we don't load big
    # parquets into RAM just to hash them.
    info <- file.info(path)
    sha  <- digest::digest(file = path, algo = "sha256")

    # `message()` writes to stderr, which `R CMD BATCH` captures into
    # the .Rout. The column widths in `sprintf` line everything up so
    # the inventory reads like a table.
    message(sprintf("  %-35s  %s  %8.1f MB  sha256=%s",
                    f,
                    format(info$mtime, "%Y-%m-%d %H:%M"),
                    info$size / 1e6,
                    sha))
  }
}

# Raw data (RAW_DATA_DIR)
list_dir(raw_data_dir)

# Derived data (DATA_DIR)
list_dir(data_dir)

# Output (OUTPUT_DIR)
list_dir(output_dir)

# Provenance (./provenance, tracked in git)
list_dir(provenance_dir)
