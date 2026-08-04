
# R Options --------------------------------------------------------------------

# I like to always get rid of scientific notation
options(scipen=999)
# print blanks for NAs in Kable documents 
options(knitr.kable.NA = '')

message("set R formatting options")

# Parquet functions ------------------------------------------------------------

# Re-exports of arrow read/write so scripts can call read_parquet() /
# write_parquet() without `library(arrow)` and without thinking about
# compression defaults. Roxygen2 (#' style comments) is the standard for
# documenting R functions — RStudio renders these blocks as in-editor help.

#' Read a parquet file into a tibble. Thin alias for `arrow::read_parquet`.
read_parquet <- arrow::read_parquet

#' Write a tibble to parquet with sensible compression defaults.
#'
#' @param x A data.frame or tibble.
#' @param p Output file path.
#' @return Invisibly, the written object (same as `arrow::write_parquet`).
write_parquet <- function(x, p) {
  arrow::write_parquet(x, p, compression = "gzip", compression_level = 5)
}

message("imported parquet functions")


# Variable transformation functions --------------------------------------------

#' Standardize a numeric vector to mean 0, standard deviation 1.
#'
#' @param x Numeric vector. NAs are propagated (`na.rm = TRUE` is used for
#'   the mean and SD, so the centering is computed on the non-NA values).
#' @return Numeric vector of the same length as `x`.
#' @examples
#'   standardize(c(1, 2, 3, 4, 5))
standardize <- function(x){
  (x - mean(x, na.rm=TRUE)) / sd(x, na.rm=TRUE)
}


#' Winsorize a numeric vector at the given lower/upper quantiles.
#'
#' Replaces extreme values with the nearest non-trimmed quantile (clipping,
#' not deletion). Useful inside `dplyr::mutate()`.
#'
#' @param x Numeric vector.
#' @param cuts Length-2 numeric vector `c(bottom, top)`. Defaults to 1% on
#'   each side (`c(0.01, 0.01)`). Pass `c(0, 0.01)` for one-sided
#'   winsorization.
#' @return Numeric vector of the same length as `x`, with values clipped
#'   to the chosen quantiles.
#' @details
#'   `quantile(..., type = 2)` returns an actual observation (no
#'   interpolation), so after winsorizing the empirical quantiles at the
#'   cut points are equal to the cut values.
winsorize_x = function(x, cuts = c(0.01, 0.01)) {
  cut_point_top    <- quantile(x, 1 - cuts[2], na.rm = TRUE, type = 2)
  cut_point_bottom <- quantile(x,     cuts[1], na.rm = TRUE, type = 2)
  i <- which(x >= cut_point_top)
  x[i] <- cut_point_top
  j <- which(x <= cut_point_bottom)
  x[j] <- cut_point_bottom
  return(x)
}

#' Truncate a numeric vector at the given lower/upper quantiles.
#'
#' Like `winsorize_x()` but replaces extreme values with `NA` instead of
#' clipping to the cutoff. Use truncation when you want to exclude
#' outliers from downstream calculations entirely; use winsorization when
#' you want to keep the observations but cap their influence.
#'
#' @param x Numeric vector.
#' @param cuts Length-2 numeric vector `c(bottom, top)`; default `c(0.01, 0.01)`.
#' @return Numeric vector of the same length as `x` with extremes set to NA.
truncate_x = function(x, cuts = c(0.01, 0.01)) {
  cut_point_top    <- quantile(x, 1 - cuts[2], na.rm = TRUE, type = 2)
  cut_point_bottom <- quantile(x,     cuts[1], na.rm = TRUE, type = 2)
  i <- which(x >= cut_point_top)
  x[i] <- NA_real_
  j <- which(x <= cut_point_bottom)
  x[j] <- NA_real_
  return(x)
}

message("imported transformation functions")

# Check Duplicates -------------------------------------------------------------

#' Find and inspect duplicate groups in a data frame.
#'
#' Counts the number of rows per group at the levels you pass via `...`,
#' reports any duplicates, and (interactively) offers to save the
#' duplicated rows to the global environment as `duplicated_data` for
#' inspection. Useful for debugging unique-key assumptions before a join.
#'
#' @param data A data frame / tibble.
#' @param ... Bare column names that should uniquely identify each row
#'   (e.g. `gvkey, datadate`).
#' @return Invisible NULL. Called for side effects: prints a one-line
#'   summary, optionally saves duplicated rows.
#' @examples
#' \dontrun{
#'   check_duplicates(fundq, gvkey, datadate)
#' }
check_duplicates <- function(data, ...) {
  # Capture the variable names
  group_vars <- rlang::enquos(...)
  
  # Count duplicates at the specified level
  duplicated_data <- data |> 
    dplyr::group_by(!!!group_vars) |> 
    dplyr::mutate(dup_count = n()) |> 
    dplyr::ungroup() |> 
    dplyr::filter(dup_count > 1) |> 
    dplyr::select(-dup_count)  # Remove the count column
  
  
  # Count the number of duplicated groups
  num_groups <- duplicated_data |> dplyr::distinct(!!!group_vars) |> nrow()
  
  # Count the number of extra observations
  extra_obs <- nrow(duplicated_data) - num_groups
  
  # Get variable names as a string
  var_names <- base::paste(purrr::map_chr(group_vars, rlang::as_name), collapse = ", ")
  
  # If there are no duplicates, tell the user
  if (base::nrow(duplicated_data) == 0) {
    base::message(glue::glue("No duplicates at the {var_names} level"))
  } 
  # If there are duplicates, do the following
  else {
    # Define the dynamic wording for output message
    multi_groups <- dplyr::if_else(num_groups == 1, 'group has', 'groups have')
    multi_dups1 <- dplyr::if_else(extra_obs == 1, 'is', 'are')
    multi_dups2 <- dplyr::if_else(extra_obs == 1, 'extra observation', 'extra observations')
    
    # tell the user there are duplicates and how many
    base::message(glue::glue("Duplicates found at the {var_names} level. {num_groups} {multi_groups} duplicates and there {multi_dups1} {extra_obs} {multi_dups2} detected."))
    
    # reorder the columns to have grouping variables at the beginning
    col_order <- c(purrr::map_chr(group_vars, rlang::as_name), setdiff(names(duplicated_data), purrr::map_chr(group_vars, rlang::as_name)))
    duplicated_data <- duplicated_data |> dplyr::select(all_of(col_order))
    
    # Show a preview of duplicate data
    print(duplicated_data |> dplyr::slice_head(n = 5))
    
    # Switch cursor to console before prompting for input
    rstudioapi::executeCommand("activateConsole")
    
    # Ask the user if they want to save the dataset
    response <- readline(prompt = "Would you like to save the duplicated dataset as 'duplicated_data'? (y/n): ")
    
    # Respond to the user input
    if (tolower(response) == "y") {
      base::assign("duplicated_data", duplicated_data, envir = .GlobalEnv)  # Save to global environment
      base::message("Dataset saved as 'duplicated_data' in the environment.")
    } else {
      base::message("Dataset not saved.")
    }
  }
}

message("imported duplicate check function")


# Row count for either lazy or in-memory tables -------------------------------

#' Count rows of either a lazy dbplyr table or an in-memory data frame.
#'
#' Lets you track sample-selection counts without forcing a full
#' `collect()` of a dbplyr query (which would pull all rows into R).
#' For lazy tables it runs a `SELECT COUNT(*)` on the database; for
#' in-memory data frames it just calls `nrow()`.
#'
#' @param x A dbplyr lazy table or a data frame / tibble.
#' @return Integer scalar — the number of rows.
#' @examples
#' \dontrun{
#'   count_rows(fundq_tbl)             # lazy: COUNT(*) on the DB
#'   count_rows(collect(fundq_tbl))    # in-memory: nrow()
#' }
count_rows <- function(x) {
  if (inherits(x, "tbl_lazy")) {
    x |> dplyr::summarize(n = dplyr::n()) |> dplyr::pull(n) |> as.integer()
  } else {
    nrow(x)
  }
}

message("imported count_rows function")


# Streaming download to parquet ------------------------------------------------

#' Stream a dbplyr lazy table to a local parquet file.
#'
#' Database-agnostic — works with any dbplyr `tbl()` backed by a DBI
#' connection (WRDS PostgreSQL via RPostgres, BigQuery via bigrquery,
#' Snowflake via odbc, local DuckDB, etc.). Used to be called
#' `download_wrds()`; the rename reflects that the WRDS-specific bit
#' lives in your connection setup, not in this function.
#'
#' @param tbl A dbplyr lazy table built via `tbl(connection, ...)` and
#'   optional dplyr verbs (`filter`, `select`, `mutate`, joins). The
#'   query is rendered to SQL and executed on the database.
#' @param output_path File path for the output parquet file.
#' @param max_ram_mb Target peak RAM in MB for auto-sized batches.
#'   Default 8000 (8 GB). Ignored if `batch_size` is set explicitly.
#' @param batch_size Rows per fetch from the server-side cursor. `NULL`
#'   (default) auto-sizes from `max_ram_mb` by peeking at the number of
#'   columns in the query result, then capping at 5 million rows so
#'   progress messages appear at sensible intervals. Pass an integer to
#'   override (no cap is applied).
#' @param compression Parquet compression codec. Default `"zstd"` —
#'   smaller files than the arrow default of `"snappy"`.
#' @param skip_if_exists If `TRUE` (default), skip the download and
#'   return immediately when `output_path` already exists. Delete the
#'   file to force a refresh. Makes replication runs fast.
#'
#' @return Invisible integer — the number of rows downloaded, or `0` if
#'   the file already existed and was skipped.
#'
#' @details
#' Each batch is written via `arrow::ParquetFileWriter`, so column
#' types are preserved and peak RAM stays bounded regardless of total
#' table size. Peak RAM ≈ `batch_size * n_columns * 8 bytes` plus 1-2 GB
#' of baseline overhead (R session, loaded packages, parquet writer
#' buffers). On a 16 GB machine the default 8 GB target is conservative;
#' on a 4 GB machine try `max_ram_mb = 2000`.
#'
#' This function follows the same shape as Ian Gow's
#' `db2pq::lazy_tbl_to_pq()`, with the addition of auto-sized batches
#' and zstd compression by default. See `001-download-data.R` for the
#' un-wrapped version of this code (the Compustat fundq download), which
#' shows each step in detail.
#'
#' @examples
#' \dontrun{
#'   # Pipe a lazy tbl into the function (most common):
#'   tbl(wrds, in_schema("crsp", "dsf_v2")) |>
#'     filter(dlycaldt >= "1970-01-01") |>
#'     select(permno, dlycaldt, dlyret) |>
#'     download_parquet("data/crsp.parquet")
#'
#'   # Lower RAM target (e.g. 4 GB machine):
#'   tbl(wrds, in_schema("comp", "funda")) |>
#'     download_parquet("data/funda.parquet", max_ram_mb = 2000)
#'
#'   # Explicit batch size (no cap applied):
#'   tbl(wrds, ...) |> download_parquet("out.parquet", batch_size = 500000)
#' }
download_parquet <- function(tbl, output_path, max_ram_mb = 8000,
                          batch_size = NULL, compression = "zstd",
                          skip_if_exists = TRUE) {

  # Skip-if-exists: if the parquet is already on disk, don't re-pull.
  # This is the JAR-replication-friendly default — a downstream user can
  # re-run the pipeline without hitting WRDS, and the original analyst can
  # delete a single file to refresh just that table.
  if (skip_if_exists && file.exists(output_path)) {
    size_mb <- file.size(output_path) / 1e6
    message(sprintf("  Skipping download — file exists: %s (%.1f MB)",
                    output_path, size_mb))
    return(invisible(0L))
  }

  # Extract connection and SQL from the lazy tbl. dbplyr::sql_render translates
  # the dplyr chain to SQL; remote_con returns the underlying DBI connection.
  con <- dbplyr::remote_con(tbl)
  sql <- as.character(dbplyr::sql_render(tbl))

  # Auto-calculate batch_size from max_ram_mb if not specified.
  # We peek at the number of columns by running the query with LIMIT 0.
  if (is.null(batch_size)) {
    peek <- DBI::dbGetQuery(con, paste0("SELECT * FROM (", sql, ") q LIMIT 0"))
    n_cols <- ncol(peek)
    # ~8 bytes per value (doubles, dates, integers are all 8 bytes in R)
    raw <- max(1000L, as.integer(floor(max_ram_mb * 1e6 / (n_cols * 8))))
    # Silent cap: large narrow tables would otherwise be fetched in one
    # giant batch, leaving the user staring at no output for minutes.
    # Capping at 5M lets progress messages print every batch (~10-20s on
    # a 100M-row download) so users see things moving.
    batch_size <- min(raw, 5000000L)
    message(sprintf("  Auto batch size: %s rows (%d columns, %s MB RAM target)",
                    format(batch_size, big.mark = ","), n_cols,
                    format(max_ram_mb, big.mark = ",")))
  }

  # Open a server-side cursor and stream rows in batches
  res <- DBI::dbSendQuery(con, sql)
  sink <- arrow::FileOutputStream$create(output_path)
  writer <- NULL
  total_rows <- 0
  start_time <- Sys.time()

  while (!DBI::dbHasCompleted(res)) {
    chunk <- DBI::dbFetch(res, n = batch_size)
    if (nrow(chunk) == 0) break

    total_rows <- total_rows + nrow(chunk)

    # Progress: rows, elapsed time, file size on disk
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
    size_so_far <- if (file.exists(output_path)) file.size(output_path) / 1e6 else 0
    cat(sprintf("\r  %s rows | %.1f min | ~%.0f MB on disk",
                format(total_rows, big.mark = ","), elapsed, size_so_far))

    tab <- arrow::Table$create(chunk)

    if (is.null(writer)) {
      writer <- arrow::ParquetFileWriter$create(
        schema = tab$schema,
        sink = sink,
        properties = arrow::ParquetWriterProperties$create(
          column_names = tab$schema$names,
          compression = compression
        )
      )
    }

    writer$WriteTable(tab, chunk_size = batch_size)
  }

  if (!is.null(writer)) writer$Close()
  sink$close()
  DBI::dbClearResult(res)
  cat("\n")

  size_mb <- file.size(output_path) / 1e6
  message(sprintf("Saved %s rows, %.1f MB -> %s",
                  format(total_rows, big.mark = ","), size_mb, output_path))

  invisible(total_rows)
}

message("imported download_parquet function")


# Run an R script via R CMD BATCH ----------------------------------------------

#' Run an R script via R CMD BATCH and write its .Rout log.
#'
#' `batch_run()` is `source()`-with-a-receipt: it spawns a fresh child R
#' process, runs the script there, and writes a sibling `.Rout` file
#' capturing the R version banner, every command echoed with `>` / `+`,
#' output interleaved, and a closing `proc.time()` block. Visually the
#' result is identical to a SAS or Stata log.
#'
#' @param file Path to the .R script to execute.
#' @param log_path Where to write the .Rout. Default is sibling .Rout
#'   (e.g. `pulls/foo.R` -> `pulls/foo.Rout`), matching R CMD BATCH's
#'   own default.
#' @param vanilla If `TRUE` (default), pass `--vanilla` to skip the
#'   user's `.Rprofile` / `.Renviron` so the run is reproducible.
#'   Override with `FALSE` only if you have a project-level `.Rprofile`
#'   that the pipeline depends on.
#' @param open If `TRUE` (default), auto-open the .Rout in your editor
#'   when finished. Set `FALSE` for non-interactive callers like
#'   run-all.R or CI.
#'
#' @return Invisible list with elements `status` (the R CMD BATCH exit
#'   code) and `log_path` (where the .Rout landed).
#'
#' @details
#' Why a child process? R CMD BATCH gives you isolation (the script
#' can't pollute your interactive session, and your interactive session
#' can't pollute the script), plus the canonical .Rout format for free.
#' `source(echo = TRUE)` runs in your current session and spreads
#' `sink()`/output through your live console — fine for live work,
#' wrong for a posterity log.
#'
#' @examples
#' \dontrun{
#'   # Interactive: write a one-off pull, then batch_run it.
#'   batch_run("pulls/2026-04-29-fundq-2020.R")
#'
#'   # In run-all: explicit log_path, no editor pop-up.
#'   batch_run("src/001-download-data.R",
#'             log_path = "log/001-download-data.Rout",
#'             open     = FALSE)
#' }
batch_run <- function(file,
                     log_path = NULL,
                     vanilla  = TRUE,
                     open     = TRUE) {

  if (!file.exists(file)) {
    stop("batch_run: script not found: ", file)
  }

  if (is.null(log_path)) {
    log_path <- sub("\\.R$", ".Rout", file, ignore.case = TRUE)
    if (log_path == file) log_path <- paste0(file, ".Rout")
  }

  args <- c("CMD", "BATCH")
  if (vanilla) args <- c(args, "--vanilla")
  args <- c(args, shQuote(file), shQuote(log_path))

  status <- system2("R", args)

  if (status == 0) {
    message(sprintf("batch_run OK -> %s", log_path))
  } else {
    warning(sprintf("R CMD BATCH exited %d (see %s)", status, log_path))
  }

  if (open && interactive()) {
    try(file.edit(log_path), silent = TRUE)
  }

  invisible(list(status = status, log_path = log_path))
}

message("imported batch_run function")


# Find an external interpreter binary -----------------------------------------

# Helper used by batch_run_stata() / batch_run_sas() to locate the
# interpreter on the user's machine. Order:
#   1. The named env var (e.g. STATA_BIN, SAS_BIN) if set.
#   2. Sys.which() against each candidate name (works if it's on PATH).
#   3. The first existing file from a list of common install paths.
# Returns the resolved path, or NULL if none of the above worked.

.find_external_bin <- function(env_var, candidate_names, common_paths) {
  bin <- Sys.getenv(env_var, unset = "")
  if (nzchar(bin) && file.exists(bin)) return(bin)

  for (nm in candidate_names) {
    found <- unname(Sys.which(nm))
    if (nzchar(found)) return(found)
  }

  for (p in common_paths) {
    hits <- Sys.glob(p)
    if (length(hits) > 0L) return(hits[[1L]])
  }

  NULL
}


# Run a Stata script via stata -b do ------------------------------------------

#' Run a Stata .do file in batch mode and write its log into log/.
#'
#' Spawns `stata -b do <file>`, which produces a log alongside the .do
#' file. We move it to `log_path` afterwards so all per-script logs end
#' up in the same place. Default log filename includes a `-stata`
#' suffix so it does not collide with a same-stem Python `.log`.
#'
#' @param file Path to the .do file to execute.
#' @param log_path Where to write the log. Default
#'   `log/<basename>-stata.log`.
#' @param stata_bin Override path to the Stata executable. Default
#'   `NULL` triggers a search: `STATA_BIN` env var → `Sys.which("stata")`
#'   / `which stata-mp` / `which StataMP-64` → common install paths →
#'   error with instructions.
#'
#' @return Invisible list with `status` (exit code) and `log_path`.
#'
#' @details
#' Stata does not add itself to PATH on install on any platform, so
#' many users will need to set `STATA_BIN` in `.env`. The function tries
#' default install paths automatically; only custom installs need
#' explicit configuration. Requires Stata 17+ (the bundled
#' `004-analyze-data.do` uses Stata's `collect` framework).
#'
#' @examples
#' \dontrun{
#'   batch_run_stata("src/004-analyze-data.do")
#' }
batch_run_stata <- function(file,
                            log_path  = NULL,
                            stata_bin = NULL) {

  if (!file.exists(file)) {
    stop("batch_run_stata: script not found: ", file, call. = FALSE)
  }

  if (is.null(log_path)) {
    base     <- sub("\\.do$", "", basename(file), ignore.case = TRUE)
    log_path <- file.path("log", paste0(base, "-stata.log"))
  }
  dir.create(dirname(log_path), showWarnings = FALSE, recursive = TRUE)

  if (is.null(stata_bin)) {
    stata_bin <- .find_external_bin(
      env_var = "STATA_BIN",
      candidate_names = c("stata", "stata-mp", "stata-se", "stata-be",
                          "StataMP-64", "StataSE-64", "StataBE-64"),
      common_paths = c(
        "C:/Program Files/Stata*/Stata*-64.exe",
        "/Applications/Stata/Stata*.app/Contents/MacOS/Stata*",
        "/usr/local/stata*/stata-*"
      )
    )
  }
  if (is.null(stata_bin)) {
    stop("batch_run_stata: could not locate Stata. Add it to PATH or set ",
         "STATA_BIN=\"path/to/stata.exe\" in .env.", call. = FALSE)
  }

  status <- system2(stata_bin, args = c("-b", "do", shQuote(file)))

  # Stata's -b mode writes <basename>.log into the current working directory
  # (not next to the .do file). Move it so all per-script logs live under log/.
  produced <- sub("\\.do$", ".log", basename(file), ignore.case = TRUE)
  if (file.exists(produced) && normalizePath(produced, mustWork = FALSE) !=
                               normalizePath(log_path, mustWork = FALSE)) {
    file.copy(produced, log_path, overwrite = TRUE)
    file.remove(produced)
  }

  if (status == 0) {
    message(sprintf("batch_run_stata OK -> %s", log_path))
  } else {
    warning(sprintf("Stata exited %d (see %s)", status, log_path))
  }

  invisible(list(status = status, log_path = log_path))
}

message("imported batch_run_stata function")


# Run a SAS script via sas -sysin ---------------------------------------------

#' Run a SAS .sas file in batch mode and write its log into log/.
#'
#' Mirrors `batch_run_stata()` for SAS. Uses `-SYSIN <file> -LOG <log_path>`
#' so SAS writes its log directly to the requested path (no post-hoc
#' move needed). Default log filename includes a `-sas` suffix to keep
#' it from colliding with same-stem Python `.log` files.
#'
#' This template doesn't currently include any `.sas` scripts, but the
#' helper is useful in projects that mix SAS into the pipeline.
#'
#' @param file Path to the .sas file to execute.
#' @param log_path Where to write the log. Default `log/<basename>-sas.log`.
#' @param sas_bin Override path to the SAS executable. Default `NULL`
#'   triggers a search: `SAS_BIN` env var → `Sys.which("sas")` →
#'   common install paths → error.
#' @param work_dir Override path for SAS's WORK library. Default `NULL`
#'   reads `SAS_WORK_DIR` from the environment; if set, passes
#'   `-WORK <path>` so SAS uses that directory instead of `%TEMP%`.
#'   Useful when the system default lands on a drive without enough
#'   free space for the pipeline's intermediate datasets.
#'
#' @return Invisible list with `status` (exit code) and `log_path`.
#'
#' @examples
#' \dontrun{
#'   batch_run_sas("src/001-download-wrds-data.sas")
#' }
batch_run_sas <- function(file,
                          log_path = NULL,
                          sas_bin  = NULL,
                          work_dir = NULL) {

  if (!file.exists(file)) {
    stop("batch_run_sas: script not found: ", file, call. = FALSE)
  }

  if (is.null(log_path)) {
    base     <- sub("\\.sas$", "", basename(file), ignore.case = TRUE)
    log_path <- file.path("log", paste0(base, "-sas.log"))
  }
  dir.create(dirname(log_path), showWarnings = FALSE, recursive = TRUE)

  if (is.null(sas_bin)) {
    sas_bin <- .find_external_bin(
      env_var = "SAS_BIN",
      candidate_names = c("sas"),
      common_paths = c(
        "C:/Program Files/SASHome/SASFoundation/*/sas.exe",
        "/usr/local/SASHome/SASFoundation/*/sas",
        "/opt/sas/SASHome/SASFoundation/*/sas"
      )
    )
  }
  if (is.null(sas_bin)) {
    stop("batch_run_sas: could not locate SAS. Add it to PATH or set ",
         "SAS_BIN=\"path/to/sas.exe\" in .env.", call. = FALSE)
  }

  if (is.null(work_dir)) {
    work_dir <- Sys.getenv("SAS_WORK_DIR", unset = "")
  }

  args <- c("-SYSIN", shQuote(file), "-LOG", shQuote(log_path))
  if (nzchar(work_dir)) {
    dir.create(work_dir, showWarnings = FALSE, recursive = TRUE)
    args <- c(args, "-WORK", shQuote(work_dir))
  }

  status <- system2(sas_bin, args = args)

  if (status == 0) {
    message(sprintf("batch_run_sas OK -> %s", log_path))
  } else {
    warning(sprintf("SAS exited %d (see %s)", status, log_path))
  }

  invisible(list(status = status, log_path = log_path))
}

message("imported batch_run_sas function")


# First-time project setup -----------------------------------------------------

#' First-time project setup: language combo, paths, credentials, prune.
#'
#' Idempotent first-run helper called at the top of `001-download-data.R`.
#' On the first call (no `.env` yet) it walks the user through choosing
#' a language combination, entering data and output directories, storing
#' WRDS credentials in the OS keyring, and optionally pruning files for
#' languages they didn't pick. On subsequent calls it sees `.env` on
#' disk and returns immediately — the file's existence is the "have I
#' been set up?" flag, no separate state.
#'
#' @param force If `TRUE`, run the prompts even when `.env` already
#'   exists (e.g. to redo language pruning or refresh credentials).
#'   Default `FALSE`.
#' @return Invisible NULL. Side effects: writes `.env`, sets keyring
#'   entries, optionally deletes files in `src/`.
#'
#' @details
#' Why is this a function in utils.R instead of a standalone setup.R
#' script? Two reasons. First, `readline()` is finicky inside
#' `source()`'d scripts but works reliably when called from an
#' interactive console — even from inside a function. Second, baking
#' setup into the top of script 1 means a fresh clone "just works" the
#' first time you try to run it; there's no separate setup step the
#' user has to remember.
#'
#' By design this function only offers R-inclusive combos (1, 3, 5, 6),
#' so it cannot delete itself or the file that called it. The Python
#' sister `project_setup()` in `utils.py` mirrors this rule with
#' Python-inclusive combos.
#'
#' @examples
#' \dontrun{
#'   # Top of 001-download-data.R:
#'   source("src/utils.R")
#'   project_setup()       # prompts on first run, no-op afterwards
#' }
project_setup <- function(force = FALSE) {
  if (!force && file.exists(".env")) {
    return(invisible())
  }

  if (!interactive()) {
    stop(
      "project_setup: no .env file and R is not interactive.\n",
      "  Open src/001-download-data.R in RStudio and run it interactively\n",
      "  to walk through first-time setup, then any subsequent run\n",
      "  (including run-all.R) will work.",
      call. = FALSE
    )
  }

  cat("\n=== First-time project setup ===\n\n")

  combo  <- .ask_language_combo()
  paths  <- .ask_paths()
  .write_env(paths)
  .ask_credentials()
  .maybe_prune(combo)

  cat("\nSetup complete. .env is on disk; future runs skip this prompt.\n\n")
  invisible()
}


# --- project_setup helpers --------------------------------------------------

# Combos that include R. Self-deletion is impossible because we never offer
# a combo that excludes R from this function.
.R_COMBOS <- list(
  "1" = list(name = "Full R (no Python, no Stata)",
             exts = c("R")),
  "3" = list(name = "Python + R (parallel figures and tables)",
             exts = c("R", "py")),
  "5" = list(name = "R + Stata (R pipeline, Stata tables)",
             exts = c("R", "do")),
  "6" = list(name = "All three (R + Python + Stata; demo / comparison mode)",
             exts = c("R", "py", "do"))
)


.ask_language_combo <- function() {
  cat("Which language(s) will this project use?\n")
  cat("  1. Full R\n")
  cat("  3. Python + R\n")
  cat("  5. R + Stata\n")
  cat("  6. All three (default; useful for demos)\n\n")
  choice <- trimws(readline(prompt = "Choice [6]: "))
  if (!nzchar(choice)) choice <- "6"
  if (is.null(.R_COMBOS[[choice]])) {
    stop("Invalid choice ", choice, ". Pick 1, 3, 5, or 6.", call. = FALSE)
  }
  combo <- .R_COMBOS[[choice]]
  cat("Selected: ", combo$name, "\n", sep = "")
  combo
}


.ask_paths <- function() {
  cat("\n--- Paths ---\n")
  cat("Use forward slashes (/) on Windows. These should be OUTSIDE the\n")
  cat("project folder (e.g. a Dropbox folder), since data is not committed.\n\n")
  raw <- gsub("\\\\", "/",
              trimws(readline(prompt = "RAW_DATA_DIR (raw WRDS pulls): ")))
  derived <- gsub("\\\\", "/",
                  trimws(readline(prompt = "DATA_DIR (derived parquets): ")))
  output <- gsub("\\\\", "/",
                 trimws(readline(prompt = "OUTPUT_DIR [output]: ")))
  if (!nzchar(output)) output <- "output"
  list(raw = raw, derived = derived, output = output)
}


.write_env <- function(paths) {
  writeLines(
    c(
      paste0("RAW_DATA_DIR=", paths$raw),
      paste0("DATA_DIR=",     paths$derived),
      paste0("OUTPUT_DIR=",   paths$output)
    ),
    ".env"
  )
  for (d in c(paths$raw, paths$derived, paths$output)) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE)
      cat("Created ", d, "\n", sep = "")
    }
  }
  cat(".env written\n")
}


.ask_credentials <- function() {
  if (!requireNamespace("keyring", quietly = TRUE)) {
    install.packages("keyring")
  }
  cat("\n--- WRDS credentials ---\n")
  cat("Stored in your OS keyring (Windows Credential Manager / macOS\n")
  cat("Keychain). Both R and Python read from the same entries.\n\n")
  existing <- tryCatch(keyring::key_get("wrds", "username"),
                       error = function(e) "")
  if (nzchar(existing)) {
    cat("Existing WRDS username: ", existing, "\n", sep = "")
    if (tolower(trimws(readline("Update? (y/n) [n]: "))) != "y") {
      return(invisible())
    }
  }
  wrds_user <- trimws(readline("WRDS username: "))
  wrds_pw   <- trimws(readline("WRDS password: "))
  keyring::key_set_with_value("wrds", "username", password = wrds_user)
  keyring::key_set_with_value("wrds", "password", password = wrds_pw)
}


.NUMBERED <- c("001-download-data", "002-transform-data", "003-figures",
               "004-analyze-data", "005-data-provenance")


.maybe_prune <- function(combo) {
  delete <- character()
  for (stem in .NUMBERED) {
    for (ext in c("py", "R", "do")) {
      f <- file.path("src", paste0(stem, ".", ext))
      if (file.exists(f) && !(ext %in% combo$exts)) {
        delete <- c(delete, f)
      }
    }
  }
  if (!("py" %in% combo$exts)) {
    for (f in c("src/utils.py", "src/run_with_echo.py", "src/run-all.py",
                "pyproject.toml", "uv.lock", ".python-version")) {
      if (file.exists(f)) delete <- c(delete, f)
    }
  }
  if (!("do" %in% combo$exts)) {
    f <- "src/004-analyze-data.do"
    if (file.exists(f)) delete <- unique(c(delete, f))
  }

  if (length(delete) == 0L) {
    cat("\nNo files to prune for this combo.\n")
    return(invisible())
  }

  cat("\nThe following files are not needed for ", combo$name, ":\n", sep = "")
  cat("  ", delete, sep = "\n  "); cat("\n\n")
  if (tolower(trimws(readline("Delete them? (y/n) [n]: "))) != "y") {
    cat("Skipped pruning. Files left in place.\n")
    return(invisible())
  }
  for (f in delete) {
    ok <- tryCatch(file.remove(f), error = function(e) FALSE)
    cat(if (isTRUE(ok)) "  removed " else "  WARN could not delete ",
        f, "\n", sep = "")
  }
}


message("imported project_setup function")


# Trading Days Function --------------------------------------------------------

#' Expand event dates into trading-day windows for an event study.
#'
#' Given a data frame with event dates and a reference table of trading
#' dates, this expands each row into `(plus_offset - minus_offset + 1)`
#' rows — one per trading day in the window around the event. Useful
#' for computing CARs, abnormal volume, etc.
#'
#' @param data Data frame containing at least one date column (the
#'   event date) and any identifiers you want to keep (e.g. permno).
#' @param trading_dates Data frame with columns `date` (trading date)
#'   and `td` (integer trading day number, sequential with no gaps).
#'   Build it as
#'   `crsp_daily |> distinct(date) |> arrange(date) |> mutate(td = row_number())`.
#' @param event_col Unquoted column name of the event date in `data`.
#' @param minus_offset Negative integer; e.g. `-1` for one trading day
#'   before the event.
#' @param plus_offset Positive integer; e.g. `+1` for one trading day
#'   after the event.
#'
#' @return The input data expanded so each row becomes
#'   `(plus_offset - minus_offset + 1)` rows, with new columns:
#'   `td` (event's trading day number), `offset`, `rel_td` (`td + offset`),
#'   and `date` (the calendar date of that trading day).
#'
#' @examples
#' \dontrun{
#'   # Earnings-announcement [-1, +1] window:
#'   ea_windows <- trading_day_window(earnings, trading_dates, rdq, -1, 1)
#' }

trading_day_window <- function(data, trading_dates, event_col, minus_offset, plus_offset) {

  #Convert column names to symbols for dynamic programming
  event_col <- ensym(event_col)

  #Max trading date
  max_td <- max(trading_dates$date) - plus_offset

  data |>
    #Filter out out of bounds event dates (the !! makes any variable input read as column)
    filter(!!event_col <= max_td) |>
    #Find closest trading date equal to or after the event date specified
    inner_join(trading_dates, by = join_by(closest(!!event_col <= date))) |>
    #Drop date variable to avoid duplicate "date" variables
    select(-date) |>
    #Expand rows for the range of trading days specified by the user
    expand_grid(offset = seq(minus_offset, plus_offset)) |>
    #Create "rel_td" which adds the offset to the initial trading day number which will be used in subsequent merge
    mutate(rel_td = td + offset) |>
    #Join again with trading_dates to get actual dates
    left_join(trading_dates, by = c("rel_td" = "td")) |>
    #Arrange rows for readability
    arrange(across(everything()))
}

message("imported trading day window function")



# Industry functions -----------------------------------------------------------

# Industry classification helpers below use the Fama-French SIC-range
# definitions from Kenneth French's data library. Each function takes a
# numeric `sic` (vector or scalar) and returns the industry label or
# number. Pair `assign_FF12()` with `assign_FF12_num()` if you want both
# the human-readable name and the numeric code in your output.

#' Map SIC code(s) to a Fama-French 12 industry name.
#'
#' @param sic Numeric SIC code, scalar or vector. Codes outside any
#'   defined range collapse to `"Other"`.
#' @return Character vector of FF12 industry names, same length as `sic`.
#' @examples
#'   assign_FF12(c(2000, 5500, 8000))  # Consumer Nondurables, Retail, Healthcare
assign_FF12 <- function(sic) {
  dplyr::case_when(
    sic >= 0100 & sic <= 0999 ~ "Consumer Nondurables",
    sic >= 2000 & sic <= 2399 ~ "Consumer Nondurables",
    sic >= 2700 & sic <= 2749 ~ "Consumer Nondurables",
    sic >= 2770 & sic <= 2799 ~ "Consumer Nondurables",
    sic >= 3100 & sic <= 3199 ~ "Consumer Nondurables",
    sic >= 3940 & sic <= 3989 ~ "Consumer Nondurables",
    
    sic >= 2500 & sic <= 2519 ~ "Consumer Durables",
    sic >= 2590 & sic <= 2599 ~ "Consumer Durables",
    sic >= 3630 & sic <= 3659 ~ "Consumer Durables",
    sic >= 3710 & sic <= 3711 ~ "Consumer Durables",
    sic >= 3714 & sic <= 3714 ~ "Consumer Durables",
    sic >= 3716 & sic <= 3716 ~ "Consumer Durables",
    sic >= 3750 & sic <= 3751 ~ "Consumer Durables",
    sic >= 3792 & sic <= 3792 ~ "Consumer Durables",
    sic >= 3900 & sic <= 3939 ~ "Consumer Durables",
    sic >= 3990 & sic <= 3999 ~ "Consumer Durables",
    
    sic >= 2520 & sic <= 2589 ~ "Manufacturing",
    sic >= 2600 & sic <= 2699 ~ "Manufacturing",
    sic >= 2750 & sic <= 2769 ~ "Manufacturing",
    sic >= 3000 & sic <= 3099 ~ "Manufacturing",
    sic >= 3200 & sic <= 3569 ~ "Manufacturing",
    sic >= 3580 & sic <= 3629 ~ "Manufacturing",
    sic >= 3700 & sic <= 3709 ~ "Manufacturing",
    sic >= 3712 & sic <= 3713 ~ "Manufacturing",
    sic >= 3715 & sic <= 3715 ~ "Manufacturing",
    sic >= 3717 & sic <= 3749 ~ "Manufacturing",
    sic >= 3752 & sic <= 3791 ~ "Manufacturing",
    sic >= 3793 & sic <= 3799 ~ "Manufacturing",
    sic >= 3830 & sic <= 3839 ~ "Manufacturing",
    sic >= 3860 & sic <= 3899 ~ "Manufacturing",
    
    sic >= 1200 & sic <= 1399 ~ "Energy",
    sic >= 2900 & sic <= 2999 ~ "Energy",
    
    sic >= 2800 & sic <= 2829 ~ "Chemicals",
    sic >= 2840 & sic <= 2899 ~ "Chemicals",
    
    sic >= 3570 & sic <= 3579 ~ "Business Equipment",
    sic >= 3660 & sic <= 3692 ~ "Business Equipment",
    sic >= 3694 & sic <= 3699 ~ "Business Equipment",
    sic >= 3810 & sic <= 3829 ~ "Business Equipment",
    sic >= 7370 & sic <= 7379 ~ "Business Equipment",
    
    sic >= 4800 & sic <= 4899 ~ "Telecommunications",
    
    sic >= 4900 & sic <= 4949 ~ "Utilities",
    
    sic >= 5000 & sic <= 5999 ~ "Retail",
    sic >= 7200 & sic <= 7299 ~ "Retail",
    sic >= 7600 & sic <= 7699 ~ "Retail",
    
    sic >= 2830 & sic <= 2839 ~ "Healthcare",
    sic >= 3693 & sic <= 3693 ~ "Healthcare",
    sic >= 3840 & sic <= 3859 ~ "Healthcare",
    sic >= 8000 & sic <= 8099 ~ "Healthcare",
    
    sic >= 6000 & sic <= 6999 ~ "Finance",
    
    TRUE ~ "Other"
  )
}

#' Map SIC code(s) to a Fama-French 12 industry number (1-12).
#'
#' Numeric counterpart of `assign_FF12()`. Useful for sorting / faceting
#' by FF12 in an order that matches the standard Fama-French numbering.
#'
#' @param sic Numeric SIC code, scalar or vector.
#' @return Integer vector of FF12 industry numbers (1-11), or `12` for
#'   anything outside the defined ranges (the "Other" bucket).
assign_FF12_num <- function(sic) {
  dplyr::case_when(
    sic >= 0100 & sic <= 0999 ~ 1,
    sic >= 2000 & sic <= 2399 ~ 1,
    sic >= 2700 & sic <= 2749 ~ 1,
    sic >= 2770 & sic <= 2799 ~ 1,
    sic >= 3100 & sic <= 3199 ~ 1,
    sic >= 3940 & sic <= 3989 ~ 1,
    
    sic >= 2500 & sic <= 2519 ~ 2,
    sic >= 2590 & sic <= 2599 ~ 2,
    sic >= 3630 & sic <= 3659 ~ 2,
    sic >= 3710 & sic <= 3711 ~ 2,
    sic >= 3714 & sic <= 3714 ~ 2,
    sic >= 3716 & sic <= 3716 ~ 2,
    sic >= 3750 & sic <= 3751 ~ 2,
    sic >= 3792 & sic <= 3792 ~ 2,
    sic >= 3900 & sic <= 3939 ~ 2,
    sic >= 3990 & sic <= 3999 ~ 2,
    
    sic >= 2520 & sic <= 2589 ~ 3,
    sic >= 2600 & sic <= 2699 ~ 3,
    sic >= 2750 & sic <= 2769 ~ 3,
    sic >= 3000 & sic <= 3099 ~ 3,
    sic >= 3200 & sic <= 3569 ~ 3,
    sic >= 3580 & sic <= 3629 ~ 3,
    sic >= 3700 & sic <= 3709 ~ 3,
    sic >= 3712 & sic <= 3713 ~ 3,
    sic >= 3715 & sic <= 3715 ~ 3,
    sic >= 3717 & sic <= 3749 ~ 3,
    sic >= 3752 & sic <= 3791 ~ 3,
    sic >= 3793 & sic <= 3799 ~ 3,
    sic >= 3830 & sic <= 3839 ~ 3,
    sic >= 3860 & sic <= 3899 ~ 3,
    
    sic >= 1200 & sic <= 1399 ~ 4,
    sic >= 2900 & sic <= 2999 ~ 4,
    
    sic >= 2800 & sic <= 2829 ~ 5,
    sic >= 2840 & sic <= 2899 ~ 5,
    
    sic >= 3570 & sic <= 3579 ~ 6,
    sic >= 3660 & sic <= 3692 ~ 6,
    sic >= 3694 & sic <= 3699 ~ 6,
    sic >= 3810 & sic <= 3829 ~ 6,
    sic >= 7370 & sic <= 7379 ~ 6,
    
    sic >= 4800 & sic <= 4899 ~ 7,
    
    sic >= 4900 & sic <= 4949 ~ 8,
    
    sic >= 5000 & sic <= 5999 ~ 9,
    sic >= 7200 & sic <= 7299 ~ 9,
    sic >= 7600 & sic <= 7699 ~ 9,
    
    sic >= 2830 & sic <= 2839 ~ 10,
    sic >= 3693 & sic <= 3693 ~ 10,
    sic >= 3840 & sic <= 3859 ~ 10,
    sic >= 8000 & sic <= 8099 ~ 10,
    
    sic >= 6000 & sic <= 6999 ~ 11,
    
    TRUE ~ 12
  )
}


#' Map SIC code(s) to a Fama-French 49 industry name.
#'
#' @param sic Numeric SIC code, scalar or vector.
#' @return Character vector of FF49 industry names. Anything outside the
#'   defined ranges collapses to `"Other"`.
# Note that FF49 is just FF48 plus "Other"
assign_FF49 <- function(sic) {
  dplyr::case_when(
    sic >= 0100 & sic <= 0199 ~ "Agriculture",
    sic >= 0200 & sic <= 0299 ~ "Agriculture",
    sic >= 0700 & sic <= 0799 ~ "Agriculture",
    sic >= 0910 & sic <= 0919 ~ "Agriculture",
    sic >= 2048 & sic <= 2048 ~ "Agriculture",
    
    sic >= 2000 & sic <= 2009 ~ "Food Products",
    sic >= 2010 & sic <= 2019 ~ "Food Products",
    sic >= 2020 & sic <= 2029 ~ "Food Products",
    sic >= 2030 & sic <= 2039 ~ "Food Products",
    sic >= 2040 & sic <= 2046 ~ "Food Products",
    sic >= 2050 & sic <= 2059 ~ "Food Products",
    sic >= 2060 & sic <= 2063 ~ "Food Products",
    sic >= 2070 & sic <= 2079 ~ "Food Products",
    sic >= 2090 & sic <= 2092 ~ "Food Products",
    sic >= 2095 & sic <= 2095 ~ "Food Products",
    sic >= 2098 & sic <= 2099 ~ "Food Products",
    
    (sic >= 2064 & sic <= 2068) | (sic >= 2086 & sic <= 2086) | (sic >= 2087 & sic <= 2087) | (sic >= 2096 & sic <= 2096) | (sic >= 2097 & sic <= 2097) ~ "Candy & Soda",
    (sic >= 2080 & sic <= 2080) | (sic >= 2082 & sic <= 2082) | (sic >= 2083 & sic <= 2083) | (sic >= 2084 & sic <= 2084) | (sic >= 2085 & sic <= 2085) ~ "Beer & Liquor",
    (sic >= 2100 & sic <= 2199) ~ "Tobacco Products",
    (sic >= 920 & sic <= 999) | (sic >= 3650 & sic <= 3651) | (sic >= 3652 & sic <= 3652) | (sic >= 3732 & sic <= 3732) | (sic >= 3930 & sic <= 3931) | (sic >= 3940 & sic <= 3949) ~ "Recreation",
    (sic >= 7800 & sic <= 7829) | (sic >= 7830 & sic <= 7833) | (sic >= 7840 & sic <= 7841) | (sic >= 7900 & sic <= 7900) | (sic >= 7910 & sic <= 7911) | (sic >= 7920 & sic <= 7929) | (sic >= 7930 & sic <= 7933) | (sic >= 7940 & sic <= 7949) | (sic >= 7980 & sic <= 7980) | (sic >= 7990 & sic <= 7999) ~ "Entertainment",
    (sic >= 2700 & sic <= 2709) | (sic >= 2710 & sic <= 2719) | (sic >= 2720 & sic <= 2729) | (sic >= 2730 & sic <= 2739) | (sic >= 2740 & sic <= 2749) | (sic >= 2770 & sic <= 2771) | (sic >= 2780 & sic <= 2789) | (sic >= 2790 & sic <= 2799) ~ "Printing and Publishing",
    (sic >= 2047 & sic <= 2047) | (sic >= 2391 & sic <= 2392) | (sic >= 2510 & sic <= 2519) | (sic >= 2590 & sic <= 2599) | (sic >= 2840 & sic <= 2843) | (sic >= 2844 & sic <= 2844) | (sic >= 3160 & sic <= 3161) | (sic >= 3170 & sic <= 3171) | (sic >= 3172 & sic <= 3172) | (sic >= 3190 & sic <= 3199) | (sic >= 3229 & sic <= 3229) | (sic >= 3260 & sic <= 3260) | (sic >= 3262 & sic <= 3263) | (sic >= 3269 & sic <= 3269) | (sic >= 3230 & sic <= 3231) | (sic >= 3630 & sic <= 3639) | (sic >= 3750 & sic <= 3751) | (sic >= 3800 & sic <= 3800) | (sic >= 3860 & sic <= 3861) | (sic >= 3870 & sic <= 3873) | (sic >= 3910 & sic <= 3911) | (sic >= 3914 & sic <= 3914) | (sic >= 3915 & sic <= 3915) | (sic >= 3960 & sic <= 3962) | (sic >= 3991 & sic <= 3991) | (sic >= 3995 & sic <= 3995) ~ "Consumer Goods",
    (sic >= 2300 & sic <= 2390) | (sic >= 3020 & sic <= 3021) | (sic >= 3100 & sic <= 3111) | (sic >= 3130 & sic <= 3131) | (sic >= 3140 & sic <= 3149) | (sic >= 3150 & sic <= 3151) | (sic >= 3963 & sic <= 3965) ~ "Apparel",
    (sic >= 8000 & sic <= 8099) ~ "Healthcare",
    (sic >= 3693 & sic <= 3693) | (sic >= 3840 & sic <= 3849) | (sic >= 3850 & sic <= 3851) ~ "Medical Equipment",
    (sic >= 2830 & sic <= 2830) | (sic >= 2831 & sic <= 2831) | (sic >= 2833 & sic <= 2833) | (sic >= 2834 & sic <= 2834) | (sic >= 2835 & sic <= 2835) | (sic >= 2836 & sic <= 2836) ~ "Pharmaceutical Products",
    (sic >= 2800 & sic <= 2809) | (sic >= 2810 & sic <= 2819) | (sic >= 2820 & sic <= 2829) | (sic >= 2850 & sic <= 2859) | (sic >= 2860 & sic <= 2869) | (sic >= 2870 & sic <= 2879) | (sic >= 2890 & sic <= 2899) ~ "Chemicals",
    (sic >= 3031 & sic <= 3031) | (sic >= 3041 & sic <= 3041) | (sic >= 3050 & sic <= 3053) | (sic >= 3060 & sic <= 3069) | (sic >= 3070 & sic <= 3079) | (sic >= 3080 & sic <= 3089) | (sic >= 3090 & sic <= 3099) ~ "Rubber and Plastic Products",
    (sic >= 2200 & sic <= 2269) | (sic >= 2270 & sic <= 2279) | (sic >= 2280 & sic <= 2284) | (sic >= 2290 & sic <= 2295) | (sic >= 2297 & sic <= 2297) | (sic >= 2298 & sic <= 2298) | (sic >= 2299 & sic <= 2299) | (sic >= 2393 & sic <= 2395) | (sic >= 2397 & sic <= 2399) ~ "Textiles",
    (sic >= 800 & sic <= 899) | (sic >= 2400 & sic <= 2439) | (sic >= 2450 & sic <= 2459) | (sic >= 2490 & sic <= 2499) | (sic >= 2660 & sic <= 2661) | (sic >= 2950 & sic <= 2952) | (sic >= 3200 & sic <= 3200) | (sic >= 3210 & sic <= 3211) | (sic >= 3240 & sic <= 3241) | (sic >= 3250 & sic <= 3259) | (sic >= 3261 & sic <= 3261) | (sic >= 3264 & sic <= 3264) | (sic >= 3270 & sic <= 3275) | (sic >= 3280 & sic <= 3281) | (sic >= 3290 & sic <= 3293) | (sic >= 3295 & sic <= 3299) | (sic >= 3420 & sic <= 3429) | (sic >= 3430 & sic <= 3433) | (sic >= 3440 & sic <= 3441) | (sic >= 3442 & sic <= 3442) | (sic >= 3446 & sic <= 3446) | (sic >= 3448 & sic <= 3448) | (sic >= 3449 & sic <= 3449) | (sic >= 3450 & sic <= 3451) | (sic >= 3452 & sic <= 3452) | (sic >= 3490 & sic <= 3499) | (sic >= 3996 & sic <= 3996) ~ "Construction Materials",
    (sic >= 1500 & sic <= 1511) | (sic >= 1520 & sic <= 1529) | (sic >= 1530 & sic <= 1539) | (sic >= 1540 & sic <= 1549) | (sic >= 1600 & sic <= 1699) | (sic >= 1700 & sic <= 1799) ~ "Construction",
    (sic >= 3300 & sic <= 3300) | (sic >= 3310 & sic <= 3317) | (sic >= 3320 & sic <= 3325) | (sic >= 3330 & sic <= 3339) | (sic >= 3340 & sic <= 3341) | (sic >= 3350 & sic <= 3357) | (sic >= 3360 & sic <= 3369) | (sic >= 3370 & sic <= 3379) | (sic >= 3390 & sic <= 3399) ~ "Steel Works, etc.",
    (sic >= 3400 & sic <= 3400) | (sic >= 3443 & sic <= 3443) | (sic >= 3444 & sic <= 3444) | (sic >= 3460 & sic <= 3469) | (sic >= 3470 & sic <= 3479) ~ "Fabricated Products",
    (sic >= 3510 & sic <= 3519) | (sic >= 3520 & sic <= 3529) | (sic >= 3530 & sic <= 3530) | (sic >= 3531 & sic <= 3531) | (sic >= 3532 & sic <= 3532) | (sic >= 3533 & sic <= 3533) | (sic >= 3534 & sic <= 3534) | (sic >= 3535 & sic <= 3535) | (sic >= 3536 & sic <= 3536) | (sic >= 3538 & sic <= 3538) | (sic >= 3540 & sic <= 3549) | (sic >= 3550 & sic <= 3559) | (sic >= 3560 & sic <= 3569) | (sic >= 3580 & sic <= 3580) | (sic >= 3581 & sic <= 3581) | (sic >= 3582 & sic <= 3582) | (sic >= 3585 & sic <= 3585) | (sic >= 3586 & sic <= 3586) | (sic >= 3589 & sic <= 3589) | (sic >= 3590 & sic <= 3599) ~ "Machinery",
    (sic >= 3600 & sic <= 3600) | (sic >= 3610 & sic <= 3613) | (sic >= 3620 & sic <= 3621) | (sic >= 3623 & sic <= 3629) | (sic >= 3640 & sic <= 3644) | (sic >= 3645 & sic <= 3645) | (sic >= 3646 & sic <= 3646) | (sic >= 3648 & sic <= 3649) | (sic >= 3660 & sic <= 3660) | (sic >= 3690 & sic <= 3690) | (sic >= 3691 & sic <= 3692) | (sic >= 3699 & sic <= 3699) ~ "Electrical Equipment",
    (sic >= 2296 & sic <= 2296) | (sic >= 2396 & sic <= 2396) | (sic >= 3010 & sic <= 3011) | (sic >= 3537 & sic <= 3537) | (sic >= 3647 & sic <= 3647) | (sic >= 3694 & sic <= 3694) | (sic >= 3700 & sic <= 3700) | (sic >= 3710 & sic <= 3710) | (sic >= 3711 & sic <= 3711) | (sic >= 3713 & sic <= 3713) | (sic >= 3714 & sic <= 3714) | (sic >= 3715 & sic <= 3715) | (sic >= 3716 & sic <= 3716) | (sic >= 3792 & sic <= 3792) | (sic >= 3790 & sic <= 3791) | (sic >= 3799 & sic <= 3799) ~ "Automobiles and Trucks",
    (sic >= 3720 & sic <= 3720) | (sic >= 3721 & sic <= 3721) | (sic >= 3723 & sic <= 3724) | (sic >= 3725 & sic <= 3725) | (sic >= 3728 & sic <= 3729) ~ "Aircraft",
    (sic >= 3730 & sic <= 3731) | (sic >= 3740 & sic <= 3743) ~ "Shipbuilding, Railroad Equipment",
    (sic >= 3760 & sic <= 3769) | (sic >= 3795 & sic <= 3795) | (sic >= 3480 & sic <= 3489) ~ "Defense",
    (sic >= 1040 & sic <= 1049) ~ "Precious Metals",
    (sic >= 1000 & sic <= 1009) | (sic >= 1010 & sic <= 1019) | (sic >= 1020 & sic <= 1029) | (sic >= 1030 & sic <= 1039) | (sic >= 1050 & sic <= 1059) | (sic >= 1060 & sic <= 1069) | (sic >= 1070 & sic <= 1079) | (sic >= 1080 & sic <= 1089) | (sic >= 1090 & sic <= 1099) | (sic >= 1100 & sic <= 1119) | (sic >= 1400 & sic <= 1499) ~ "Non-Metallic and Industrial Metal Mining",
    (sic >= 1200 & sic <= 1299) ~ "Coal",
    (sic >= 1300 & sic <= 1300) | (sic >= 1310 & sic <= 1319) | (sic >= 1320 & sic <= 1329) | (sic >= 1330 & sic <= 1339) | (sic >= 1370 & sic <= 1379) | (sic >= 1380 & sic <= 1380) | (sic >= 1381 & sic <= 1381) | (sic >= 1382 & sic <= 1382) | (sic >= 1389 & sic <= 1389) | (sic >= 2900 & sic <= 2912) | (sic >= 2990 & sic <= 2999) ~ "Petroleum and Natural Gas",
    (sic >= 4900 & sic <= 4900) | (sic >= 4910 & sic <= 4911) | (sic >= 4920 & sic <= 4922) | (sic >= 4923 & sic <= 4923) | (sic >= 4924 & sic <= 4925) | (sic >= 4930 & sic <= 4931) | (sic >= 4932 & sic <= 4932) | (sic >= 4939 & sic <= 4939) | (sic >= 4940 & sic <= 4942) ~ "Utilities",
    (sic >= 4800 & sic <= 4800) | (sic >= 4810 & sic <= 4813) | (sic >= 4820 & sic <= 4822) | (sic >= 4830 & sic <= 4839) | (sic >= 4840 & sic <= 4841) | (sic >= 4880 & sic <= 4889) | (sic >= 4890 & sic <= 4890) | (sic >= 4891 & sic <= 4891) | (sic >= 4892 & sic <= 4892) | (sic >= 4899 & sic <= 4899) ~ "Communication",
    (sic >= 7020 & sic <= 7021) | (sic >= 7030 & sic <= 7033) | (sic >= 7200 & sic <= 7200) | (sic >= 7210 & sic <= 7212) | (sic >= 7214 & sic <= 7214) | (sic >= 7215 & sic <= 7216) | (sic >= 7217 & sic <= 7217) | (sic >= 7219 & sic <= 7219) | (sic >= 7220 & sic <= 7221) | (sic >= 7230 & sic <= 7231) | (sic >= 7240 & sic <= 7241) | (sic >= 7250 & sic <= 7251) | (sic >= 7260 & sic <= 7269) | (sic >= 7270 & sic <= 7290) | (sic >= 7291 & sic <= 7291) | (sic >= 7292 & sic <= 7299) | (sic >= 7395 & sic <= 7395) | (sic >= 7500 & sic <= 7500) | (sic >= 7520 & sic <= 7529) | (sic >= 7530 & sic <= 7539) | (sic >= 7540 & sic <= 7549) | (sic >= 7600 & sic <= 7600) | (sic >= 7620 & sic <= 7620) | (sic >= 7622 & sic <= 7622) | (sic >= 7623 & sic <= 7623) | (sic >= 7629 & sic <= 7629) | (sic >= 7630 & sic <= 7631) | (sic >= 7640 & sic <= 7641) | (sic >= 7690 & sic <= 7699) | (sic >= 8100 & sic <= 8199) | (sic >= 8200 & sic <= 8299) | (sic >= 8300 & sic <= 8399) | (sic >= 8400 & sic <= 8499) | (sic >= 8600 & sic <= 8699) | (sic >= 8800 & sic <= 8899) | (sic >= 7510 & sic <= 7515) ~ "Personal Services",
    (sic >= 2750 & sic <= 2759) | (sic >= 3993 & sic <= 3993) | (sic >= 7218 & sic <= 7218) | (sic >= 7300 & sic <= 7300) | (sic >= 7310 & sic <= 7319) | (sic >= 7320 & sic <= 7329) | (sic >= 7330 & sic <= 7339) | (sic >= 7340 & sic <= 7342) | (sic >= 7349 & sic <= 7349) | (sic >= 7350 & sic <= 7351) | (sic >= 7352 & sic <= 7352) | (sic >= 7353 & sic <= 7353) | (sic >= 7359 & sic <= 7359) | (sic >= 7360 & sic <= 7369) | (sic >= 7374 & sic <= 7374) | (sic >= 7376 & sic <= 7376) | (sic >= 7377 & sic <= 7377) | (sic >= 7378 & sic <= 7378) | (sic >= 7379 & sic <= 7379) | (sic >= 7380 & sic <= 7380) | (sic >= 7381 & sic <= 7382) | (sic >= 7383 & sic <= 7383) | (sic >= 7384 & sic <= 7384) | (sic >= 7385 & sic <= 7385) | (sic >= 7389 & sic <= 7390) | (sic >= 7391 & sic <= 7391) | (sic >= 7392 & sic <= 7392) | (sic >= 7393 & sic <= 7393) | (sic >= 7394 & sic <= 7394) | (sic >= 7396 & sic <= 7396) | (sic >= 7397 & sic <= 7397) | (sic >= 7399 & sic <= 7399) | (sic >= 7519 & sic <= 7519) | (sic >= 8700 & sic <= 8700) | (sic >= 8710 & sic <= 8713) | (sic >= 8720 & sic <= 8721) | (sic >= 8730 & sic <= 8734) | (sic >= 8740 & sic <= 8748) | (sic >= 8900 & sic <= 8910) | (sic >= 8911 & sic <= 8911) | (sic >= 8920 & sic <= 8999) | (sic >= 4220 & sic <= 4229) ~ "Business Services",
    (sic >= 3570 & sic <= 3579) | (sic >= 3680 & sic <= 3680) | (sic >= 3681 & sic <= 3681) | (sic >= 3682 & sic <= 3682) | (sic >= 3683 & sic <= 3683) | (sic >= 3684 & sic <= 3684) | (sic >= 3685 & sic <= 3685) | (sic >= 3686 & sic <= 3686) | (sic >= 3687 & sic <= 3687) | (sic >= 3688 & sic <= 3688) | (sic >= 3689 & sic <= 3689) | (sic >= 3695 & sic <= 3695) ~ "Computer Hardware",
    (sic >= 7370 & sic <= 7372) | (sic >= 7375 & sic <= 7375) | (sic >= 7373 & sic <= 7373) ~ "Computer Software",
    (sic >= 3622 & sic <= 3622) | (sic >= 3661 & sic <= 3661) | (sic >= 3662 & sic <= 3662) | (sic >= 3663 & sic <= 3663) | (sic >= 3664 & sic <= 3664) | (sic >= 3665 & sic <= 3665) | (sic >= 3666 & sic <= 3666) | (sic >= 3669 & sic <= 3669) | (sic >= 3670 & sic <= 3679) | (sic >= 3810 & sic <= 3810) | (sic >= 3812 & sic <= 3812) ~ "Electronic Equipment",
    (sic >= 3811 & sic <= 3811) | (sic >= 3820 & sic <= 3820) | (sic >= 3821 & sic <= 3821) | (sic >= 3822 & sic <= 3822) | (sic >= 3823 & sic <= 3823) | (sic >= 3824 & sic <= 3824) | (sic >= 3825 & sic <= 3825) | (sic >= 3826 & sic <= 3826) | (sic >= 3827 & sic <= 3827) | (sic >= 3829 & sic <= 3829) | (sic >= 3830 & sic <= 3839) ~ "Measuring and Control Equipment",
    (sic >= 2520 & sic <= 2549) | (sic >= 2600 & sic <= 2639) | (sic >= 2670 & sic <= 2699) | (sic >= 2760 & sic <= 2761) | (sic >= 3950 & sic <= 3955) ~ "Business Supplies",
    (sic >= 2440 & sic <= 2449) | (sic >= 2640 & sic <= 2659) | (sic >= 3220 & sic <= 3221) | (sic >= 3410 & sic <= 3412) ~ "Shipping Containers",
    (sic >= 4000 & sic <= 4013) | (sic >= 4040 & sic <= 4049) | (sic >= 4100 & sic <= 4100) | (sic >= 4110 & sic <= 4119) | (sic >= 4120 & sic <= 4121) | (sic >= 4130 & sic <= 4131) | (sic >= 4140 & sic <= 4142) | (sic >= 4150 & sic <= 4151) | (sic >= 4170 & sic <= 4173) | (sic >= 4190 & sic <= 4199) | (sic >= 4200 & sic <= 4200) | (sic >= 4210 & sic <= 4219) | (sic >= 4230 & sic <= 4231) | (sic >= 4240 & sic <= 4249) | (sic >= 4400 & sic <= 4499) | (sic >= 4500 & sic <= 4599) | (sic >= 4600 & sic <= 4699) | (sic >= 4700 & sic <= 4700) | (sic >= 4710 & sic <= 4712) | (sic >= 4720 & sic <= 4729) | (sic >= 4730 & sic <= 4739) | (sic >= 4740 & sic <= 4749) | (sic >= 4780 & sic <= 4780) | (sic >= 4782 & sic <= 4782) | (sic >= 4783 & sic <= 4783) | (sic >= 4784 & sic <= 4784) | (sic >= 4785 & sic <= 4785) | (sic >= 4789 & sic <= 4789) ~ "Transportation",
    (sic >= 5000 & sic <= 5000) | (sic >= 5010 & sic <= 5015) | (sic >= 5020 & sic <= 5023) | (sic >= 5030 & sic <= 5039) | (sic >= 5040 & sic <= 5042) | (sic >= 5043 & sic <= 5043) | (sic >= 5044 & sic <= 5044) | (sic >= 5045 & sic <= 5045) | (sic >= 5046 & sic <= 5046) | (sic >= 5047 & sic <= 5047) | (sic >= 5048 & sic <= 5048) | (sic >= 5049 & sic <= 5049) | (sic >= 5050 & sic <= 5059) | (sic >= 5060 & sic <= 5060) | (sic >= 5063 & sic <= 5063) | (sic >= 5064 & sic <= 5064) | (sic >= 5065 & sic <= 5065) | (sic >= 5070 & sic <= 5078) | (sic >= 5080 & sic <= 5080) | (sic >= 5081 & sic <= 5081) | (sic >= 5082 & sic <= 5082) | (sic >= 5083 & sic <= 5083) | (sic >= 5084 & sic <= 5084) | (sic >= 5085 & sic <= 5085) | (sic >= 5086 & sic <= 5087) | (sic >= 5088 & sic <= 5088) | (sic >= 5090 & sic <= 5090) | (sic >= 5091 & sic <= 5092) | (sic >= 5093 & sic <= 5093) | (sic >= 5094 & sic <= 5094) | (sic >= 5099 & sic <= 5099) | (sic >= 5100 & sic <= 5100) | (sic >= 5110 & sic <= 5113) | (sic >= 5120 & sic <= 5122) | (sic >= 5130 & sic <= 5139) | (sic >= 5140 & sic <= 5149) | (sic >= 5150 & sic <= 5159) | (sic >= 5160 & sic <= 5169) | (sic >= 5170 & sic <= 5172) | (sic >= 5180 & sic <= 5182) | (sic >= 5190 & sic <= 5199) ~ "Wholesale",
    (sic >= 5200 & sic <= 5200) | (sic >= 5210 & sic <= 5219) | (sic >= 5220 & sic <= 5229) | (sic >= 5230 & sic <= 5231) | (sic >= 5250 & sic <= 5251) | (sic >= 5260 & sic <= 5261) | (sic >= 5270 & sic <= 5271) | (sic >= 5300 & sic <= 5300) | (sic >= 5310 & sic <= 5311) | (sic >= 5320 & sic <= 5320) | (sic >= 5330 & sic <= 5331) | (sic >= 5334 & sic <= 5334) | (sic >= 5340 & sic <= 5349) | (sic >= 5390 & sic <= 5399) | (sic >= 5400 & sic <= 5400) | (sic >= 5410 & sic <= 5411) | (sic >= 5412 & sic <= 5412) | (sic >= 5420 & sic <= 5429) | (sic >= 5430 & sic <= 5439) | (sic >= 5440 & sic <= 5449) | (sic >= 5450 & sic <= 5459) | (sic >= 5460 & sic <= 5469) | (sic >= 5490 & sic <= 5499) | (sic >= 5500 & sic <= 5500) | (sic >= 5510 & sic <= 5529) | (sic >= 5530 & sic <= 5539) | (sic >= 5540 & sic <= 5549) | (sic >= 5550 & sic <= 5559) | (sic >= 5560 & sic <= 5569) | (sic >= 5570 & sic <= 5579) | (sic >= 5590 & sic <= 5599) | (sic >= 5600 & sic <= 5699) | (sic >= 5700 & sic <= 5700) | (sic >= 5710 & sic <= 5719) | (sic >= 5720 & sic <= 5722) | (sic >= 5730 & sic <= 5733) | (sic >= 5734 & sic <= 5734) | (sic >= 5735 & sic <= 5735) | (sic >= 5736 & sic <= 5736) | (sic >= 5750 & sic <= 5799) | (sic >= 5900 & sic <= 5900) | (sic >= 5910 & sic <= 5912) | (sic >= 5920 & sic <= 5929) | (sic >= 5930 & sic <= 5932) | (sic >= 5940 & sic <= 5940) | (sic >= 5941 & sic <= 5941) | (sic >= 5942 & sic <= 5942) | (sic >= 5943 & sic <= 5943) | (sic >= 5944 & sic <= 5944) | (sic >= 5945 & sic <= 5945) | (sic >= 5946 & sic <= 5946) | (sic >= 5947 & sic <= 5947) | (sic >= 5948 & sic <= 5948) | (sic >= 5949 & sic <= 5949) | (sic >= 5950 & sic <= 5959) | (sic >= 5960 & sic <= 5969) | (sic >= 5970 & sic <= 5979) | (sic >= 5980 & sic <= 5989) | (sic >= 5990 & sic <= 5990) | (sic >= 5992 & sic <= 5992) | (sic >= 5993 & sic <= 5993) | (sic >= 5994 & sic <= 5994) | (sic >= 5995 & sic <= 5995) | (sic >= 5999 & sic <= 5999) ~ "Retail",
    (sic >= 5800 & sic <= 5819) | (sic >= 5820 & sic <= 5829) | (sic >= 5890 & sic <= 5899) | (sic >= 7000 & sic <= 7000) | (sic >= 7010 & sic <= 7019) | (sic >= 7040 & sic <= 7049) | (sic >= 7213 & sic <= 7213) ~ "Restaurants, Hotels, Motels",
    (sic >= 6000 & sic <= 6000) | (sic >= 6010 & sic <= 6019) | (sic >= 6020 & sic <= 6020) | (sic >= 6021 & sic <= 6021) | (sic >= 6022 & sic <= 6022) | (sic >= 6023 & sic <= 6024) | (sic >= 6025 & sic <= 6025) | (sic >= 6026 & sic <= 6026) | (sic >= 6027 & sic <= 6027) | (sic >= 6028 & sic <= 6029) | (sic >= 6030 & sic <= 6036) | (sic >= 6040 & sic <= 6059) | (sic >= 6060 & sic <= 6062) | (sic >= 6080 & sic <= 6082) | (sic >= 6090 & sic <= 6099) | (sic >= 6100 & sic <= 6100) | (sic >= 6110 & sic <= 6111) | (sic >= 6112 & sic <= 6113) | (sic >= 6120 & sic <= 6129) | (sic >= 6130 & sic <= 6139) | (sic >= 6140 & sic <= 6149) | (sic >= 6150 & sic <= 6159) | (sic >= 6160 & sic <= 6169) | (sic >= 6170 & sic <= 6179) | (sic >= 6190 & sic <= 6199) ~ "Banking",
    (sic >= 6300 & sic <= 6300) | (sic >= 6310 & sic <= 6319) | (sic >= 6320 & sic <= 6329) | (sic >= 6330 & sic <= 6331) | (sic >= 6350 & sic <= 6351) | (sic >= 6360 & sic <= 6361) | (sic >= 6370 & sic <= 6379) | (sic >= 6390 & sic <= 6399) | (sic >= 6400 & sic <= 6411) ~ "Insurance",
    (sic >= 6500 & sic <= 6500) | (sic >= 6510 & sic <= 6510) | (sic >= 6512 & sic <= 6512) | (sic >= 6513 & sic <= 6513) | (sic >= 6514 & sic <= 6514) | (sic >= 6515 & sic <= 6515) | (sic >= 6517 & sic <= 6519) | (sic >= 6520 & sic <= 6529) | (sic >= 6530 & sic <= 6531) | (sic >= 6532 & sic <= 6532) | (sic >= 6540 & sic <= 6541) | (sic >= 6550 & sic <= 6553) | (sic >= 6590 & sic <= 6599) | (sic >= 6610 & sic <= 6611) ~ "Real Estate",
    (sic >= 6200 & sic <= 6299) | (sic >= 6700 & sic <= 6700) | (sic >= 6710 & sic <= 6719) | (sic >= 6720 & sic <= 6722) | (sic >= 6723 & sic <= 6723) | (sic >= 6724 & sic <= 6724) | (sic >= 6725 & sic <= 6725) | (sic >= 6726 & sic <= 6726) | (sic >= 6730 & sic <= 6733) | (sic >= 6740 & sic <= 6779) | (sic >= 6790 & sic <= 6791) | (sic >= 6792 & sic <= 6792) | (sic >= 6793 & sic <= 6793) | (sic >= 6794 & sic <= 6794) | (sic >= 6795 & sic <= 6795) | (sic >= 6798 & sic <= 6798) | (sic >= 6799 & sic <= 6799) ~ "Trading",
    TRUE ~ "Other"
  )
}



#' Map SIC code(s) to a Fama-French 49 industry number (1-49).
#'
#' Numeric counterpart of `assign_FF49()`.
#'
#' @param sic Numeric SIC code, scalar or vector.
#' @return Integer vector of FF49 industry numbers; codes outside the
#'   defined ranges return `49` ("Almost Nothing").
assign_FF49_num <- function(sic) {
  dplyr::case_when(
    sic >= 0100 & sic <= 0199 ~ 1,
    sic >= 0200 & sic <= 0299 ~ 1,
    sic >= 0700 & sic <= 0799 ~ 1,
    sic >= 0910 & sic <= 0919 ~ 1,
    sic >= 2048 & sic <= 2048 ~ 1,
    
    sic >= 2000 & sic <= 2009 ~ 2,
    sic >= 2010 & sic <= 2019 ~ 2,
    sic >= 2020 & sic <= 2029 ~ 2,
    sic >= 2030 & sic <= 2039 ~ 2,
    sic >= 2040 & sic <= 2046 ~ 2,
    sic >= 2050 & sic <= 2059 ~ 2,
    sic >= 2060 & sic <= 2063 ~ 2,
    sic >= 2070 & sic <= 2079 ~ 2,
    sic >= 2090 & sic <= 2092 ~ 2,
    sic >= 2095 & sic <= 2095 ~ 2,
    sic >= 2098 & sic <= 2099 ~ 2,
    
    (sic >= 2064 & sic <= 2068) | (sic >= 2086 & sic <= 2086) | (sic >= 2087 & sic <= 2087) | (sic >= 2096 & sic <= 2096) | (sic >= 2097 & sic <= 2097) ~ 3,
    (sic >= 2080 & sic <= 2080) | (sic >= 2082 & sic <= 2082) | (sic >= 2083 & sic <= 2083) | (sic >= 2084 & sic <= 2084) | (sic >= 2085 & sic <= 2085) ~ 4,
    (sic >= 2100 & sic <= 2199) ~ 5,
    (sic >= 920 & sic <= 999) | (sic >= 3650 & sic <= 3651) | (sic >= 3652 & sic <= 3652) | (sic >= 3732 & sic <= 3732) | (sic >= 3930 & sic <= 3931) | (sic >= 3940 & sic <= 3949) ~ 6,
    (sic >= 7800 & sic <= 7829) | (sic >= 7830 & sic <= 7833) | (sic >= 7840 & sic <= 7841) | (sic >= 7900 & sic <= 7900) | (sic >= 7910 & sic <= 7911) | (sic >= 7920 & sic <= 7929) | (sic >= 7930 & sic <= 7933) | (sic >= 7940 & sic <= 7949) | (sic >= 7980 & sic <= 7980) | (sic >= 7990 & sic <= 7999) ~ 7,
    (sic >= 2700 & sic <= 2709) | (sic >= 2710 & sic <= 2719) | (sic >= 2720 & sic <= 2729) | (sic >= 2730 & sic <= 2739) | (sic >= 2740 & sic <= 2749) | (sic >= 2770 & sic <= 2771) | (sic >= 2780 & sic <= 2789) | (sic >= 2790 & sic <= 2799) ~ 8,
    (sic >= 2047 & sic <= 2047) | (sic >= 2391 & sic <= 2392) | (sic >= 2510 & sic <= 2519) | (sic >= 2590 & sic <= 2599) | (sic >= 2840 & sic <= 2843) | (sic >= 2844 & sic <= 2844) | (sic >= 3160 & sic <= 3161) | (sic >= 3170 & sic <= 3171) | (sic >= 3172 & sic <= 3172) | (sic >= 3190 & sic <= 3199) | (sic >= 3229 & sic <= 3229) | (sic >= 3260 & sic <= 3260) | (sic >= 3262 & sic <= 3263) | (sic >= 3269 & sic <= 3269) | (sic >= 3230 & sic <= 3231) | (sic >= 3630 & sic <= 3639) | (sic >= 3750 & sic <= 3751) | (sic >= 3800 & sic <= 3800) | (sic >= 3860 & sic <= 3861) | (sic >= 3870 & sic <= 3873) | (sic >= 3910 & sic <= 3911) | (sic >= 3914 & sic <= 3914) | (sic >= 3915 & sic <= 3915) | (sic >= 3960 & sic <= 3962) | (sic >= 3991 & sic <= 3991) | (sic >= 3995 & sic <= 3995) ~9,
    (sic >= 2300 & sic <= 2390) | (sic >= 3020 & sic <= 3021) | (sic >= 3100 & sic <= 3111) | (sic >= 3130 & sic <= 3131) | (sic >= 3140 & sic <= 3149) | (sic >= 3150 & sic <= 3151) | (sic >= 3963 & sic <= 3965) ~ 10,
    (sic >= 8000 & sic <= 8099) ~ 11,
    (sic >= 3693 & sic <= 3693) | (sic >= 3840 & sic <= 3849) | (sic >= 3850 & sic <= 3851) ~ 12,
    (sic >= 2830 & sic <= 2830) | (sic >= 2831 & sic <= 2831) | (sic >= 2833 & sic <= 2833) | (sic >= 2834 & sic <= 2834) | (sic >= 2835 & sic <= 2835) | (sic >= 2836 & sic <= 2836) ~ 13,
    (sic >= 2800 & sic <= 2809) | (sic >= 2810 & sic <= 2819) | (sic >= 2820 & sic <= 2829) | (sic >= 2850 & sic <= 2859) | (sic >= 2860 & sic <= 2869) | (sic >= 2870 & sic <= 2879) | (sic >= 2890 & sic <= 2899) ~ 14,
    (sic >= 3031 & sic <= 3031) | (sic >= 3041 & sic <= 3041) | (sic >= 3050 & sic <= 3053) | (sic >= 3060 & sic <= 3069) | (sic >= 3070 & sic <= 3079) | (sic >= 3080 & sic <= 3089) | (sic >= 3090 & sic <= 3099) ~ 15,
    (sic >= 2200 & sic <= 2269) | (sic >= 2270 & sic <= 2279) | (sic >= 2280 & sic <= 2284) | (sic >= 2290 & sic <= 2295) | (sic >= 2297 & sic <= 2297) | (sic >= 2298 & sic <= 2298) | (sic >= 2299 & sic <= 2299) | (sic >= 2393 & sic <= 2395) | (sic >= 2397 & sic <= 2399) ~ 16,
    (sic >= 800 & sic <= 899) | (sic >= 2400 & sic <= 2439) | (sic >= 2450 & sic <= 2459) | (sic >= 2490 & sic <= 2499) | (sic >= 2660 & sic <= 2661) | (sic >= 2950 & sic <= 2952) | (sic >= 3200 & sic <= 3200) | (sic >= 3210 & sic <= 3211) | (sic >= 3240 & sic <= 3241) | (sic >= 3250 & sic <= 3259) | (sic >= 3261 & sic <= 3261) | (sic >= 3264 & sic <= 3264) | (sic >= 3270 & sic <= 3275) | (sic >= 3280 & sic <= 3281) | (sic >= 3290 & sic <= 3293) | (sic >= 3295 & sic <= 3299) | (sic >= 3420 & sic <= 3429) | (sic >= 3430 & sic <= 3433) | (sic >= 3440 & sic <= 3441) | (sic >= 3442 & sic <= 3442) | (sic >= 3446 & sic <= 3446) | (sic >= 3448 & sic <= 3448) | (sic >= 3449 & sic <= 3449) | (sic >= 3450 & sic <= 3451) | (sic >= 3452 & sic <= 3452) | (sic >= 3490 & sic <= 3499) | (sic >= 3996 & sic <= 3996) ~ 17,
    (sic >= 1500 & sic <= 1511) | (sic >= 1520 & sic <= 1529) | (sic >= 1530 & sic <= 1539) | (sic >= 1540 & sic <= 1549) | (sic >= 1600 & sic <= 1699) | (sic >= 1700 & sic <= 1799) ~ 18,
    (sic >= 3300 & sic <= 3300) | (sic >= 3310 & sic <= 3317) | (sic >= 3320 & sic <= 3325) | (sic >= 3330 & sic <= 3339) | (sic >= 3340 & sic <= 3341) | (sic >= 3350 & sic <= 3357) | (sic >= 3360 & sic <= 3369) | (sic >= 3370 & sic <= 3379) | (sic >= 3390 & sic <= 3399) ~ 19,
    (sic >= 3400 & sic <= 3400) | (sic >= 3443 & sic <= 3443) | (sic >= 3444 & sic <= 3444) | (sic >= 3460 & sic <= 3469) | (sic >= 3470 & sic <= 3479) ~ 20,
    (sic >= 3510 & sic <= 3519) | (sic >= 3520 & sic <= 3529) | (sic >= 3530 & sic <= 3530) | (sic >= 3531 & sic <= 3531) | (sic >= 3532 & sic <= 3532) | (sic >= 3533 & sic <= 3533) | (sic >= 3534 & sic <= 3534) | (sic >= 3535 & sic <= 3535) | (sic >= 3536 & sic <= 3536) | (sic >= 3538 & sic <= 3538) | (sic >= 3540 & sic <= 3549) | (sic >= 3550 & sic <= 3559) | (sic >= 3560 & sic <= 3569) | (sic >= 3580 & sic <= 3580) | (sic >= 3581 & sic <= 3581) | (sic >= 3582 & sic <= 3582) | (sic >= 3585 & sic <= 3585) | (sic >= 3586 & sic <= 3586) | (sic >= 3589 & sic <= 3589) | (sic >= 3590 & sic <= 3599) ~ 21,
    (sic >= 3600 & sic <= 3600) | (sic >= 3610 & sic <= 3613) | (sic >= 3620 & sic <= 3621) | (sic >= 3623 & sic <= 3629) | (sic >= 3640 & sic <= 3644) | (sic >= 3645 & sic <= 3645) | (sic >= 3646 & sic <= 3646) | (sic >= 3648 & sic <= 3649) | (sic >= 3660 & sic <= 3660) | (sic >= 3690 & sic <= 3690) | (sic >= 3691 & sic <= 3692) | (sic >= 3699 & sic <= 3699) ~ 22,
    (sic >= 2296 & sic <= 2296) | (sic >= 2396 & sic <= 2396) | (sic >= 3010 & sic <= 3011) | (sic >= 3537 & sic <= 3537) | (sic >= 3647 & sic <= 3647) | (sic >= 3694 & sic <= 3694) | (sic >= 3700 & sic <= 3700) | (sic >= 3710 & sic <= 3710) | (sic >= 3711 & sic <= 3711) | (sic >= 3713 & sic <= 3713) | (sic >= 3714 & sic <= 3714) | (sic >= 3715 & sic <= 3715) | (sic >= 3716 & sic <= 3716) | (sic >= 3792 & sic <= 3792) | (sic >= 3790 & sic <= 3791) | (sic >= 3799 & sic <= 3799) ~ 23,
    (sic >= 3720 & sic <= 3720) | (sic >= 3721 & sic <= 3721) | (sic >= 3723 & sic <= 3724) | (sic >= 3725 & sic <= 3725) | (sic >= 3728 & sic <= 3729) ~ 24,
    (sic >= 3730 & sic <= 3731) | (sic >= 3740 & sic <= 3743) ~ 25,
    (sic >= 3760 & sic <= 3769) | (sic >= 3795 & sic <= 3795) | (sic >= 3480 & sic <= 3489) ~ 26,
    (sic >= 1040 & sic <= 1049) ~ 27,
    (sic >= 1000 & sic <= 1009) | (sic >= 1010 & sic <= 1019) | (sic >= 1020 & sic <= 1029) | (sic >= 1030 & sic <= 1039) | (sic >= 1050 & sic <= 1059) | (sic >= 1060 & sic <= 1069) | (sic >= 1070 & sic <= 1079) | (sic >= 1080 & sic <= 1089) | (sic >= 1090 & sic <= 1099) | (sic >= 1100 & sic <= 1119) | (sic >= 1400 & sic <= 1499) ~28,
    (sic >= 1200 & sic <= 1299) ~ 29,
    (sic >= 1300 & sic <= 1300) | (sic >= 1310 & sic <= 1319) | (sic >= 1320 & sic <= 1329) | (sic >= 1330 & sic <= 1339) | (sic >= 1370 & sic <= 1379) | (sic >= 1380 & sic <= 1380) | (sic >= 1381 & sic <= 1381) | (sic >= 1382 & sic <= 1382) | (sic >= 1389 & sic <= 1389) | (sic >= 2900 & sic <= 2912) | (sic >= 2990 & sic <= 2999) ~30,
    (sic >= 4900 & sic <= 4900) | (sic >= 4910 & sic <= 4911) | (sic >= 4920 & sic <= 4922) | (sic >= 4923 & sic <= 4923) | (sic >= 4924 & sic <= 4925) | (sic >= 4930 & sic <= 4931) | (sic >= 4932 & sic <= 4932) | (sic >= 4939 & sic <= 4939) | (sic >= 4940 & sic <= 4942) ~ 31,
    (sic >= 4800 & sic <= 4800) | (sic >= 4810 & sic <= 4813) | (sic >= 4820 & sic <= 4822) | (sic >= 4830 & sic <= 4839) | (sic >= 4840 & sic <= 4841) | (sic >= 4880 & sic <= 4889) | (sic >= 4890 & sic <= 4890) | (sic >= 4891 & sic <= 4891) | (sic >= 4892 & sic <= 4892) | (sic >= 4899 & sic <= 4899) ~ 32,
    (sic >= 7020 & sic <= 7021) | (sic >= 7030 & sic <= 7033) | (sic >= 7200 & sic <= 7200) | (sic >= 7210 & sic <= 7212) | (sic >= 7214 & sic <= 7214) | (sic >= 7215 & sic <= 7216) | (sic >= 7217 & sic <= 7217) | (sic >= 7219 & sic <= 7219) | (sic >= 7220 & sic <= 7221) | (sic >= 7230 & sic <= 7231) | (sic >= 7240 & sic <= 7241) | (sic >= 7250 & sic <= 7251) | (sic >= 7260 & sic <= 7269) | (sic >= 7270 & sic <= 7290) | (sic >= 7291 & sic <= 7291) | (sic >= 7292 & sic <= 7299) | (sic >= 7395 & sic <= 7395) | (sic >= 7500 & sic <= 7500) | (sic >= 7520 & sic <= 7529) | (sic >= 7530 & sic <= 7539) | (sic >= 7540 & sic <= 7549) | (sic >= 7600 & sic <= 7600) | (sic >= 7620 & sic <= 7620) | (sic >= 7622 & sic <= 7622) | (sic >= 7623 & sic <= 7623) | (sic >= 7629 & sic <= 7629) | (sic >= 7630 & sic <= 7631) | (sic >= 7640 & sic <= 7641) | (sic >= 7690 & sic <= 7699) | (sic >= 8100 & sic <= 8199) | (sic >= 8200 & sic <= 8299) | (sic >= 8300 & sic <= 8399) | (sic >= 8400 & sic <= 8499) | (sic >= 8600 & sic <= 8699) | (sic >= 8800 & sic <= 8899) | (sic >= 7510 & sic <= 7515) ~33,
    (sic >= 2750 & sic <= 2759) | (sic >= 3993 & sic <= 3993) | (sic >= 7218 & sic <= 7218) | (sic >= 7300 & sic <= 7300) | (sic >= 7310 & sic <= 7319) | (sic >= 7320 & sic <= 7329) | (sic >= 7330 & sic <= 7339) | (sic >= 7340 & sic <= 7342) | (sic >= 7349 & sic <= 7349) | (sic >= 7350 & sic <= 7351) | (sic >= 7352 & sic <= 7352) | (sic >= 7353 & sic <= 7353) | (sic >= 7359 & sic <= 7359) | (sic >= 7360 & sic <= 7369) | (sic >= 7374 & sic <= 7374) | (sic >= 7376 & sic <= 7376) | (sic >= 7377 & sic <= 7377) | (sic >= 7378 & sic <= 7378) | (sic >= 7379 & sic <= 7379) | (sic >= 7380 & sic <= 7380) | (sic >= 7381 & sic <= 7382) | (sic >= 7383 & sic <= 7383) | (sic >= 7384 & sic <= 7384) | (sic >= 7385 & sic <= 7385) | (sic >= 7389 & sic <= 7390) | (sic >= 7391 & sic <= 7391) | (sic >= 7392 & sic <= 7392) | (sic >= 7393 & sic <= 7393) | (sic >= 7394 & sic <= 7394) | (sic >= 7396 & sic <= 7396) | (sic >= 7397 & sic <= 7397) | (sic >= 7399 & sic <= 7399) | (sic >= 7519 & sic <= 7519) | (sic >= 8700 & sic <= 8700) | (sic >= 8710 & sic <= 8713) | (sic >= 8720 & sic <= 8721) | (sic >= 8730 & sic <= 8734) | (sic >= 8740 & sic <= 8748) | (sic >= 8900 & sic <= 8910) | (sic >= 8911 & sic <= 8911) | (sic >= 8920 & sic <= 8999) | (sic >= 4220 & sic <= 4229) ~ 34,
    (sic >= 3570 & sic <= 3579) | (sic >= 3680 & sic <= 3680) | (sic >= 3681 & sic <= 3681) | (sic >= 3682 & sic <= 3682) | (sic >= 3683 & sic <= 3683) | (sic >= 3684 & sic <= 3684) | (sic >= 3685 & sic <= 3685) | (sic >= 3686 & sic <= 3686) | (sic >= 3687 & sic <= 3687) | (sic >= 3688 & sic <= 3688) | (sic >= 3689 & sic <= 3689) | (sic >= 3695 & sic <= 3695) ~ 35,
    (sic >= 7370 & sic <= 7372) | (sic >= 7375 & sic <= 7375) | (sic >= 7373 & sic <= 7373) ~ 36,
    (sic >= 3622 & sic <= 3622) | (sic >= 3661 & sic <= 3661) | (sic >= 3662 & sic <= 3662) | (sic >= 3663 & sic <= 3663) | (sic >= 3664 & sic <= 3664) | (sic >= 3665 & sic <= 3665) | (sic >= 3666 & sic <= 3666) | (sic >= 3669 & sic <= 3669) | (sic >= 3670 & sic <= 3679) | (sic >= 3810 & sic <= 3810) | (sic >= 3812 & sic <= 3812) ~ 37,
    (sic >= 3811 & sic <= 3811) | (sic >= 3820 & sic <= 3820) | (sic >= 3821 & sic <= 3821) | (sic >= 3822 & sic <= 3822) | (sic >= 3823 & sic <= 3823) | (sic >= 3824 & sic <= 3824) | (sic >= 3825 & sic <= 3825) | (sic >= 3826 & sic <= 3826) | (sic >= 3827 & sic <= 3827) | (sic >= 3829 & sic <= 3829) | (sic >= 3830 & sic <= 3839) ~ 38,
    (sic >= 2520 & sic <= 2549) | (sic >= 2600 & sic <= 2639) | (sic >= 2670 & sic <= 2699) | (sic >= 2760 & sic <= 2761) | (sic >= 3950 & sic <= 3955) ~ 39,
    (sic >= 2440 & sic <= 2449) | (sic >= 2640 & sic <= 2659) | (sic >= 3220 & sic <= 3221) | (sic >= 3410 & sic <= 3412) ~ 40,
    (sic >= 4000 & sic <= 4013) | (sic >= 4040 & sic <= 4049) | (sic >= 4100 & sic <= 4100) | (sic >= 4110 & sic <= 4119) | (sic >= 4120 & sic <= 4121) | (sic >= 4130 & sic <= 4131) | (sic >= 4140 & sic <= 4142) | (sic >= 4150 & sic <= 4151) | (sic >= 4170 & sic <= 4173) | (sic >= 4190 & sic <= 4199) | (sic >= 4200 & sic <= 4200) | (sic >= 4210 & sic <= 4219) | (sic >= 4230 & sic <= 4231) | (sic >= 4240 & sic <= 4249) | (sic >= 4400 & sic <= 4499) | (sic >= 4500 & sic <= 4599) | (sic >= 4600 & sic <= 4699) | (sic >= 4700 & sic <= 4700) | (sic >= 4710 & sic <= 4712) | (sic >= 4720 & sic <= 4729) | (sic >= 4730 & sic <= 4739) | (sic >= 4740 & sic <= 4749) | (sic >= 4780 & sic <= 4780) | (sic >= 4782 & sic <= 4782) | (sic >= 4783 & sic <= 4783) | (sic >= 4784 & sic <= 4784) | (sic >= 4785 & sic <= 4785) | (sic >= 4789 & sic <= 4789) ~41,
    (sic >= 5000 & sic <= 5000) | (sic >= 5010 & sic <= 5015) | (sic >= 5020 & sic <= 5023) | (sic >= 5030 & sic <= 5039) | (sic >= 5040 & sic <= 5042) | (sic >= 5043 & sic <= 5043) | (sic >= 5044 & sic <= 5044) | (sic >= 5045 & sic <= 5045) | (sic >= 5046 & sic <= 5046) | (sic >= 5047 & sic <= 5047) | (sic >= 5048 & sic <= 5048) | (sic >= 5049 & sic <= 5049) | (sic >= 5050 & sic <= 5059) | (sic >= 5060 & sic <= 5060) | (sic >= 5063 & sic <= 5063) | (sic >= 5064 & sic <= 5064) | (sic >= 5065 & sic <= 5065) | (sic >= 5070 & sic <= 5078) | (sic >= 5080 & sic <= 5080) | (sic >= 5081 & sic <= 5081) | (sic >= 5082 & sic <= 5082) | (sic >= 5083 & sic <= 5083) | (sic >= 5084 & sic <= 5084) | (sic >= 5085 & sic <= 5085) | (sic >= 5086 & sic <= 5087) | (sic >= 5088 & sic <= 5088) | (sic >= 5090 & sic <= 5090) | (sic >= 5091 & sic <= 5092) | (sic >= 5093 & sic <= 5093) | (sic >= 5094 & sic <= 5094) | (sic >= 5099 & sic <= 5099) | (sic >= 5100 & sic <= 5100) | (sic >= 5110 & sic <= 5113) | (sic >= 5120 & sic <= 5122) | (sic >= 5130 & sic <= 5139) | (sic >= 5140 & sic <= 5149) | (sic >= 5150 & sic <= 5159) | (sic >= 5160 & sic <= 5169) | (sic >= 5170 & sic <= 5172) | (sic >= 5180 & sic <= 5182) | (sic >= 5190 & sic <= 5199) ~ 42,
    (sic >= 5200 & sic <= 5200) | (sic >= 5210 & sic <= 5219) | (sic >= 5220 & sic <= 5229) | (sic >= 5230 & sic <= 5231) | (sic >= 5250 & sic <= 5251) | (sic >= 5260 & sic <= 5261) | (sic >= 5270 & sic <= 5271) | (sic >= 5300 & sic <= 5300) | (sic >= 5310 & sic <= 5311) | (sic >= 5320 & sic <= 5320) | (sic >= 5330 & sic <= 5331) | (sic >= 5334 & sic <= 5334) | (sic >= 5340 & sic <= 5349) | (sic >= 5390 & sic <= 5399) | (sic >= 5400 & sic <= 5400) | (sic >= 5410 & sic <= 5411) | (sic >= 5412 & sic <= 5412) | (sic >= 5420 & sic <= 5429) | (sic >= 5430 & sic <= 5439) | (sic >= 5440 & sic <= 5449) | (sic >= 5450 & sic <= 5459) | (sic >= 5460 & sic <= 5469) | (sic >= 5490 & sic <= 5499) | (sic >= 5500 & sic <= 5500) | (sic >= 5510 & sic <= 5529) | (sic >= 5530 & sic <= 5539) | (sic >= 5540 & sic <= 5549) | (sic >= 5550 & sic <= 5559) | (sic >= 5560 & sic <= 5569) | (sic >= 5570 & sic <= 5579) | (sic >= 5590 & sic <= 5599) | (sic >= 5600 & sic <= 5699) | (sic >= 5700 & sic <= 5700) | (sic >= 5710 & sic <= 5719) | (sic >= 5720 & sic <= 5722) | (sic >= 5730 & sic <= 5733) | (sic >= 5734 & sic <= 5734) | (sic >= 5735 & sic <= 5735) | (sic >= 5736 & sic <= 5736) | (sic >= 5750 & sic <= 5799) | (sic >= 5900 & sic <= 5900) | (sic >= 5910 & sic <= 5912) | (sic >= 5920 & sic <= 5929) | (sic >= 5930 & sic <= 5932) | (sic >= 5940 & sic <= 5940) | (sic >= 5941 & sic <= 5941) | (sic >= 5942 & sic <= 5942) | (sic >= 5943 & sic <= 5943) | (sic >= 5944 & sic <= 5944) | (sic >= 5945 & sic <= 5945) | (sic >= 5946 & sic <= 5946) | (sic >= 5947 & sic <= 5947) | (sic >= 5948 & sic <= 5948) | (sic >= 5949 & sic <= 5949) | (sic >= 5950 & sic <= 5959) | (sic >= 5960 & sic <= 5969) | (sic >= 5970 & sic <= 5979) | (sic >= 5980 & sic <= 5989) | (sic >= 5990 & sic <= 5990) | (sic >= 5992 & sic <= 5992) | (sic >= 5993 & sic <= 5993) | (sic >= 5994 & sic <= 5994) | (sic >= 5995 & sic <= 5995) | (sic >= 5999 & sic <= 5999) ~ 43,
    (sic >= 5800 & sic <= 5819) | (sic >= 5820 & sic <= 5829) | (sic >= 5890 & sic <= 5899) | (sic >= 7000 & sic <= 7000) | (sic >= 7010 & sic <= 7019) | (sic >= 7040 & sic <= 7049) | (sic >= 7213 & sic <= 7213) ~ 44,
    (sic >= 6000 & sic <= 6000) | (sic >= 6010 & sic <= 6019) | (sic >= 6020 & sic <= 6020) | (sic >= 6021 & sic <= 6021) | (sic >= 6022 & sic <= 6022) | (sic >= 6023 & sic <= 6024) | (sic >= 6025 & sic <= 6025) | (sic >= 6026 & sic <= 6026) | (sic >= 6027 & sic <= 6027) | (sic >= 6028 & sic <= 6029) | (sic >= 6030 & sic <= 6036) | (sic >= 6040 & sic <= 6059) | (sic >= 6060 & sic <= 6062) | (sic >= 6080 & sic <= 6082) | (sic >= 6090 & sic <= 6099) | (sic >= 6100 & sic <= 6100) | (sic >= 6110 & sic <= 6111) | (sic >= 6112 & sic <= 6113) | (sic >= 6120 & sic <= 6129) | (sic >= 6130 & sic <= 6139) | (sic >= 6140 & sic <= 6149) | (sic >= 6150 & sic <= 6159) | (sic >= 6160 & sic <= 6169) | (sic >= 6170 & sic <= 6179) | (sic >= 6190 & sic <= 6199) ~ 45,
    (sic >= 6300 & sic <= 6300) | (sic >= 6310 & sic <= 6319) | (sic >= 6320 & sic <= 6329) | (sic >= 6330 & sic <= 6331) | (sic >= 6350 & sic <= 6351) | (sic >= 6360 & sic <= 6361) | (sic >= 6370 & sic <= 6379) | (sic >= 6390 & sic <= 6399) | (sic >= 6400 & sic <= 6411) ~ 46,
    (sic >= 6500 & sic <= 6500) | (sic >= 6510 & sic <= 6510) | (sic >= 6512 & sic <= 6512) | (sic >= 6513 & sic <= 6513) | (sic >= 6514 & sic <= 6514) | (sic >= 6515 & sic <= 6515) | (sic >= 6517 & sic <= 6519) | (sic >= 6520 & sic <= 6529) | (sic >= 6530 & sic <= 6531) | (sic >= 6532 & sic <= 6532) | (sic >= 6540 & sic <= 6541) | (sic >= 6550 & sic <= 6553) | (sic >= 6590 & sic <= 6599) | (sic >= 6610 & sic <= 6611) ~ 47,
    (sic >= 6200 & sic <= 6299) | (sic >= 6700 & sic <= 6700) | (sic >= 6710 & sic <= 6719) | (sic >= 6720 & sic <= 6722) | (sic >= 6723 & sic <= 6723) | (sic >= 6724 & sic <= 6724) | (sic >= 6725 & sic <= 6725) | (sic >= 6726 & sic <= 6726) | (sic >= 6730 & sic <= 6733) | (sic >= 6740 & sic <= 6779) | (sic >= 6790 & sic <= 6791) | (sic >= 6792 & sic <= 6792) | (sic >= 6793 & sic <= 6793) | (sic >= 6794 & sic <= 6794) | (sic >= 6795 & sic <= 6795) | (sic >= 6798 & sic <= 6798) | (sic >= 6799 & sic <= 6799) ~ 48,
    TRUE ~ 49
  )
}

message("imported industry functions")


