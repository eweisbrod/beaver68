# ==============================================================================
# 000-check-setup.R
#
# Purpose:
#   Check that your computer is ready to run this project, BEFORE you spend
#   20 minutes on a download that then fails at the last step.
#
#   This script changes nothing except installing missing R packages. It is
#   safe to run as many times as you like. Run it first; run it again any
#   time something breaks and you want to know whether the problem is your
#   setup or your code.
#
# Usage:
#   Open beaver68.Rproj in RStudio, then open this file and run it
#   (Ctrl+A, then Ctrl+Enter).
#
# What it checks:
#   1. R version
#   2. Working directory (are you in the project root?)
#   3. Every R package the pipeline needs
#   4. .env file and the three data directories
#   5. WRDS credentials in your OS keyring
#   6. A live connection to the WRDS server
#   7. Free disk space
#   8. LaTeX (only needed if you write up in LaTeX rather than Word)
# ==============================================================================


# A tiny reporting helper -------------------------------------------------------

# Collects results as we go so we can print one summary at the end. `status`
# is "OK", "WARN" (you can proceed), or "FAIL" (fix before continuing).

results <- list()

report <- function(status, check, detail = "") {
  results[[length(results) + 1]] <<- list(status = status, check = check,
                                          detail = detail)
  symbol <- switch(status, OK = "  [OK]  ", WARN = "  [WARN]", FAIL = "  [FAIL]")
  cat(symbol, " ", check, sep = "")
  if (nzchar(detail)) cat(" -- ", detail, sep = "")
  cat("\n")
}

cat("\n===============================================================\n")
cat(" Beaver (1968) replication -- setup check\n")
cat("===============================================================\n\n")


# 1. R version -----------------------------------------------------------------

# The scripts use the native pipe |> and the \(x) lambda shorthand, both of
# which need R 4.1 or later. 4.0 will fail with a syntax error.

r_version <- getRversion()

if (r_version >= "4.1.0") {
  report("OK", "R version", as.character(r_version))
} else {
  report("FAIL", "R version",
         paste0(r_version, " -- need 4.1.0 or later for the |> pipe. ",
                "Update R at https://cran.r-project.org/"))
}


# 2. Working directory ---------------------------------------------------------

# Every script refers to files as "src/utils.R" and ".env" -- paths relative
# to the PROJECT ROOT. If your working directory is somewhere else, those
# paths do not resolve and you get "cannot open file 'src/utils.R'".
#
# Opening beaver68.Rproj in RStudio sets this correctly. If you opened the
# .R file directly instead, you may be in the wrong place.

if (file.exists("src/utils.R") && file.exists("README.md")) {
  report("OK", "Working directory", getwd())
} else {
  report("FAIL", "Working directory",
         paste0("Currently ", getwd(),
                " -- this is not the project root. Close RStudio and reopen ",
                "it by double-clicking beaver68.Rproj."))
}


# 3. R packages ----------------------------------------------------------------

# pacman::p_load() installs anything missing and then loads it. We call it
# here for the union of everything the five pipeline scripts need, so all
# installation happens once, up front, rather than surprising you in the
# middle of a run.

if (!require("pacman", quietly = TRUE)) install.packages("pacman")

required_packages <- c(
  # Project plumbing
  "dotenv",        # read the .env file
  "glue",          # string interpolation for file paths
  "keyring",       # WRDS credentials in the OS credential store
  "tictoc",        # timing long downloads
  "digest",        # SHA256 hashes for the provenance log

  # Data access and storage
  "DBI",           # database interface
  "RPostgres",     # PostgreSQL driver -- this is how we reach WRDS
  "dbplyr",        # write dplyr, get SQL
  "duckdb",        # in-process engine for the big joins
  "arrow",         # read and write parquet files

  # Data manipulation
  "tidyverse",     # dplyr, ggplot2, tidyr, stringr, readr, ...
  "lubridate",     # dates

  # Figures
  "scales",        # percent axis labels

  # Tables and models
  "fixest",        # fast fixed-effects regressions
  "modelsummary",  # regression tables
  "tinytable",     # the LaTeX/HTML table engine modelsummary uses
  "officer",       # build the Word .docx output
  "flextable"      # tables inside the .docx
)

cat("\nChecking R packages (installing any that are missing)...\n\n")

# `install = TRUE` is the default but stated explicitly here so it is
# obvious that this line may download packages the first time you run it.
suppressPackageStartupMessages(
  pacman::p_load(char = required_packages, install = TRUE)
)

missing <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]

if (length(missing) == 0) {
  report("OK", "R packages", paste(length(required_packages), "packages available"))
} else {
  report("FAIL", "R packages",
         paste0("could not install: ", paste(missing, collapse = ", "),
                ". Try installing them one at a time to see the error."))
}


# 4. .env file and data directories --------------------------------------------

# .env holds your three directory paths. It is created by project_setup()
# the first time you run src/001-download-data.R interactively, so it is
# perfectly normal for it to be missing right now.

if (!file.exists(".env")) {
  report("WARN", ".env file",
         paste0("not created yet -- this is expected on a fresh clone. ",
                "Run src/001-download-data.R interactively to create it."))
} else {
  dotenv::load_dot_env(".env")

  dirs <- c(RAW_DATA_DIR = Sys.getenv("RAW_DATA_DIR"),
            DATA_DIR     = Sys.getenv("DATA_DIR"),
            OUTPUT_DIR   = Sys.getenv("OUTPUT_DIR"))

  for (nm in names(dirs)) {
    path <- dirs[[nm]]
    if (!nzchar(path)) {
      report("FAIL", nm, "not set in .env")
    } else if (!dir.exists(path)) {
      report("WARN", nm, paste0(path, " does not exist yet (will be created)"))
    } else {
      # Confirm we can actually write there. A path that exists but is
      # read-only (a synced folder mid-conflict, a full disk) fails much
      # later and much more confusingly.
      test_file <- file.path(path, ".write-test")
      can_write <- tryCatch({
        writeLines("test", test_file); file.remove(test_file); TRUE
      }, error = function(e) FALSE, warning = function(w) FALSE)

      if (can_write) {
        report("OK", nm, path)
      } else {
        report("FAIL", nm, paste0(path, " exists but is not writable"))
      }
    }
  }
}


# 5. WRDS credentials ----------------------------------------------------------

# Credentials live in the operating system's credential store (Windows
# Credential Manager, macOS Keychain) rather than in any file, so they
# cannot be committed to Git by accident.

wrds_user <- tryCatch(keyring::key_get("wrds", "username"),
                      error = function(e) "")

if (nzchar(wrds_user)) {
  report("OK", "WRDS credentials", paste0("username '", wrds_user, "' in keyring"))
} else {
  report("WARN", "WRDS credentials",
         paste0("not stored yet. project_setup() will ask for them, or set ",
                "them now with: keyring::key_set('wrds', 'username')"))
}


# 6. Live WRDS connection ------------------------------------------------------

# The single most useful check here. A wrong password, an expired account,
# or a campus firewall all produce the same symptom -- script 1 dying at the
# connection step -- and it is much better to learn that now.

if (nzchar(wrds_user)) {
  cat("\nTesting the WRDS connection (this takes a few seconds)...\n\n")

  conn_ok <- tryCatch({
    wrds <- DBI::dbConnect(
      RPostgres::Postgres(),
      host     = "wrds-pgdata.wharton.upenn.edu",
      port     = 9737,
      user     = keyring::key_get("wrds", "username"),
      password = keyring::key_get("wrds", "password"),
      sslmode  = "require",
      dbname   = "wrds"
    )
    on.exit(try(DBI::dbDisconnect(wrds), silent = TRUE), add = TRUE)

    # Confirm we can not only connect but actually read the two libraries
    # this project needs. A WRDS account can authenticate yet lack a
    # subscription to Compustat or CRSP.
    DBI::dbGetQuery(wrds, "SELECT 1 FROM comp.fundq LIMIT 1")
    DBI::dbGetQuery(wrds, "SELECT 1 FROM crsp.dsf_v2 LIMIT 1")
    TRUE
  }, error = function(e) {
    attr(TRUE, "msg") <- conditionMessage(e)
    structure(FALSE, msg = conditionMessage(e))
  })

  if (isTRUE(conn_ok)) {
    report("OK", "WRDS connection", "connected; Compustat and CRSP both readable")
  } else {
    report("FAIL", "WRDS connection", attr(conn_ok, "msg"))
  }
} else {
  report("WARN", "WRDS connection", "skipped -- no credentials stored yet")
}


# 7. Disk space ----------------------------------------------------------------

# The raw CRSP daily pull back to 1970 is the space hog. Roughly 1-2 GB,
# depending on how far back FIRST_DATE goes in script 1.

free_gb <- tryCatch({
  target <- if (nzchar(Sys.getenv("RAW_DATA_DIR"))) Sys.getenv("RAW_DATA_DIR") else "."
  info <- fs_info <- NULL
  # base R has no portable free-space function; ask the OS.
  if (.Platform$OS.type == "windows") {
    drive <- substr(normalizePath(target, mustWork = FALSE), 1, 2)
    out <- system2("cmd", c("/c", "dir", shQuote(drive)), stdout = TRUE, stderr = FALSE)
    bytes_line <- grep("bytes free", out, value = TRUE)
    as.numeric(gsub("[^0-9]", "", bytes_line[length(bytes_line)])) / 1e9
  } else {
    out <- system2("df", c("-k", shQuote(target)), stdout = TRUE)
    as.numeric(strsplit(trimws(out[2]), "\\s+")[[1]][4]) * 1024 / 1e9
  }
}, error = function(e) NA_real_)

if (is.na(free_gb)) {
  report("WARN", "Disk space", "could not determine free space; you need ~2 GB")
} else if (free_gb >= 3) {
  report("OK", "Disk space", sprintf("%.1f GB free", free_gb))
} else if (free_gb >= 1.5) {
  report("WARN", "Disk space",
         sprintf("%.1f GB free -- tight. Consider setting FIRST_DATE to 2000-01-01 in script 1.",
                 free_gb))
} else {
  report("FAIL", "Disk space",
         sprintf("%.1f GB free -- not enough for the raw CRSP pull.", free_gb))
}


# 8. LaTeX (optional) ----------------------------------------------------------

# Only relevant if you write up in LaTeX. Word users can ignore a WARN here.
# Overleaf users can also ignore it -- Overleaf compiles in the cloud.

latex_bin <- Sys.which("pdflatex")

if (nzchar(latex_bin)) {
  report("OK", "LaTeX", as.character(latex_bin))
} else if (requireNamespace("tinytex", quietly = TRUE) &&
           !is.null(tinytex::tinytex_root()) &&
           nzchar(tinytex::tinytex_root())) {
  report("OK", "LaTeX", paste("TinyTeX at", tinytex::tinytex_root()))
} else {
  report("WARN", "LaTeX",
         paste0("not found. Only needed if you use writeup-template.tex. ",
                "Install with: install.packages('tinytex'); tinytex::install_tinytex(). ",
                "Word users and Overleaf users can ignore this."))
}


# Summary ----------------------------------------------------------------------

statuses <- vapply(results, \(r) r$status, character(1))
n_fail   <- sum(statuses == "FAIL")
n_warn   <- sum(statuses == "WARN")

cat("\n===============================================================\n")

if (n_fail == 0 && n_warn == 0) {
  cat(" ALL CHECKS PASSED. You are ready to run the pipeline.\n")
  cat(" Next: src/run-all.R\n")
} else if (n_fail == 0) {
  cat(sprintf(" %d warning(s), no failures.\n\n", n_warn))
  cat(" Warnings are usually fine -- a fresh clone always warns about\n")
  cat(" .env and credentials, because project_setup() has not run yet.\n\n")
  cat(" Next: open src/001-download-data.R and run it INTERACTIVELY.\n")
} else {
  cat(sprintf(" %d FAILURE(S) and %d warning(s). Fix the failures first:\n\n",
              n_fail, n_warn))
  for (r in results) {
    if (r$status == "FAIL") cat("   - ", r$check, ": ", r$detail, "\n", sep = "")
  }
  cat("\n If you are stuck, bring this output to office hours.\n")
}

cat("===============================================================\n\n")
