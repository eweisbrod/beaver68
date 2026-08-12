# ==============================================================================
# run-all.R
#
# Purpose:
#   Run the whole pipeline end to end, writing one log file per step.
#
# Outputs (to log/):
#   001-download-data.Rout
#   002-transform-data.Rout
#   003-figures.Rout
#   004-analyze-data.Rout
#   005-data-provenance.Rout
#
# Notes:
#   - RUN src/001-download-data.R INTERACTIVELY FIRST. Its project_setup()
#     call needs a live console to prompt you for paths and credentials.
#     Once .env exists, this script works unattended.
#   - Each batch_run() spawns R CMD BATCH in a child process, so a failure
#     in one step does not take down your R session -- read the .Rout to
#     see what happened.
#   - A fresh run overwrites the previous run's logs. The proc.time()
#     block at the bottom of each .Rout records how long the step took.
#   - Those .Rout files are not just for you. The JAR Data and Code
#     Sharing Policy expects a log documenting end-to-end execution, and
#     this is it.
# ==============================================================================


# Setup ------------------------------------------------------------------------

# Installed by renv at the versions pinned in renv.lock. If this errors,
# run src/000-check-setup.R.
library(dotenv)

source("src/utils.R")  # provides batch_run()

if (!file.exists(".env")) {
  stop("No .env file found.\n",
       "  Open src/001-download-data.R in RStudio and run it interactively\n",
       "  first -- project_setup() will walk you through setup. Then come\n",
       "  back and run this script.",
       call. = FALSE)
}

load_dot_env(".env")

dir.create("log", showWarnings = FALSE, recursive = TRUE)


# Run the pipeline -------------------------------------------------------------

# open = FALSE keeps RStudio from popping open an editor tab per script.

batch_run("src/001-download-data.R",
          log_path = "log/001-download-data.Rout", open = FALSE)

batch_run("src/002-transform-data.R",
          log_path = "log/002-transform-data.Rout", open = FALSE)

batch_run("src/003-figures.R",
          log_path = "log/003-figures.Rout", open = FALSE)

batch_run("src/004-analyze-data.R",
          log_path = "log/004-analyze-data.Rout", open = FALSE)

batch_run("src/005-data-provenance.R",
          log_path = "log/005-data-provenance.Rout", open = FALSE)

cat("\nPipeline complete. Logs in: log/\n")
cat("Figures and tables in:", Sys.getenv("OUTPUT_DIR"), "\n")
