# ==============================================================================
# .Rprofile -- runs automatically every time R starts in this project.
#
# You do not need to edit or even read this file. It exists so that on the
# department server, R packages are stored on the E: drive rather than on C:,
# which is small and shared by everyone.
#
# If you are running this project on your own computer, this file quietly does
# nothing and the project works normally. You do not need an E: drive.
#
# This must run BEFORE renv activates on the last line, because renv reads
# RENV_PATHS_CACHE at activation time, not later.
# ==============================================================================

local({

  # If you have deliberately set your own cache location (see README), respect
  # it and do nothing here.
  if (nzchar(Sys.getenv("RENV_PATHS_CACHE"))) return()

  # One shared cache for everyone on the department server. The first person
  # to install a package pays the download; everyone after that gets it
  # instantly, and only one copy is stored no matter how many of us use it.
  shared_cache <- "E:/R_package_cache"

  # Use it only if it actually exists. On your own laptop it will not, so renv
  # falls back to its normal per-user cache and everything still works. We test
  # for the folder itself, not just an E: drive -- a laptop might have an E:
  # drive for something else entirely.
  if (dir.exists(shared_cache)) {
    Sys.setenv(RENV_PATHS_CACHE = shared_cache)
  }

})

source("renv/activate.R")
