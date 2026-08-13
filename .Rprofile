# ==============================================================================
# .Rprofile -- runs automatically every time R starts in this project.
#
# You do not need to edit or even read this file. It does two things:
#
#   1. On the department server, stores packages on the E: drive rather than
#      C:, which is small and shared by everyone.
#   2. Installs Windows packages as pre-built binaries rather than compiling
#      them from source, so you do not need Rtools.
#
# If you are running this project on your own computer, it quietly adapts.
#
# Both settings must happen BEFORE renv activates on the last line, because
# renv reads them at activation time, not later.
# ==============================================================================

local({

  # --- 1. Where packages are stored ------------------------------------------

  # If you have deliberately set your own cache location, respect it.
  if (!nzchar(Sys.getenv("RENV_PATHS_CACHE"))) {

    # One shared cache for everyone on the department server. The first person
    # to install a package pays the download; everyone after that gets it
    # instantly, and only one copy is stored no matter how many of us use it.
    shared_cache <- "E:/R_package_cache"

    # Use it only if it actually exists. On your own laptop it will not, so
    # renv falls back to its normal per-user cache and everything still works.
    # We test for the folder itself, not just an E: drive -- a laptop might
    # have an E: drive for something else entirely.
    if (dir.exists(shared_cache)) {
      Sys.setenv(RENV_PATHS_CACHE = shared_cache)
    }
  }


})

source("renv/activate.R")


# --- 2. Where packages are downloaded from -----------------------------------
#
# NOTE THE ORDER: this runs AFTER renv activates, deliberately. renv sets
# options(repos) from renv.lock during activation, so anything set before that
# line is silently overwritten.
#
# Plain CRAN only keeps Windows binaries for the CURRENT version of each
# package. renv.lock deliberately pins exact versions, so as soon as CRAN moves
# on, those pinned versions are available only as source -- and installing from
# source on Windows needs Rtools, which most people do not have. That failure
# would surface weeks into the semester, not on day one.
#
# Posit Public Package Manager keeps binaries for historical versions. The DATE
# in the URL pins the repository to a fixed snapshot, so the versions recorded
# in renv.lock stay installable as binaries indefinitely.
#
# Windows only: P3M does not build macOS binaries, and Mac users get working
# source installs from CRAN via the Xcode command line tools they already
# installed for Git.
local({
  if (.Platform$OS.type == "windows") {
    options(repos = c(P3M = "https://packagemanager.posit.co/cran/2026-08-13"))
  }
})
