# ==============================================================================
# make-writeup-docx.R
#
# Purpose:
#   Build writeup/writeup-template.docx from writeup/writeup-template.tex.
#
#   The .tex file is the SINGLE SOURCE for the write-up skeleton. Edit the
#   prose there, re-run this script, and the Word version follows. Never
#   edit the .docx by hand: it is a build artifact and this script
#   overwrites it.
#
#   The two documents have to differ in exactly one respect. LaTeX pulls
#   each generated table in with \input{} and each figure with
#   \includegraphics{}, so a recompile updates every number by itself.
#   Word can do neither, so its users paste from output/tables.docx
#   instead. This script performs that one swap, prepends the paste
#   instructions Word users need, and hands the rest to pandoc unchanged.
#
# Requirements:
#   pandoc >= 3, on PATH. Check with: pandoc --version
#   Nothing else -- deliberately no R package dependencies, so this runs
#   whether or not renv has been restored.
#
# Usage (from the repo root):
#   Rscript writeup/make-writeup-docx.R
#
# Inputs:
#   writeup/writeup-template.tex   the source document
#   writeup/references.bib         resolves \citet{} into author-year text
#
# Output:
#   writeup/writeup-template.docx
# ==============================================================================

options(scipen = 999)

writeup_dir <- "writeup"
tex_path    <- file.path(writeup_dir, "writeup-template.tex")
bib_path    <- file.path(writeup_dir, "references.bib")
docx_path   <- file.path(writeup_dir, "writeup-template.docx")


# --- what each generated file becomes in the Word version ---------------------

# Keys are the bare file names the pipeline writes into OUTPUT_DIR; values
# are the instruction a Word user reads in place of the table or figure.
# Adding a table or figure to the .tex means adding a line here -- the
# check below refuses to build until you do, rather than silently dropping
# it from the Word version.
PASTE_LABELS <- c(
  "sample-selection"      = "Table 1: Sample selection",
  "descriptives"          = "Table 2: Descriptive statistics",
  "main-results-returns"  = "Table 3: Return variability",
  "main-results-turnover" = "Table 4: Trading volume (turnover)",
  "by-decade"             = "Table 5: By decade",
  "fig1-volume"           = "Figure 1: Trading volume",
  "fig2-return-variability" = "Figure 2: Return variability",
  "fig3-turnover-by-decade" = "Figure 3: Turnover by decade",

  # The extension section. Unlike everything above, the student writes
  # these two files -- the pipeline does not produce them. The names are
  # a convention the template depends on; README.md tells students to
  # save their partition figure and table under exactly these names.
  "fig6-my-partition"     = "Figure 4: Your own partition",
  "my-partition"          = "Table 6: Your own partition"
)


# --- Word-only front matter ---------------------------------------------------

# The .tex explains its own workflow in a header comment, which pandoc
# drops. Word users need the equivalent, and it is genuinely specific to
# Word -- it describes pasting, which the LaTeX version never does -- so
# it lives here rather than in the shared source.
HOW_TO_USE <- paste(
  "\\section*{How to use this template}",
  "\\guidance{Run the pipeline first (src/run-all.R). It writes every",
  "table and figure into your OUTPUT\\_DIR, and bundles all of them into a",
  "single file, output/tables.docx. Open that file alongside this one and",
  "copy the tables and figures into the placeholders below. Do not retype",
  "numbers by hand -- copy them, so that re-running the pipeline and",
  "re-copying is the only way a number ever changes.}",
  "",
  "\\guidance{Delete this section before you submit.}",
  sep = "\n")

# \pastebox{} exists only in the converted copy, so it is defined here
# rather than in the .tex, which never uses it.
PASTEBOX_DEF <- "\\newcommand{\\pastebox}[1]{\\medskip\\noindent\\textbf{[ #1 ]}\\par\\medskip}"


# --- helpers ------------------------------------------------------------------

# Pull the bare names out of every \input{../output/NAME.tex} in the
# source. Returns a character vector, possibly empty.
find_inputs <- function(tex) {
  m <- gregexpr("\\\\input\\{\\.\\./output/([^}]+)\\.tex\\}", tex, perl = TRUE)
  hits <- regmatches(tex, m)[[1]]
  sub("^\\\\input\\{\\.\\./output/(.+)\\.tex\\}$", "\\1", hits)
}

# Pull the bare names out of every \includegraphics[...]{NAME.pdf}.
find_graphics <- function(tex) {
  m <- gregexpr("\\\\includegraphics(\\[[^]]*\\])?\\{([^}]+)\\.pdf\\}", tex, perl = TRUE)
  hits <- regmatches(tex, m)[[1]]
  sub("^.*\\{(.+)\\.pdf\\}$", "\\1", hits)
}

# Read a .docx back as plain text, by unzipping word/document.xml and
# stripping tags. Used only for the post-build check, so it does not need
# to be a faithful renderer -- it needs to be dependency-free.
docx_text <- function(path) {
  con <- unz(path, "word/document.xml")
  xml <- paste(readLines(con, warn = FALSE, encoding = "UTF-8"), collapse = "")
  close(con)
  xml <- gsub("</w:p>", "\n", xml, fixed = TRUE)
  gsub("<[^>]*>", "", xml)
}


# --- preflight ----------------------------------------------------------------

if (!file.exists(tex_path)) {
  stop("Cannot find ", tex_path, ". Run this from the repo root:\n",
       "  Rscript writeup/make-writeup-docx.R", call. = FALSE)
}

if (nchar(Sys.which("pandoc")) == 0) {
  stop("pandoc is not on your PATH. Install it from https://pandoc.org/install.html\n",
       "and re-open your shell. RStudio bundles a copy but does not expose it.",
       call. = FALSE)
}

tex <- paste(readLines(tex_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")

# Every generated file referenced by the .tex must have a Word label. An
# unmapped one would vanish from the .docx with no visible trace, so stop
# instead.
referenced <- c(find_inputs(tex), find_graphics(tex))
unmapped   <- setdiff(referenced, names(PASTE_LABELS))
if (length(unmapped) > 0) {
  stop("These generated files appear in ", tex_path, " but have no Word\n",
       "paste label. Add them to PASTE_LABELS in this script:\n",
       paste0("  ", unmapped, collapse = "\n"), call. = FALSE)
}

# The reverse case is harmless but worth reporting: a label nobody uses
# usually means a table was renamed or dropped in the .tex.
unused <- setdiff(names(PASTE_LABELS), referenced)
if (length(unused) > 0) {
  message("Note: PASTE_LABELS entries not referenced by the .tex: ",
          paste(unused, collapse = ", "))
}


# --- rewrite the source for Word ----------------------------------------------

converted <- tex

# Tables: the whole \input{} line becomes a paste instruction.
for (nm in find_inputs(converted)) {
  converted <- gsub(
    paste0("\\input{../output/", nm, ".tex}"),
    paste0("\\pastebox{Paste ", PASTE_LABELS[[nm]], ", from output/tables.docx}"),
    converted, fixed = TRUE)
}

# Figures: only the \includegraphics is swapped, so the surrounding
# figure environment and its \caption still reach Word.
for (nm in find_graphics(converted)) {
  converted <- gsub(
    paste0("\\\\includegraphics(\\[[^]]*\\])?\\{", nm, "\\.pdf\\}"),
    paste0("\\\\pastebox{Paste ", PASTE_LABELS[[nm]], "}"),
    converted, perl = TRUE)
}

# Define \pastebox, then add the Word-only front matter after \maketitle.
converted <- sub("\\begin{document}",
                 paste0(PASTEBOX_DEF, "\n\n\\begin{document}"),
                 converted, fixed = TRUE)
converted <- sub("\\maketitle",
                 paste0("\\maketitle\n\n", HOW_TO_USE),
                 converted, fixed = TRUE)

tmp_tex <- tempfile(fileext = ".tex")
on.exit(unlink(tmp_tex), add = TRUE)
writeLines(converted, tmp_tex, useBytes = TRUE)


# --- convert ------------------------------------------------------------------

# --citeproc plus the .bib turns \citet{beaver1968} into "Beaver (1968)"
# and builds the reference list. Without it every citation converts to an
# empty string and the questions read "the research questions of  and  ".
status <- system2("pandoc",
                  args = c(shQuote(tmp_tex),
                           "--from=latex",
                           "--citeproc",
                           paste0("--bibliography=", shQuote(bib_path)),
                           "-o", shQuote(docx_path)))

if (status != 0) {
  stop("pandoc exited with status ", status, ". The .docx was not rebuilt.",
       call. = FALSE)
}


# --- check the result ---------------------------------------------------------

body <- docx_text(docx_path)

# An unresolved citation leaves the surrounding sentence grammatically
# broken rather than erroring, so check for the text we expect instead.
if (!grepl("Beaver (1968)", body, fixed = TRUE)) {
  stop("Citations did not resolve -- 'Beaver (1968)' is absent from the\n",
       "output. Check that ", bib_path, " exists and that pandoc was built\n",
       "with citeproc support.", call. = FALSE)
}

# A literal backslash command in the body means a macro survived
# unexpanded instead of rendering.
leaked <- grep("\\\\(pastebox|guidance|citet|input)\\b", body, value = TRUE, perl = TRUE)
if (length(leaked) > 0) {
  stop("Unexpanded LaTeX reached the Word output. First instance:\n  ",
       substr(leaked[1], 1, 200), call. = FALSE)
}

n_paste <- lengths(regmatches(body, gregexpr("[ Paste ", body, fixed = TRUE)))
cat("Wrote ", docx_path, "\n", sep = "")
cat("  paste placeholders: ", n_paste, " (expected ", length(referenced), ")\n", sep = "")

if (n_paste != length(referenced)) {
  stop("Placeholder count does not match the ", length(referenced),
       " generated files referenced by the .tex.", call. = FALSE)
}
