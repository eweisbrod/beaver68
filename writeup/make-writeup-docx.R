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
#   each generated table and figure in from output/, so a recompile
#   updates every number by itself. Word can do neither, so its users
#   paste from output/tables.docx instead. This script performs that one
#   swap, prepends the paste instructions Word users need, and hands the
#   rest to pandoc unchanged.
#
#   Every result in the .tex is written as \resulttable{stem}{name} or
#   \resultgraphic{stem}{name}. The name in the second argument is what
#   becomes the Word paste instruction, so this script needs no list of
#   its own -- adding a result to the .tex is the only step.
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

# Both macros take two brace-free arguments: the output file stem, then
# the human name. Captured as \1 and \2 below.
RE_TABLE   <- "\\\\resulttable\\{([^{}]+)\\}\\{([^{}]+)\\}"
RE_GRAPHIC <- "\\\\resultgraphic\\{([^{}]+)\\}\\{([^{}]+)\\}"


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

# Drop whole-line LaTeX comments. Necessary because the .tex documents
# its own macros in comments -- a line reading "% \resulttable{file
# stem}{human name}" matches the pattern below and would otherwise be
# counted and converted as though it were a real result. Only lines that
# START with % are removed, so a trailing % (which suppresses a newline
# and is load-bearing inside the macro definitions) is left alone.
strip_comments <- function(txt) {
  lines <- strsplit(txt, "\n", fixed = TRUE)[[1]]
  paste(lines[!grepl("^\\s*%", lines)], collapse = "\n")
}

# Count how many times a pattern matches, for the before-and-after check.
count_matches <- function(pattern, txt) {
  m <- gregexpr(pattern, txt, perl = TRUE)[[1]]
  if (length(m) == 1 && m[1] == -1) 0L else length(m)
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
tex <- strip_comments(tex)

n_tables   <- count_matches(RE_TABLE, tex)
n_graphics <- count_matches(RE_GRAPHIC, tex)
n_expected <- n_tables + n_graphics

# A .tex with no results at all means the macros were renamed or the file
# is not the one we think it is. Better to say so than to ship a Word
# template with every table silently absent.
if (n_expected == 0) {
  stop("Found no \\resulttable{} or \\resultgraphic{} calls in ", tex_path,
       ".\nEither the file is wrong or the macros were renamed -- if the",
       " latter,\nupdate RE_TABLE and RE_GRAPHIC in this script.", call. = FALSE)
}


# --- rewrite the source for Word ----------------------------------------------

converted <- tex

# Tables live in output/tables.docx, so the instruction names it.
converted <- gsub(RE_TABLE,
                  "\\\\pastebox{Paste \\2, from output/tables.docx}",
                  converted, perl = TRUE)

# Figures are pasted from the .png files, and the surrounding figure
# environment keeps its \caption, so the instruction stays short.
converted <- gsub(RE_GRAPHIC,
                  "\\\\pastebox{Paste \\2}",
                  converted, perl = TRUE)

# The macro definitions are dead weight once every call site is gone, and
# \IfFileExists is not something pandoc needs to reason about.
# (?s) so that . spans the newlines inside a multi-line definition; the
# lazy .*? then stops at the first line consisting of a bare closing brace.
converted <- gsub("(?s)\\\\newcommand\\{\\\\(resulttable|resultgraphic|missingresult)\\}.*?\\n\\}\\n",
                  "", converted, perl = TRUE)

# Define \pastebox, then add the Word-only front matter after \maketitle.
converted <- sub("\\begin{document}",
                 paste0(PASTEBOX_DEF, "\n\n\\begin{document}"),
                 converted, fixed = TRUE)
converted <- sub("\\maketitle",
                 paste0("\\maketitle\n\n", HOW_TO_USE),
                 converted, fixed = TRUE)

# Nothing may survive the swap: a leftover call would reach pandoc, which
# does not know the macro, and the result would vanish from the Word file.
leftover <- count_matches(RE_TABLE, converted) + count_matches(RE_GRAPHIC, converted)
if (leftover > 0) {
  stop(leftover, " \\resulttable/\\resultgraphic call(s) were not converted.\n",
       "An argument probably contains braces, which the patterns do not allow.",
       call. = FALSE)
}

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
leaked <- grep("\\\\(pastebox|guidance|citet|resulttable|resultgraphic)\\b",
               body, value = TRUE, perl = TRUE)
if (length(leaked) > 0) {
  stop("Unexpanded LaTeX reached the Word output. First instance:\n  ",
       substr(leaked[1], 1, 200), call. = FALSE)
}

n_paste <- count_matches("\\[ Paste ", body)
cat("Wrote ", docx_path, "\n", sep = "")
cat("  results: ", n_tables, " tables, ", n_graphics, " figures\n", sep = "")
cat("  paste placeholders: ", n_paste, " (expected ", n_expected, ")\n", sep = "")

if (n_paste != n_expected) {
  stop("Placeholder count does not match the ", n_expected,
       " results referenced by the .tex.", call. = FALSE)
}
