# ==============================================================================
# make-writeup-template.R
#
# Purpose:
#   Generate writeup-template.docx -- the Word version of the write-up
#   skeleton, with the same section structure and discussion questions as
#   writeup-template.tex.
#
#   The .docx is committed to the repo, so students do NOT need to run
#   this. It lives here so the Word template can be edited in one place
#   (this script) rather than by hand-editing a binary file, and so the
#   two skeletons cannot silently drift apart.
#
# Usage:
#   Rscript writeup/make-writeup-template.R
#
# Output:
#   writeup/writeup-template.docx
# ==============================================================================

# Installed by renv at the version pinned in renv.lock. If this errors,
# run src/000-check-setup.R.
library(officer)

out_path <- file.path("writeup", "writeup-template.docx")

# --- helpers ------------------------------------------------------------------

# A discussion question: bold prompt, then an italic placeholder for the
# student's answer.
add_question <- function(doc, n, title, prompt) {
  doc |>
    body_add_par(paste0(n, ". ", title), style = "heading 2") |>
    body_add_par(prompt, style = "Normal") |>
    body_add_par("Your answer:", style = "Normal") |>
    body_add_par("", style = "Normal")
}

# A placeholder telling the student which generated file to paste in.
add_placeholder <- function(doc, what) {
  doc |>
    body_add_par(paste0("[ ", what, " ]"), style = "Normal") |>
    body_add_par("", style = "Normal")
}


# --- build --------------------------------------------------------------------

doc <- read_docx() |>

  body_add_par("Replicating Beaver (1968): The Information Content of Annual Earnings Announcements",
               style = "heading 1") |>
  body_add_par("Your Name -- ACCT 932", style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("How to use this template", style = "heading 2") |>
  body_add_par(paste(
    "Run the pipeline first (src/run-all.R). It writes every table and figure",
    "into your OUTPUT_DIR, and bundles all of them into a single file,",
    "output/tables.docx. Open that file alongside this one and copy the",
    "tables and figures you need into the placeholders below. Do not retype",
    "numbers by hand -- copy them, so that re-running the pipeline and",
    "re-copying is the only way a number ever changes."),
    style = "Normal") |>
  body_add_par(paste(
    "Delete this section before you submit."),
    style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("Abstract", style = "heading 2") |>
  body_add_par("One paragraph: what you did and what you found. Write this last.",
               style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("1. Introduction", style = "heading 1") |>
  body_add_par(paste(
    "What question did Beaver ask, why did it matter in 1968, and what does",
    "your replication do? Two or three paragraphs."), style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("2. Research design", style = "heading 1") |>
  body_add_par(paste(
    "Describe the sample, the event window, the two outcome measures, and how",
    "you handle announcements that fall on non-trading days. Be explicit about",
    "where you depart from Beaver -- the DESIGN CHOICE comments at the top of",
    "src/002-transform-data.R flag each departure."), style = "Normal") |>
  body_add_par(paste(
    "The design implemented here follows Gow and Ding (2024), Chapter 12.",
    "Cite it when you describe the event-window construction. The chapter is",
    "free online at https://iangow.github.io/far_book/beaver68.html and is",
    "worth reading before you write this section."), style = "Normal") |>
  add_placeholder("Paste Table 1: Sample selection, from output/tables.docx") |>

  body_add_par("3. Results", style = "heading 1") |>

  body_add_par("3.1 Descriptive statistics", style = "heading 2") |>
  add_placeholder("Paste Table 2: Descriptive statistics") |>
  body_add_par("Interpret the table in a paragraph.", style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("3.2 Replication of Beaver's figures", style = "heading 2") |>
  add_placeholder("Paste Figure 1: Trading volume (compare to Beaver Fig. 1)") |>
  add_placeholder("Paste Figure 2: Return variability (compare to Beaver Fig. 6)") |>
  body_add_par("Interpret the figures in a paragraph.", style = "Normal") |>
  body_add_par("", style = "Normal") |>

  body_add_par("3.3 Formal statistical tests", style = "heading 2") |>
  body_add_par(paste(
    "Beaver assessed significance informally. Explain what the regression",
    "specification does and why the fixed effects and the clustered standard",
    "errors are there."), style = "Normal") |>
  add_placeholder("Paste Table 3: Return variability") |>
  add_placeholder("Paste Table 4: Trading volume (turnover)") |>

  body_add_par("3.4 Has the effect changed over time?", style = "heading 2") |>
  add_placeholder("Paste Figure 3: Turnover by decade") |>
  add_placeholder("Paste Table 5: By decade") |>

  body_add_par("4. Discussion questions", style = "heading 1") |>
  body_add_par(paste(
    "Answer each in a paragraph or two. Graded on the quality of the",
    "reasoning, not the length."), style = "Normal") |>
  body_add_par("", style = "Normal")

doc <- doc |>
  add_question(1, "Research questions", paste(
    "How do the research questions of Beaver (1968) and Ball and Brown (1968)",
    "differ? If there is overlap, does one paper provide superior evidence, or",
    "are they just different?")) |>

  add_question(2, "Data differences", paste(
    "What differences are there in the data (sample period, frequency, number",
    "of firms) used by Beaver relative to Ball and Brown? Why do these",
    "differences exist?")) |>

  add_question(3, "Price versus volume", paste(
    "Which is the better variable for Beaver's research question -- price or",
    "volume? Is it helpful to have both?")) |>

  add_question(4, "How much information?", paste(
    "Ball and Shivakumar (2008) argue earnings announcements provide relatively",
    "little of the market's total information. If volume measures information",
    "content, what does your Figure 1 imply about the proportion of information",
    "conveyed during announcement periods? Can that be reconciled with Beaver?")) |>

  add_question(5, "Statistical significance", paste(
    "Beaver discusses significance informally (pp. 77, 81-82). Why did he use",
    "that approach? Compare it to the regression in Table 3 -- what does the",
    "formal test buy you, and what assumptions does it add?")) |>

  add_question(6, "Why plots?", paste(
    "The primary analyses in Beaver (1968) are plots. The cost of producing",
    "plots has collapsed since 1968, yet we rarely see primary analyses",
    "presented as plots today. Why?")) |>

  add_question(7, "Overgeneralization", paste(
    "After reading Bamber, Christensen and Gaver (2000), do Beaver's sample",
    "selection criteria still seem reasonable? Do your results support the",
    "generalizations later researchers made from Beaver, or do the concerns of",
    "Bamber et al. remain applicable?")) |>

  add_question(8, "Your design choices", paste(
    "Your replication makes several measurement and design choices that differ",
    "from Beaver's. Identify them. Do you expect them to materially affect the",
    "tenor of the results? Change at least one of the parameters at the top of",
    "src/002-transform-data.R, re-run, and report what happens.")) |>

  body_add_par("5. Conclusion", style = "heading 1") |>
  body_add_par("", style = "Normal") |>

  body_add_par("References", style = "heading 1") |>
  body_add_par(paste(
    "Ball, R., and P. Brown. 1968. An empirical evaluation of accounting income",
    "numbers. Journal of Accounting Research 6 (2): 159-178."), style = "Normal") |>
  body_add_par(paste(
    "Ball, R., and L. Shivakumar. 2008. How much new information is there in",
    "earnings? Journal of Accounting Research 46 (5): 975-1016."), style = "Normal") |>
  body_add_par(paste(
    "Bamber, L. S., T. E. Christensen, and K. M. Gaver. 2000. Do we really",
    "'know' what we think we know? A case study of seminal research and its",
    "subsequent overgeneralization. Accounting, Organizations and Society 25",
    "(2): 103-129."), style = "Normal") |>
  body_add_par(paste(
    "Beaver, W. H. 1968. The information content of annual earnings",
    "announcements. Journal of Accounting Research 6: 67-92."), style = "Normal") |>
  body_add_par(paste(
    "Gow, I. D., and T. Ding. 2024. Empirical Research in Accounting: Tools and",
    "Methods. Boca Raton, FL: Chapman & Hall/CRC."), style = "Normal")

print(doc, target = out_path)

cat("Wrote", out_path, "\n")
