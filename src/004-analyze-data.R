# ==============================================================================
# 004-analyze-data.R
#
# Purpose:
#   The formal statistical tests that Beaver (1968) could not run.
#
#   This script exists because of one of the discussion questions. Beaver
#   assessed significance informally -- he counted how many firms showed
#   the predicted pattern and eyeballed the plots (pp. 77, 81-82). He had
#   good reason: in 1968 there were no packaged statistical routines and
#   computing was expensive. The question the exercise asks is "how would
#   you evaluate significance if you were writing this paper today?" and
#   this script is one answer.
#
#   The modern answer is a regression with an announcement-window
#   indicator, firm and calendar fixed effects, and standard errors
#   clustered on both firm and date. Every one of those pieces addresses
#   a specific way the informal approach could mislead you, and the
#   comments below say which.
#
# Inputs (from DATA_DIR):
#   event-panel.parquet
#   sample-selection.parquet
#
# Outputs (to OUTPUT_DIR):
#   sample-selection.tex         Sample selection steps      (LaTeX)
#   descriptives.tex             Descriptive statistics      (LaTeX)
#   main-results-returns.tex     Return-variability results  (LaTeX)
#   main-results-turnover.tex    Volume results              (LaTeX)
#   by-decade.tex                Day-0 effect by decade      (LaTeX)
#   tables.docx                  All tables + figures        (Word)
#
#   Everything is produced in BOTH LaTeX and Word from a single set of
#   fitted models. Use whichever matches how you are writing up.
# ==============================================================================


# Setup ------------------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dotenv, glue, arrow, fixest, modelsummary, tinytable,
               officer, flextable, tidyverse)

options(scipen = 999)

# Keep modelsummary from wrapping LaTeX numbers in formatting macros that
# plain LaTeX documents will not have defined.
options(modelsummary_format_numeric_latex = "plain")

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

panel <- read_parquet(glue("{data_dir}/event-panel.parquet"))
sample_selection <- read_parquet(glue("{data_dir}/sample-selection.parquet"))


# Build the regression variables ------------------------------------------------

# The dependent variables are Beaver's two measures:
#   abs_ret_mkt   absolute market-adjusted return  (return variability)
#   turn          share turnover                   (volume)
#
# The independent variables of interest are indicators for where we are
# relative to the announcement:
#   event_day     day 0 exactly
#   event_window  days -1 through +1, which catches announcements made
#                 after the close (the reaction lands on day +1) and
#                 leakage on day -1
#
# The omitted category is therefore "an ordinary non-announcement day for
# this same firm" -- which is exactly the comparison Beaver made
# informally, now made explicit.

# MEMORY NOTE. On the full 1970-2024 history this panel is ~3.4 million
# rows, and each fitted model below retains vectors as long as the data.
# Two habits keep the script inside a normal laptop's RAM:
#   1. select() down to only the columns the regressions actually use
#      before doing anything else;
#   2. rm() the full panel once regdata exists, then gc().
# Skipping these is what turns this script from "runs in two minutes" into
# "R session crashes with no error message."

regdata <- panel |>
  select(gvkey, datadate, date, relative_td, ret_mkt, turn, rel_vol,
         year, decade) |>
  mutate(abs_ret_mkt  = abs(ret_mkt),
         event_day    = as.integer(relative_td == 0L),
         event_window = as.integer(abs(relative_td) <= 1L),
         # One fixed effect per announcement: the formal version of
         # Beaver's "each firm is its own control."
         firm_event   = paste(gvkey, datadate)) |>
  filter(!is.na(abs_ret_mkt), !is.na(turn), is.finite(turn))

rm(panel)
invisible(gc())

message("Regression sample: ", format(nrow(regdata), big.mark = ","), " firm-days")


# Table 1: sample selection -----------------------------------------------------

# Every empirical paper reports how it got from "all the data" to "the
# sample I analyze." Reviewers read this table first.

ss_display <- sample_selection |>
  mutate(obs = format(obs, big.mark = ",")) |>
  rename(Step = step, Description = description, `Observations` = obs)

tt_ss <- tt(ss_display,
            caption = "Sample selection") |>
  format_tt(escape = TRUE)

save_tt(tt_ss, glue("{output_dir}/sample-selection.tex"), overwrite = TRUE)


# Table 2: descriptive statistics -----------------------------------------------

# Report the distribution of each variable, split by whether the day is an
# announcement day. This table alone almost tells the story -- if Beaver
# is right, the announcement-day column should show visibly higher values
# for both measures.

descrip <- regdata |>
  mutate(Period = if_else(event_day == 1L,
                          "Announcement day (0)",
                          "Non-announcement days")) |>
  group_by(Period) |>
  summarize(
    # NOTE ON COLUMN NAMES: avoid the pipe character. These strings end
    # up in a LaTeX table, and a bare `|` in LaTeX text mode typesets as
    # an em dash -- so "|Ret|" silently renders as "-Ret-".
    N                        = n(),
    `Mean abs. abn. return`   = mean(abs_ret_mkt, na.rm = TRUE),
    `Median abs. abn. return` = median(abs_ret_mkt, na.rm = TRUE),
    `SD of abn. return`       = sd(ret_mkt, na.rm = TRUE),
    `Mean turnover`           = mean(turn, na.rm = TRUE),
    `Median turnover`         = median(turn, na.rm = TRUE),
    `Mean relative volume`    = mean(rel_vol, na.rm = TRUE),
    .groups = "drop") |>
  mutate(across(where(is.numeric) & !N, \(x) round(x, 5)),
         N = format(N, big.mark = ","))

tt_desc <- tt(descrip,
              caption = "Descriptive statistics, announcement vs non-announcement days") |>
  format_tt(escape = TRUE)

save_tt(tt_desc, glue("{output_dir}/descriptives.tex"), overwrite = TRUE)


# Table 3: the main regression --------------------------------------------------

# WHY EACH PIECE OF THE SPECIFICATION IS THERE:
#
# Firm-event fixed effects (`firm_event`). Each announcement is compared
#   ONLY against the other 40 days in its own window. This is Beaver's
#   "each firm is its own control" logic, imposed formally. Without it, a
#   firm that is simply more volatile than average could drive the result.
#
# Calendar-date fixed effects (`date`). Absorbs market-wide shocks. If
#   many firms announce on the same day and the market happened to move
#   that day, we would otherwise credit earnings for it.
#
# Two-way clustered standard errors (firm and date). The two ways these
#   data are NOT independent. Observations on the same firm are
#   correlated over time; observations on the same calendar date are
#   correlated across firms (common shocks). Ignoring either understates
#   the standard errors, sometimes by a lot. This is the single most
#   common way an event study overstates its own significance.

# We report the two outcomes in SEPARATE tables rather than one wide one.
# Six columns overflows the text width of a standard 1-inch-margin
# article, and the two outcomes are in different units anyway, so
# stacking them in one table invites the reader to compare coefficients
# that are not comparable.

# All models are fitted through this one helper, so the estimation
# settings stay identical across every column of every table.
#
# WHY lean AND mem.clean. By default a fitted fixest object keeps several
# vectors as long as the input data -- residuals, fitted values, the
# demeaned regressors. At 3.4 million rows that is a few hundred MB PER
# MODEL, and we fit eight of them. Without these two arguments R runs out
# of memory and dies with a segmentation fault and no error message,
# which is a genuinely awful thing to debug.
#
#   lean = TRUE       drop those large components after estimation.
#                     Coefficients, clustered standard errors, N, R2 and
#                     the fixed-effect flags all survive -- everything
#                     modelsummary needs for the tables below.
#   mem.clean = TRUE  garbage-collect during estimation rather than after.
#
# The trade-off: you cannot call predict() or residuals() on a lean model.
# We do not need to. If you extend the analysis and do need them, refit
# that one model with lean = FALSE on a subset of the data.

fit <- function(formula) {
  feols(formula, data = regdata, cluster = ~ gvkey + date,
        lean = TRUE, mem.clean = TRUE)
}

ret_models <- list(
  "(1) Day 0"           = fit(abs_ret_mkt ~ event_day | firm_event),
  "(2) Window [-1, +1]" = fit(abs_ret_mkt ~ event_window | firm_event),
  "(3) Day 0, date FE"  = fit(abs_ret_mkt ~ event_day | firm_event + date)
)

turn_models <- list(
  "(1) Day 0"           = fit(turn ~ event_day | firm_event),
  "(2) Window [-1, +1]" = fit(turn ~ event_window | firm_event),
  "(3) Day 0, date FE"  = fit(turn ~ event_day | firm_event + date)
)

coef_labels <- c(
  "event_day"    = "Announcement day (0)",
  "event_window" = "Announcement window [-1, +1]"
)

# Which goodness-of-fit rows to suppress. modelsummary's defaults for a
# fixest model include several rows nobody wants in a published table
# (AIC/BIC on a fixed-effects panel are not informative). What survives is
# the observation count, R-squared, and the "FE: ..." rows that record
# which fixed effects each column includes.
GOF_OMIT <- "AIC|BIC|Log.Lik|RMSE|Std.Errors|R2 Within|R2 Pseudo|R2 Adj"

STARS <- c("*" = .1, "**" = .05, "***" = .01)

table_note <- paste("Standard errors clustered by firm and date.",
                    "The coefficient is the increase in the outcome on",
                    "announcement days relative to other days in the same",
                    "event window.")

modelsummary(ret_models,
             coef_map = coef_labels,
             stars    = STARS,
             gof_omit = GOF_OMIT,
             title    = "Return variability around annual earnings announcements",
             notes    = paste("Dependent variable: absolute market-adjusted return.",
                              table_note),
             output   = glue("{output_dir}/main-results-returns.tex"))

modelsummary(turn_models,
             coef_map = coef_labels,
             stars    = STARS,
             gof_omit = GOF_OMIT,
             title    = "Trading volume around annual earnings announcements",
             notes    = paste("Dependent variable: share turnover",
                              "(volume / shares outstanding).", table_note),
             output   = glue("{output_dir}/main-results-turnover.tex"))

# Same models, Word-flavoured, for the .docx below.
ft_ret <- modelsummary(ret_models,
                       coef_map = coef_labels, stars = STARS,
                       gof_omit = GOF_OMIT, output = "flextable")

ft_turn <- modelsummary(turn_models,
                        coef_map = coef_labels, stars = STARS,
                        gof_omit = GOF_OMIT, output = "flextable")


# Table 4: has it changed over time? --------------------------------------------

# Beaver had five years. We have five decades, so we can ask whether the
# announcement-day effect has grown or shrunk. Interacting the day-0
# indicator with decade gives a separate effect per decade in one
# regression.

m_decade_ret  <- fit(abs_ret_mkt ~ event_day:decade | firm_event)
m_decade_turn <- fit(turn ~ event_day:decade | firm_event)

decade_models <- list("Abs. abn. return" = m_decade_ret,
                      "Turnover"         = m_decade_turn)

decade_rename <- \(x) str_replace(x, "event_day:decade", "Day 0 x ")

# fmt = 4 rather than the default 3: the early-decade turnover effects are
# genuinely small (~0.0004), and at three decimals they round to "0.000",
# which reads as a zero effect when it is actually a precisely estimated
# small one.
modelsummary(decade_models,
             stars       = STARS,
             gof_omit    = GOF_OMIT,
             fmt         = 4,
             title       = "Announcement-day effect by decade",
             coef_rename = decade_rename,
             notes       = paste("Each coefficient is the announcement-day effect",
                                 "estimated within that decade. Standard errors",
                                 "clustered by firm and date."),
             output      = glue("{output_dir}/by-decade.tex"))

ft_decade <- modelsummary(decade_models,
                          stars       = STARS,
                          gof_omit    = GOF_OMIT,
                          fmt         = 4,
                          coef_rename = decade_rename,
                          output      = "flextable")


# Assemble the Word document ----------------------------------------------------

# officer builds a .docx containing every table and figure, so a student
# writing in Word has one file to copy from rather than five. The LaTeX
# users already have their .tex files and .pdf figures.

fig_png <- \(name) glue("{output_dir}/{name}.png")

doc <- read_docx() |>
  body_add_par("Beaver (1968) Replication: Tables and Figures", style = "heading 1") |>

  body_add_par("Table 1: Sample selection", style = "heading 2") |>
  body_add_flextable(autofit(flextable(ss_display))) |>
  body_add_break() |>

  body_add_par("Table 2: Descriptive statistics", style = "heading 2") |>
  body_add_flextable(autofit(flextable(descrip))) |>
  body_add_break() |>

  body_add_par("Table 3: Return variability", style = "heading 2") |>
  body_add_flextable(autofit(ft_ret)) |>
  body_add_break() |>

  body_add_par("Table 4: Trading volume (turnover)", style = "heading 2") |>
  body_add_flextable(autofit(ft_turn)) |>
  body_add_break() |>

  body_add_par("Table 5: By decade", style = "heading 2") |>
  body_add_flextable(autofit(ft_decade)) |>
  body_add_break() |>

  body_add_par("Figure 1: Trading volume", style = "heading 2") |>
  body_add_img(fig_png("fig1-volume"), width = 6, height = 3.86) |>

  body_add_par("Figure 2: Return variability", style = "heading 2") |>
  body_add_img(fig_png("fig2-return-variability"), width = 6, height = 3.86) |>

  body_add_par("Figure 3: Turnover by decade", style = "heading 2") |>
  body_add_img(fig_png("fig3-turnover-by-decade"), width = 6, height = 3.86) |>

  body_add_par("Figure 4: Return variability by decade", style = "heading 2") |>
  body_add_img(fig_png("fig4-variability-by-decade"), width = 6, height = 3.86)

print(doc, target = glue("{output_dir}/tables.docx"))


cat("\nTables written to", output_dir, "\n")
cat("  LaTeX: sample-selection.tex, descriptives.tex,\n")
cat("         main-results-returns.tex, main-results-turnover.tex, by-decade.tex\n")
cat("  Word:  tables.docx (all tables + figures in one file)\n")
cat("Next: src/005-data-provenance.R\n")
