# ==============================================================================
# 003-figures.R
#
# Purpose:
#   Reproduce Beaver's (1968) two headline figures on modern data, plus a
#   by-decade version that asks whether his findings still hold.
#
#   Beaver's paper made its argument almost entirely through PLOTS -- which
#   is unusual by modern standards and is itself one of the discussion
#   questions. The figures here are deliberately close to his in shape:
#   the x-axis is trading days relative to the announcement, and the story
#   is whether day 0 stands out from its neighbours.
#
# Based on:
#   The plots follow Chapter 12 of Gow, I. D., and T. Ding. 2024. Empirical
#   Research in Accounting: Tools and Methods. Chapman & Hall/CRC, which
#   builds the same two figures. Read the chapter alongside this file:
#   https://iangow.github.io/far_book/beaver68.html
#
# Inputs (from DATA_DIR):
#   event-summary.parquet    relative_td x year
#   decade-summary.parquet   relative_td x decade
#
# Outputs (to OUTPUT_DIR), each written as BOTH .pdf and .png:
#   fig1-volume.{pdf,png}            Beaver Fig. 1 analogue: relative volume
#   fig2-return-variability.{pdf,png} Beaver Fig. 5 analogue: return dispersion
#   fig3-turnover-by-decade.{pdf,png} Median turnover, by decade
#   fig4-variability-by-decade.{pdf,png} Return dispersion, by decade
#   fig5-observations.{pdf,png}       Sanity check: obs per relative day
#
#   Use the .pdf files if you are writing in LaTeX (vector, scales
#   cleanly). Use the .png files if you are writing in Word.
# ==============================================================================


# Setup ------------------------------------------------------------------------

# Installed by renv at the versions pinned in renv.lock. If one of these is
# missing, run src/000-check-setup.R.
library(dotenv)
library(glue)
library(arrow)
library(scales)
library(tidyverse)

options(scipen = 999)

load_dot_env(".env")
data_dir   <- Sys.getenv("DATA_DIR")
output_dir <- Sys.getenv("OUTPUT_DIR")

source("src/utils.R")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

event_summary  <- read_parquet(glue("{data_dir}/event-summary.parquet"))
decade_summary <- read_parquet(glue("{data_dir}/decade-summary.parquet"))


# A shared look for every figure ------------------------------------------------

# Defining this once means all five figures are visually consistent, and
# changing the look is a one-line edit rather than five.

theme_beaver <- theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        legend.position  = "bottom",
        legend.title     = element_blank(),
        plot.title       = element_text(face = "bold", size = 12),
        plot.caption     = element_text(colour = "grey40", hjust = 0))

#' Wrap a caption so it does not run off the edge of the figure.
#'
#' ggplot2 does NOT wrap the `caption` text -- a long one is simply
#' clipped at the panel edge, which is easy to miss until it shows up
#' truncated in your compiled paper. str_wrap() inserts the newlines.
cap <- function(x, width = 95) str_wrap(x, width = width)

#' Save a figure as both PDF (for LaTeX) and PNG (for Word).
#'
#' @param plot   A ggplot object.
#' @param name   File stem, no extension.
#' @param width,height Inches.
save_fig <- function(plot, name, width = 7, height = 4.5) {
  ggsave(glue("{output_dir}/{name}.pdf"), plot, width = width, height = height)
  ggsave(glue("{output_dir}/{name}.png"), plot, width = width, height = height,
         dpi = 300, bg = "white")
  message("wrote ", name, ".pdf and ", name, ".png")
}

# A vertical line at day 0 marks the announcement on every figure.
annc_line <- geom_vline(xintercept = 0, linetype = "dashed",
                        colour = "grey50", linewidth = 0.4)


# Pool across years for the two "Beaver" figures --------------------------------

# event_summary is one row per relative_td per year. For the headline
# figures we want a single line pooled across all years.
#
# NOTE the weighting. We cannot simply average the yearly averages --
# that would give a year with 800 announcements the same weight as one
# with 4,000. weighted.mean() with obs as the weight recovers the
# all-announcement average.

# NOTE: the weighted means must be computed BEFORE we collapse `obs`
# itself, and the result is given a different name (`n_obs`). Inside
# summarize(), later expressions see the values created by earlier ones --
# so writing `obs = sum(obs)` first would leave the weighted.mean() calls
# below trying to weight a 41-element vector by a single number.

pooled <- event_summary |>
  group_by(relative_td) |>
  summarize(mean_rel_vol = weighted.mean(mean_rel_vol, obs, na.rm = TRUE),
            mad_ret_mkt  = weighted.mean(mad_ret_mkt,  obs, na.rm = TRUE),
            sd_ret_mkt   = weighted.mean(sd_ret_mkt,   obs, na.rm = TRUE),
            n_obs        = sum(obs),
            .groups = "drop")

sample_years <- range(event_summary$year, na.rm = TRUE)
period_label <- glue("Annual earnings announcements, {sample_years[1]}-{sample_years[2]}")


# Figure 1: volume ---------------------------------------------------------------

# Beaver's Figure 1. His finding: volume in the announcement week runs
# roughly 1.5x its normal level. The horizontal reference line at 1.0 is
# "an ordinary day" -- the whole question is how far day 0 rises above it.

fig1 <- pooled |>
  ggplot(aes(x = relative_td, y = mean_rel_vol)) +
  geom_hline(yintercept = 1, colour = "grey70", linewidth = 0.4) +
  annc_line +
  geom_line(colour = "#1f4e79", linewidth = 0.7) +
  geom_point(size = 1.1, colour = "#1f4e79") +
  scale_x_continuous(breaks = seq(-20, 20, 5)) +
  labs(title = "Trading volume around annual earnings announcements",
       subtitle = period_label,
       x = "Trading days relative to earnings announcement",
       y = "Volume relative to the event's own average",
       caption = cap(paste("Replication of Beaver (1968, Fig. 1) on modern data.",
                           "Values above 1.0 indicate above-normal volume."))) +
  theme_beaver

save_fig(fig1, "fig1-volume")


# Figure 2: return variability ---------------------------------------------------

# Beaver's Figure 5. The key insight of his design: do NOT plot mean
# returns. An announcement is good news for some firms and bad news for
# others, so the mean is near zero whether or not the announcement was
# informative. Plot the DISPERSION instead -- if the announcement moves
# prices, it moves them in both directions, and dispersion spikes.
#
# We use mean absolute market-adjusted return. Beaver used a squared
# measure; the absolute version is far less sensitive to a single
# extreme observation, and one of the discussion questions asks you to
# compare the two.

fig2 <- pooled |>
  ggplot(aes(x = relative_td, y = mad_ret_mkt)) +
  annc_line +
  geom_line(colour = "#a33d3d", linewidth = 0.7) +
  geom_point(size = 1.1, colour = "#a33d3d") +
  scale_x_continuous(breaks = seq(-20, 20, 5)) +
  scale_y_continuous(labels = label_percent(accuracy = 0.1)) +
  labs(title = "Return variability around annual earnings announcements",
       subtitle = period_label,
       x = "Trading days relative to earnings announcement",
       y = "Mean absolute market-adjusted return",
       caption = cap(paste("Replication of Beaver (1968, Fig. 5) on modern data.",
                           "Market-adjusted return = firm return less the CRSP",
                           "value-weighted index return."))) +
  theme_beaver

save_fig(fig2, "fig2-return-variability")


# Figure 3: turnover by decade ---------------------------------------------------

# Beaver could only look at 1961-1965. We have five decades, which lets us
# ask a question he could not: has the informativeness of the earnings
# announcement changed over time?
#
# We switch to MEDIAN TURNOVER here rather than mean relative volume, for
# two reasons. (1) Turnover (volume / shares outstanding) is comparable
# across firms and across decades in a way that raw volume is not --
# market-wide volume has grown enormously since 1970. (2) The median is
# robust to the handful of enormous volume days that otherwise dominate
# a mean.

fig3 <- decade_summary |>
  filter(!is.na(decade)) |>
  ggplot(aes(x = relative_td, y = med_turn,
             colour = decade, group = decade)) +
  annc_line +
  geom_line(linewidth = 0.7) +
  scale_x_continuous(breaks = seq(-20, 20, 5)) +
  # accuracy = 0.01 rather than 0.1: turnover breaks land on 0.25%
  # increments, and rounding those to one decimal produces a misleading
  # axis (0.2%, 0.5%, 0.8%, 1.0%, 1.2% -- irregular-looking steps that
  # are actually evenly spaced).
  scale_y_continuous(labels = label_percent(accuracy = 0.01)) +
  scale_colour_viridis_d(option = "D", end = 0.9) +
  labs(title = "Median share turnover around annual earnings announcements",
       subtitle = "By decade",
       x = "Trading days relative to earnings announcement",
       y = "Median daily turnover",
       caption = cap(paste("Turnover replaces Beaver's relative-volume measure so that",
                       "levels are comparable across decades."))) +
  theme_beaver

save_fig(fig3, "fig3-turnover-by-decade")


# Figure 4: return variability by decade -----------------------------------------

fig4 <- decade_summary |>
  filter(!is.na(decade)) |>
  ggplot(aes(x = relative_td, y = mad_ret_mkt,
             colour = decade, group = decade)) +
  annc_line +
  geom_line(linewidth = 0.7) +
  scale_x_continuous(breaks = seq(-20, 20, 5)) +
  scale_y_continuous(labels = label_percent(accuracy = 0.1)) +
  scale_colour_viridis_d(option = "D", end = 0.9) +
  labs(title = "Return variability around annual earnings announcements",
       subtitle = "By decade",
       x = "Trading days relative to earnings announcement",
       y = "Mean absolute market-adjusted return",
       caption = cap(paste("A rising day-0 spike relative to surrounding days would",
                       "indicate announcements have become more informative."))) +
  theme_beaver

save_fig(fig4, "fig4-variability-by-decade")


# Figure 5: observation counts (a sanity check) ----------------------------------

# ALWAYS PLOT YOUR SAMPLE SIZE. If one relative day has far fewer
# observations than its neighbours, any spike you see there may be a
# data artifact rather than an economic effect. This figure is not for
# the paper -- it is for you, before you believe the other four.

fig5 <- event_summary |>
  group_by(relative_td) |>
  summarize(obs = sum(obs), .groups = "drop") |>
  ggplot(aes(x = relative_td, y = obs)) +
  annc_line +
  geom_col(fill = "grey60") +
  scale_x_continuous(breaks = seq(-20, 20, 5)) +
  scale_y_continuous(labels = label_comma()) +
  labs(title = "Observations by relative trading day",
       subtitle = "Diagnostic: a flat profile means no day is unusually thin",
       x = "Trading days relative to earnings announcement",
       y = "Firm-day observations") +
  theme_beaver

save_fig(fig5, "fig5-observations")


cat("\nFigures written to", output_dir, "\n")
cat("  .pdf versions for LaTeX, .png versions for Word\n")
cat("Next: src/004-analyze-data.R\n")
