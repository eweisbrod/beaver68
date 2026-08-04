# AGENTS.md

Context for AI coding assistants working in this repository.

## What this is

A teaching replication package for ACCT 932 (PhD seminar, University of
Kansas). Students re-run the research design of Beaver (1968), "The
Information Content of Annual Earnings Announcements," on modern WRDS data.

**The audience is first- and second-year accounting PhD students**, many of
whom are new to R and to WRDS. Code here is read far more often than it is
run. Comments explain *why* a step exists, not what the syntax does. When
editing, preserve that register — terse, clever code is a regression in this
repository even when it is correct.

## Deliberate design decisions

- **No Quarto, R Markdown, or notebooks.** Code writes figures and tables to
  `OUTPUT_DIR`; the write-up is a separate document that pulls those files in.
  Students choose Word or LaTeX. Do not reintroduce a literate-programming
  layer.
- **R only.** The upstream `project-template` ships parallel Python and Stata
  implementations; this repository deliberately does not.
- **Every table is emitted in both LaTeX (`.tex`) and Word (`.docx`)** from a
  single set of fitted models, and every figure as both `.pdf` and `.png`. If
  you add an output, add it in both formats.
- **`DESIGN CHOICE` comments** in `src/002-transform-data.R` flag each place
  the replication matches or departs from Beaver. They are referenced by the
  discussion questions in `writeup/`. Keep them in sync.

## Layout

```
src/000-check-setup.R       preflight check; installs packages, verifies
                            R version / cwd / .env / keyring / WRDS / disk /
                            LaTeX. Read-only otherwise. NOT part of run-all.
src/001-download-data.R     WRDS -> parquet (RAW_DATA_DIR)
src/002-transform-data.R    event-window panel (DATA_DIR)
src/003-figures.R           figures (OUTPUT_DIR)
src/004-analyze-data.R      tables (OUTPUT_DIR)
src/005-data-provenance.R   sample IDs + SHA256 inventory
src/run-all.R               orchestrator, logs to log/
src/utils.R                 helpers, from eweisbrod/project-template
writeup/                    Word + LaTeX skeletons
```

Paths are relative to the project root. Scripts assume the working directory
is the repo root (RStudio sets this via `beaver68.Rproj`).

## Configuration

`.env` (gitignored, created by `project_setup()` in `src/utils.R`):

```
RAW_DATA_DIR=...   raw WRDS pulls, never modified
DATA_DIR=...       derived files
OUTPUT_DIR=...     figures and tables
```

Env files are strict `KEY=VALUE` — **no `#` comments, no blank-line section
headers**. The template file is `.example-env`, not `.env.example`.

WRDS credentials live in the OS keyring (`service = "wrds"`, keys `username`
and `password`), never in `.env` or in code.

## Dependencies

The canonical package list lives in `required_packages` in
`src/000-check-setup.R` and is documented for humans in the README's
Dependencies section. **If you add a package to any script, add it in both
places.** Individual scripts declare only what they use, via
`pacman::p_load()`; 000 declares the union so installation happens once.

## Conventions

- Native pipe `|>`, not magrittr `%>%`.
- `glue()` for string interpolation, including file paths.
- `modelsummary` / `fixest` for tables and regressions.
- `options(scipen = 999)` near the top of any analysis script.
- Heavy joins run in DuckDB directly against parquet; only collapsed results
  are `collect()`ed into R. The CRSP daily file must never be loaded whole.
- WRDS data product naming: **LSEG**, not Refinitiv.

## Gotchas that have already bitten

- **CRSP v2 (CIZ) column names.** `dlycaldt`/`dlyret`/`dlyvol`/`primaryexch`,
  not `date`/`ret`/`vol`/`exchcd`. `primaryexch == "N"` is NYSE. These live on
  `dsf_v2` directly, so the old date-range join to `stocknames` is unnecessary.
- **`shrout` is in thousands; `dlyvol` is in shares.** Hence
  `turn = vol / (shrout * 1000)`. Verified against AAPL.
- **`dplyr::summarize()` evaluates sequentially.** `obs = sum(obs)` before a
  `weighted.mean(x, obs)` in the same call silently breaks the weighting.
- **A bare `|` in a LaTeX table renders as an em dash.** Never put `|Ret|` in
  a column name or model label; write "Abs. abn. return".
- **`modelsummary` output already includes `\begin{table}`.** Do not wrap
  `\input{}` of a generated table in another `table` environment — LaTeX
  errors with "Not in outer par mode."
- **`ggplot2` does not wrap `caption` text.** Long captions are silently
  clipped. Use the `cap()` helper in `src/003-figures.R`.
- **The generated tables need `tabularray`** in the LaTeX preamble. The block
  in `writeup/writeup-template.tex` is required.

## Testing a change

There is no test suite. To verify a change end to end without a 20-minute
download, temporarily set `FIRST_DATE <- "2018-01-01"` in
`src/001-download-data.R`, point `.env` at scratch directories, and run the
five scripts in order. Reset `FIRST_DATE` afterwards.
