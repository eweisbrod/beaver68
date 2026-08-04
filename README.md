# Replicating Beaver (1968)

A teaching replication package for **ACCT 932**. You will re-run the research
design of

> Beaver, W. H. 1968. The information content of annual earnings announcements.
> *Journal of Accounting Research* 6: 67–92.

on modern WRDS data, and write up what you find.

Beaver asked whether earnings announcements convey new information to the
market. His evidence was that in the week of an annual earnings announcement,
both **trading volume** and the **variability of stock returns** spike relative
to non-announcement weeks. You will rebuild that test from raw data, extend it
in ways Beaver could not, and evaluate its significance with tools he did not
have.

---

## What this repository is

A five-script R pipeline. Each script does one thing, reads what the previous
one wrote, and leaves its output on disk:

| Script | What it does |
|---|---|
| `src/000-check-setup.R` | Verify your computer is ready. Changes nothing. **Run this first.** |
| `src/001-download-data.R` | Pull Compustat and CRSP tables from WRDS to parquet |
| `src/002-transform-data.R` | Build the event-window panel (±20 trading days) |
| `src/003-figures.R` | Beaver's figures, as `.pdf` and `.png` |
| `src/004-analyze-data.R` | Descriptives and regressions, as `.tex` and `.docx` |
| `src/005-data-provenance.R` | Sample identifiers + SHA256 file inventory |

`src/run-all.R` runs scripts 1–5 and writes a log per step to `log/`.

**There is no Quarto, R Markdown, or notebook here.** Code produces figures and
tables into an output folder; you write your paper separately and pull those
files in. That separation is deliberate — it is how essentially every empirical
accounting paper is actually produced, and it means your write-up format is
your choice.

---

## Getting started

### 1. Install what you need

| | Required? | Where |
|---|---|---|
| **R** ≥ 4.1 | Yes | <https://cran.r-project.org/> |
| **RStudio** | Yes | <https://posit.co/download/rstudio-desktop/> |
| **WRDS account** with Compustat + CRSP | Yes | <https://wrds-www.wharton.upenn.edu/> |
| **~2 GB free disk space** | Yes | for the raw CRSP pull |
| **LaTeX** (TinyTeX) | Only for the LaTeX write-up | `install.packages("tinytex"); tinytex::install_tinytex()` |

R must be **4.1 or later** — the scripts use the native pipe `|>` and the
`\(x)` lambda shorthand, both introduced in 4.1. On R 4.0 you get a syntax
error.

You do **not** need to install R packages by hand. Every script begins with
`pacman::p_load(...)`, which installs anything missing and then loads it. The
full list is documented under [Dependencies](#dependencies) below.

### 2. Clone and open

```bash
git clone https://github.com/eweisbrod/beaver68.git
cd beaver68
```

Open `beaver68.Rproj` in RStudio. **This matters** — every script uses paths
like `src/utils.R` that are relative to the project root, and opening the
`.Rproj` is what sets the working directory correctly. If you open a `.R` file
directly instead, you will get `cannot open file 'src/utils.R'`.

### 3. Check your setup

Open `src/000-check-setup.R` and run it (Ctrl+A, then Ctrl+Enter). It installs
the R packages and then verifies your R version, working directory, data
directories, WRDS credentials, a live WRDS connection, disk space, and LaTeX.
It changes nothing else and is safe to re-run any time.

On a fresh clone it will warn that `.env` and your WRDS credentials do not
exist yet. That is expected — the next step creates them.

Run this **before** the download, not after. It takes about a minute and
catches the problems that would otherwise surface 20 minutes into a data pull.

### 4. First-time setup

Open `src/001-download-data.R` and **run it interactively** (Ctrl+A, then
Ctrl+Enter). The `project_setup()` call near the top will prompt you once for:

- **`RAW_DATA_DIR`** — where raw WRDS pulls go. Put this **outside** the repo,
  e.g. a Dropbox folder. Data does not belong in Git.
- **`DATA_DIR`** — where derived files go. Also outside the repo.
- **`OUTPUT_DIR`** — figures and tables. `output` (inside the repo) is fine.
- **WRDS username and password** — stored in your operating system's
  credential manager via the `keyring` package, *not* in any file.

That writes a `.env` file, which is gitignored. After it exists, setup never
runs again and `src/run-all.R` works unattended.

`.example-env` shows the expected shape if you would rather write it by hand:

```
RAW_DATA_DIR=C:/Users/yourname/Dropbox/beaver68/raw
DATA_DIR=C:/Users/yourname/Dropbox/beaver68/derived
OUTPUT_DIR=output
```

Two rules for this file:

- Use **forward slashes** in paths, even on Windows. `C:\Users\...` will fail.
- Strict `KEY=VALUE` only — **no `#` comments and no blank lines**. The parser
  chokes on them.

Your WRDS password is *not* in this file and should never be put there.

### 5. Run it

Either run `src/run-all.R`, or step through the five scripts in order. The
first run takes roughly 20–25 minutes, almost all of it downloading CRSP daily
returns. Subsequent runs skip any raw file already on disk.

> **In a hurry?** Change `FIRST_DATE` at the top of `src/001-download-data.R`
> from `"1970-01-01"` to `"2000-01-01"`. The download drops to about five
> minutes. You lose the early decades in the by-decade figures.

### 6. Write it up

Two skeletons are provided in `writeup/`. Use whichever you prefer — the
pipeline produces both formats regardless.

| | Word | LaTeX |
|---|---|---|
| Skeleton | `writeup/writeup-template.docx` | `writeup/writeup-template.tex` |
| Tables | `output/tables.docx` (all tables + figures in one file, paste from it) | `\input{}` the `.tex` files directly |
| Figures | `output/*.png` | `output/*.pdf` |

**If you use Word:** run the pipeline, open `output/tables.docx` beside your
write-up, and copy the tables and figures into the placeholders. Do not retype
numbers by hand.

**If you use LaTeX:** the template already `\input{}`s every generated table
and `\includegraphics{}` every figure, so re-running the pipeline and
recompiling updates every number automatically. Compile with
`pdflatex → bibtex → pdflatex → pdflatex`. On Overleaf, upload the template,
`references.bib`, and the contents of `output/`.

The LaTeX template carries a required preamble block for the generated tables
(they use the `tabularray` package). Do not delete it.

---

## What to submit

1. Your write-up, as **PDF or Word**, uploaded to Canvas.
2. Your **code**, including whatever you modified.

The discussion questions are in both skeletons. Several ask you to change a
parameter and re-run — that is the point of the exercise, so budget time for it.

---

## Where to make changes

Everything you are likely to want to vary sits at the top of
`src/002-transform-data.R`:

```r
DAYS_BEFORE <- 20L      # event window, trading days before
DAYS_AFTER  <- 20L      # and after
ANNUAL_ONLY <- TRUE     # annual (Q4) announcements only, as in Beaver
NYSE_ONLY   <- TRUE     # NYSE-listed firms only, as in Beaver
MIN_OBS     <- 30L      # minimum return observations per window
```

The script flags every place it matches or departs from Beaver's design with a
`DESIGN CHOICE` comment. Those comments are the raw material for the discussion
questions — read them before you start writing.

Changing any of these requires re-running scripts 2–4 only. The raw download
(script 1) does not need to repeat.

### Splitting the sample by your own variable

The assignment asks you to replicate Dechow, Sloan & Zha (2014) Figure 1 —
which splits the sample **by decade** — and then to find *your own*
partitioning variable that shows variation in the earnings response.

The decade split is already wired end to end, so use it as your worked
example. To partition on something else, change these four places:

1. **`src/002-transform-data.R`**, in the panel-construction block: add your
   variable alongside `decade`. Anything you can compute from the columns
   already on the panel (`ret`, `vol`, `prc`, `shrout`, `mve`, `year`, …) or
   join in from `fundq-raw.parquet` (`saleq`, `ibq`, `atq`, …) works. Keep it
   inside the `mutate()` that runs in DuckDB.

   ```r
   mutate(my_group = case_when(<your rule> ~ "High", TRUE ~ "Low"))
   ```

2. **Same script**, in the `decade_summary` block: swap `decade` for
   `my_group` in the `group_by()`, and write the result to a new parquet
   file.

3. **`src/003-figures.R`**: copy the `fig3` block, point it at your summary,
   and change `colour = decade` to `colour = my_group`.

4. **`src/004-analyze-data.R`**: copy the by-decade regression and replace
   `event_day:decade` with `event_day:my_group`.

Two warnings from the syllabus: **firm size is off limits**, and you must get
your variable approved before you build on it.

---

## Notes on the data

**Beaver's sample cannot be reproduced exactly.** He studied 143 NYSE firms
over 1961–1965. CRSP daily data begins in mid-1962 and Compustat's announcement
date field (`rdq`) is sparse before the 1970s, so this package starts in 1970.
You are replicating his *research design*, not his *sample* — a distinction
worth being precise about in your write-up.

**Raw vs. derived.** Raw WRDS pulls land in `RAW_DATA_DIR` and are never
modified; everything computed lands in `DATA_DIR`. That split is what makes a
replication cheap: someone else can rerun scripts 2–5 against your preserved
raw inputs without touching WRDS. It is also what the *Journal of Accounting
Research* Data and Code Sharing Policy expects, which is why script 5 exists.

**CRSP "v2" column names.** WRDS migrated CRSP to a new schema. If you consult
older code or textbooks you will see `date`, `ret`, `vol`, and `exchcd`; the
current names are `dlycaldt`, `dlyret`, `dlyvol`, and `primaryexch`. The
scripts here use the current ones.

---

## Dependencies

Run `src/000-check-setup.R` and it handles all of this. This section is for
reference — when something breaks, or when you want to know what a package is
actually doing.

### System

| | Version | Why |
|---|---|---|
| R | ≥ 4.1 | native pipe `\|>` and `\(x)` lambdas |
| RStudio | any recent | sets the working directory via `beaver68.Rproj` |
| WRDS account | — | Compustat (`comp`) and CRSP (`crsp`) subscriptions |
| Disk | ~2 GB | raw CRSP daily file back to 1970 |
| LaTeX | optional | only for `writeup-template.tex` |

Verified on R 4.5.1 / Windows 11. Nothing here is platform-specific; paths use
forward slashes throughout, which R accepts on Windows.

### R packages

All installed automatically by `pacman::p_load()`. Grouped by what they do:

| Package | Used for | Scripts |
|---|---|---|
| `pacman` | installs and loads everything else | all |
| `dotenv` | reads `.env` into environment variables | all |
| `glue` | string interpolation, mostly file paths | all |
| `keyring` | WRDS credentials in the OS credential store | 000, 001 |
| `tictoc` | timing the long downloads | 001, 002 |
| `DBI` | generic database interface | 000, 001, 002 |
| `RPostgres` | PostgreSQL driver — the actual WRDS connection | 000, 001 |
| `dbplyr` | translates dplyr verbs into SQL | 001, 002 |
| `duckdb` | runs the big joins without loading data into R | 002 |
| `arrow` | reads and writes parquet files | 001–005 |
| `tidyverse` | `dplyr`, `ggplot2`, `tidyr`, `stringr`, … | 002–004 |
| `lubridate` | date handling | 002 |
| `scales` | percent-formatted axis labels | 003 |
| `fixest` | fast fixed-effects regressions (`feols`) | 004 |
| `modelsummary` | regression tables | 004 |
| `tinytable` | table engine `modelsummary` writes LaTeX with | 004 |
| `officer` | builds the Word `.docx` output | 004, writeup |
| `flextable` | tables inside the `.docx` | 004 |
| `digest` | SHA256 hashes for the provenance inventory | 005 |

### LaTeX packages

Only if you compile `writeup/writeup-template.tex`. On Overleaf these are all
preinstalled. On a local TinyTeX install, missing packages are fetched
automatically on first compile.

`geometry`, `graphicx`, `booktabs`, `caption`, `float`, `amsmath`,
`hyperref`, `natbib` — plus `tabularray`, `ulem`, and `siunitx`, which the
generated tables require. The preamble block in the template loads all of them;
do not delete it.

The bibliography uses `plainnat`, which ships with `natbib` and therefore
always works. If you want a journal house style (`chicago`, `aer`), swap
`\bibliographystyle{}` — on TinyTeX you may need
`tinytex::tlmgr_install("chicago")` first.

### External data

| Source | Tables | Pulled by |
|---|---|---|
| Compustat | `comp.fundq` | 001 |
| CRSP | `crsp.dsf_v2`, `crsp.inddlyseriesdata`, `crsp.ccmxpf_lnkhist` | 001 |

`crsp.dsf_v2` is the CRSP "v2" (CIZ) daily stock file. Older code and textbooks
use the legacy `crsp.dsf`, whose column names differ — see *Notes on the data*.

---

## Attribution

The discussion questions are adapted from Chapter 12 of

> Gow, I. D., and T. Ding. 2024. *Empirical Research in Accounting: Tools and
> Methods*. Chapman and Hall/CRC. <https://iangow.github.io/far_book/>

which presents this exercise in Quarto using the `farr` package. This version
restructures it as a plain R pipeline and adds the formal statistical tests and
the by-decade extension.

The pipeline structure follows
[eweisbrod/project-template](https://github.com/eweisbrod/project-template).

## License

[CC-BY-4.0](LICENSE). Fork and reuse with attribution.
