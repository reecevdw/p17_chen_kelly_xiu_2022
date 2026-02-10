"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init

## Custom reporter: Print PyDoit Text in Green
# This is helpful because some tasks write to sterr and pollute the output in
# the console. I don't want to mute this output, because this can sometimes
# cause issues when, for example, LaTeX hangs on an error and requires
# presses on the keyboard before continuing. However, I want to be able
# to easily see the task lines printed by PyDoit. I want them to stand out
# from among all the other lines printed to the console.
from doit.reporter import ConsoleReporter

from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        # other config here...
        # "cleanforget": True, # Doit will forget about tasks that have been cleaned.
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}
init(autoreset=True)


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")
USER = config("USER")

## Helpers for handling Jupyter Notebook tasks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    """Clear the output of a notebook"""
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    """Move a file to a folder"""
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        command = f"mv {from_path} {to_path}"
    else:
        command = f"move {from_path} {to_path}"
    return command


def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull data from external sources"""
    yield {
        "name": "crsp_stock",
        "doc": "Pull daily CRSP stock data and Russell 1000 proxy from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_CRSP_stock.py",
        ],
        "targets": [
            DATA_DIR / "CRSP_DAILY_PAPER_UNIVERSE.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_CRSP_stock.py"],
        "clean": [],
    }

    yield {
        "name": "russell_1000",
        "doc": "Pull Russell 1000 constituents from iShares",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_russell_1000.py",
        ],
        "targets": [
            DATA_DIR / "RUSSELL_1000_CONSTITUENTS.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_russell_1000.py"],
        "clean": [],
    }

    yield {
        "name": "ravenpack_news",
        "doc": "Pull RavenPack DJ headlines (US, relevance=100, single-stock, deduplicated) from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_ravenpack.py",
        ],
        "targets": [
            DATA_DIR / "RAVENPACK_NEWS.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_ravenpack.py"],
        "clean": [],
    }

    yield {
        "name": "link_ravenpack_crsp",
        "doc": "Link RavenPack to CRSP using WRDS method (ncusip vs isin->cusip8), then merge to CRSP daily",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/link_ravenpack_crsp.py",
        ],
        "targets": [
            DATA_DIR / "raven_crsp_crosswalk.parquet",
            DATA_DIR / "RAVENPACK_NEWS_WITH_PERMNO.parquet",
            DATA_DIR / "RAVENPACK_CRSP_MERGED.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/link_ravenpack_crsp.py",
            DATA_DIR / "RAVENPACK_NEWS.parquet",
            DATA_DIR / "CRSP_DAILY_PAPER_UNIVERSE.parquet",
        ],
        "task_dep": [
            "pull:crsp_stock",
            "pull:ravenpack_news",
        ],
        "clean": [],
    }

    yield {
        "name": "exploratory_charts",
        "doc": "Generate exploratory HTML charts for CRSP, RavenPack, and merged data",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/generate_charts.py",
        ],
        "targets": [
            OUTPUT_DIR / "crsp_avg_market_cap.html",
            OUTPUT_DIR / "ravenpack_daily_volume.html",
            OUTPUT_DIR / "ravenpack_sentiment_distribution.html",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/generate_charts.py",
            DATA_DIR / "CRSP_DAILY_PAPER_UNIVERSE.parquet",
            DATA_DIR / "RAVENPACK_NEWS.parquet",
            DATA_DIR / "RAVENPACK_CRSP_MERGED.parquet",
        ],
        "task_dep": [
            "pull:crsp_stock",
            "pull:ravenpack_news",
            "pull:link_ravenpack_crsp",
        ],
        "clean": [],
    }


# def task_summary_stats():
#     """Generate summary statistics tables"""
#     file_dep = ["./src/example_table.py"]
#     file_output = [
#         "example_table.tex",
#         "pandas_to_latex_simple_table1.tex",
#     ]
#     targets = [OUTPUT_DIR / file for file in file_output]

#     return {
#         "actions": [
#             "ipython ./src/example_table.py",
#             "ipython ./src/pandas_to_latex_demo.py",
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }


# notebook_tasks = {
#     "01_example_notebook_interactive_ipynb": {
#         "path": "./src/01_example_notebook_interactive_ipynb.py",
#         "file_dep": [],
#         "targets": [],
#     },
# }


# # fmt: off
# def task_run_notebooks():
#     """Preps the notebooks for presentation format.
#     Execute notebooks if the script version of it has been changed.
#     """
#     for notebook in notebook_tasks.keys():
#         pyfile_path = Path(notebook_tasks[notebook]["path"])
#         notebook_path = pyfile_path.with_suffix(".ipynb")
#         yield {
#             "name": notebook,
#             "actions": [
#                 """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
#                 f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
#                 jupyter_execute_notebook(notebook_path),
#                 jupyter_to_html(notebook_path),
#                 mv(notebook_path, OUTPUT_DIR),
#                 """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
#             ],
#             "file_dep": [
#                 pyfile_path,
#                 *notebook_tasks[notebook]["file_dep"],
#             ],
#             "targets": [
#                 OUTPUT_DIR / f"{notebook}.html",
#                 *notebook_tasks[notebook]["targets"],
#             ],
#             "clean": True,
#         }
# # fmt: on

###############################################################
## Task below is for LaTeX compilation
###############################################################


# def task_compile_latex_docs():
#     """Compile the LaTeX documents to PDFs"""
#     file_dep = [
#         "./reports/report_example.tex",
#         "./reports/my_article_header.sty",
#         "./reports/slides_example.tex",
#         "./reports/my_beamer_header.sty",
#         "./reports/my_common_header.sty",
#         "./reports/report_simple_example.tex",
#         "./reports/slides_simple_example.tex",
#         "./src/example_plot.py",
#         "./src/example_table.py",
#     ]
#     targets = [
#         "./reports/report_example.pdf",
#         "./reports/slides_example.pdf",
#         "./reports/report_simple_example.pdf",
#         "./reports/slides_simple_example.pdf",
#     ]

#     return {
#         "actions": [
#             # My custom LaTeX templates
#             "latexmk -xelatex -halt-on-error -cd ./reports/report_example.tex",  # Compile
#             "latexmk -xelatex -halt-on-error -c -cd ./reports/report_example.tex",  # Clean
#             "latexmk -xelatex -halt-on-error -cd ./reports/slides_example.tex",  # Compile
#             "latexmk -xelatex -halt-on-error -c -cd ./reports/slides_example.tex",  # Clean
#             # Simple templates based on small adjustments to Overleaf templates
#             "latexmk -xelatex -halt-on-error -cd ./reports/report_simple_example.tex",  # Compile
#             "latexmk -xelatex -halt-on-error -c -cd ./reports/report_simple_example.tex",  # Clean
#             "latexmk -xelatex -halt-on-error -cd ./reports/slides_simple_example.tex",  # Compile
#             "latexmk -xelatex -halt-on-error -c -cd ./reports/slides_simple_example.tex",  # Clean
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }

# sphinx_targets = [
#     "./docs/index.html",
# ]


# def task_build_chartbook_site():
#     """Compile Sphinx Docs"""
#     notebook_scripts = [
#         Path(notebook_tasks[notebook]["path"]) for notebook in notebook_tasks.keys()
#     ]
#     file_dep = [
#         "./README.md",
#         "./chartbook.toml",
#         *notebook_scripts,
#     ]

#     return {
#         "actions": [
#             "chartbook build -f",
#         ],  # Use docs as build destination
#         "targets": sphinx_targets,
#         "file_dep": file_dep,
#         "task_dep": [
#             "run_notebooks",
#         ],
#         "clean": True,
#     }
