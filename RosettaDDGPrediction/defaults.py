#!/usr/bin/env python
# -*- Mode: python; coding:utf-8 -*-
import collections
import os.path
from pathlib import Path

# Modern Pathlib replacement for pkg_resources
_BASE_DIR = Path(__file__).parent.resolve()

############################ STEPS/OPTIONS ############################

# Recognized protocols
ROSETTA_PROTOCOLS = \
    {"flexddg" : \
        {"flexddg" : {"role" : "ddg",
                      "run_by" : "rosetta",
                      "executable" : "rosetta_scripts",
                      "executable_extract" : "score_jd2"}},
    "cartddg" : \
        {"relax" : {"role" : "processing",
                    "run_by" : "rosetta",
                    "executable" : "relax"},
         "structure_selection" : {"run_by" : "python"},
         "cartesian" : {"role" : "ddg",
                        "run_by" : "rosetta",
                        "executable" : "cartesian_ddg"}},
    "cartddg2020" : \
        {"relax2020" : {"role" : "processing",
                        "run_by" : "rosetta",
                        "executable" : "rosetta_scripts"},
         "structure_selection" : {"run_by" : "python"},
         "cartesian" : {"role" : "ddg",
                        "run_by" : "rosetta",
                        "executable" : "cartesian_ddg"}}}


# Possible ways of defining various Rosetta options that
# are needed to correctly retrieve files/parameters
ROSETTA_OPTIONS = \
    collections.defaultdict(str,
        {"ddg_out" : ("-ddg:out", "-out"),
         "in_pdb_file" : ("-in:file:s", "-s"),
         "mutfile" : ("-ddg:mut_file", "-mut_file"),
         "out_prefix" : ("-out:prefix", "-prefix"),
         "out_suffix" : ("-out:suffix", "-suffix"),
         "script_vars" : ("-parser:script_vars", "-script_vars"),
         "scorefile" : ("-out:file:scorefile", "-scorefile"),
         "scf_name" : ("-score:weights", "-weights", "scfname"),
         "backrub_n_trials" : ("backrubntrials",),
         "backrub_traj_stride" : ("backrubtrajstride",),
         "ddg_db_file" : ("ddgdbfile",),
         "protocol" : ("-parser:protocol", "-protocol"),
         "resfile" : ("resfile",),
         "struct_db_file" : ("structdbfile",),
         "db_name" : ("-inout:dbms:database_name",)})


# Default name for the Rosetta crash log
ROSETTA_CRASH_LOG = "ROSETTA_CRASH.log"

########################## DIRECTORIES/FILES ##########################

# Directory containing configuration files for running the protocols
CONFIG_RUN_DIR = str(_BASE_DIR / "config_run")

# Directory containing configuration files for aggregating results
CONFIG_AGGR_DIR = str(_BASE_DIR / "config_aggregate")

# Directory containing configuration files for plotting
CONFIG_PLOT_DIR = str(_BASE_DIR / "config_plot")

# Directory containing configuration files for run settings
CONFIG_SETTINGS_DIR = str(_BASE_DIR / "config_settings")

# Directory containing RosettaScripts used in some protocols
ROSETTA_SCRIPTS_DIR = str(_BASE_DIR / "RosettaScripts")

# Default configuration file for data aggregation
CONFIG_AGGR_FILE = str(_BASE_DIR / "config_aggregate" / "aggregate.yaml")

############################## MUTATIONS ##############################

MUT = "_mut_"
STRUCT = "_struct_"
WTR = "_wtr_"
MUTR = "_mutr_"
NUMR = "_numr_"
CHAIN = "_chain_"
POSR = "_nomutr_"
MUT_DIR_PATH = "_dirpath_"
MUT_DIR_NAME = "_dirname_"
MUT_SEP = ","
COMP_SEP = "."
CHAIN_SEP = "-"
DIR_MUT_SEP = "_"
MULTI_MUT_SEP = ":"

######################### STRUCTURE EXTRACTION ########################

FLEXDDG_STATES = ("backrub", "wt", "mut")
STRUCT_EXTRACTED_PATTERN = r"(\d+)_0001.pdb"

############################# AGGREGATION #############################

MUTINFO_COLS = {"mut_name" : "mut_name",
                "dir_name" : "dir_name",
                "mut_label" : "mut_label",
                "pos_label" : "pos_label"}

ROSETTA_DF_COLS = {"b_steps" : "backrub_steps",
                   "energy_unit" : "energy_unit",
                   "mutation" : "mutation",
                   "mut_label" : "mutation_label",
                   "pos_label" : "position_label",
                   "name" : "name",
                   "n_struct" : "nstruct",
                   "scf_name" : "score_function_name",
                   "sc_type" : "score_type_name",
                   "sc_value" : "score_value",
                   "state" : "state",
                   "struct_id" : "struct_id",
                   "struct_num" : "struct_num",
                   "tot_score" : "total_score"}

############################### PLOTTING ##############################

PLOT_TYPES = ["contributions_barplot", "dg_swarmplot",
              "total_heatmap", "total_heatmap_saturation"]
