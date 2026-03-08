#!/usr/bin/env python
# -*- Mode: python; coding:utf-8 -*-
import argparse
import itertools
import logging as log
import sys
from pathlib import Path
from typing import Optional

from .defaults import CONFIG_RUN_DIR, MUTINFO_COLS
from . import util

def main() -> None:
    parser = argparse.ArgumentParser(description="Check that the Rosetta calculations have terminated without errors.")
    parser.add_argument("-cr", "--configfile-run", type=str, required=True, help=f"Configuration file of the protocol. Assumed to be in {CONFIG_RUN_DIR} if just a name.")
    parser.add_argument("-d", "--running-dir", type=str, default=str(Path.cwd()), help="Directory where the protocol was run.")
    parser.add_argument("-mf", "--mutinfofile", type=str, default=None, help="File with info about the mutations.")

    args = parser.parse_args()

    run_dir = Path(util.get_abspath(args.running_dir))
    mutinfo_file = Path(util.get_abspath(args.mutinfofile)) if args.mutinfofile else None

    log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        config_run = util.get_config_run(args.configfile_run)
    except Exception as e:
        log.error(f"Configuration error: {e}")
        sys.exit(1)

    family = config_run["family"]

    if mutinfo_file:
        try:
            mutinfo = util.get_mutinfo(mutinfo_file=str(mutinfo_file))
        except Exception as e:
            log.error(f"Failed loading mutations info from {mutinfo_file}: {e}")
            sys.exit(1)

        dirs_mutations = [run_dir / Path(d) for d in mutinfo[MUTINFO_COLS["dir_name"]]]

        if family in ("cartddg", "cartddg2020"):
            dirs_paths = [str(d) for d in dirs_mutations]
        elif family == "flexddg":
            n_struct = config_run["mutations"]["nstruct"]
            struct_nums = [str(num) for num in range(1, n_struct + 1)]
            dirs_paths = [str(d / sn) for d in dirs_mutations for sn in struct_nums]
        else:
            dirs_paths = [str(run_dir)]
    else:
        dirs_paths = [str(run_dir)]

    try:
        crashed_runs_paths = util.check_rosetta_run(dirs_paths)
    except Exception as e:
        log.error(f"Error checking runs in {run_dir}: {e}")
        sys.exit(1)

    if not crashed_runs_paths:
        msg = "No crashed run has been found."
        log.info(msg)
        sys.stdout.write(msg + "\n")
    else:
        for crashed_run_path in crashed_runs_paths:
            msg = f"The run in the following directory reported a crash: {crashed_run_path}."
            log.warning(msg)
            sys.stdout.write(msg + "\n")

if __name__ == "__main__":
    main()
