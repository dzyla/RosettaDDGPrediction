#!/usr/bin/env python
# -*- Mode: python; coding:utf-8 -*-
import argparse
import logging as log
import sys
from pathlib import Path
from typing import Optional

import dask
from distributed import Client, fire_and_forget, LocalCluster
import yaml

from . import cleaning, pythonsteps, util
from .defaults import (CONFIG_RUN_DIR, CONFIG_SETTINGS_DIR, MUT_DIR_NAME,
                       MUT_DIR_PATH, ROSETTA_PROTOCOLS)

def main() -> None:
    parser = argparse.ArgumentParser(description="Run Rosetta ΔΔG prediction protocols.")
    general_args = parser.add_argument_group("General arguments")
    sat_args = parser.add_argument_group("Saturation-related arguments")

    general_args.add_argument("-p", "--pdbfile", type=str, required=True, help="PDB file of the wild-type structure.")
    general_args.add_argument("-cr", "--configfile-run", type=str, required=True, help="Configuration file of the protocol.")
    general_args.add_argument("-cs", "--configfile-settings", type=str, required=True, help="Configuration file for run settings.")
    general_args.add_argument("-r", "--rosettapath", type=str, required=True, help="Path to the Rosetta installation directory.")
    general_args.add_argument("-d", "--rundir", type=str, default=str(Path.cwd()), help="Run directory.")
    general_args.add_argument("-l", "--listfile", type=str, default=None, help="Mutations list file.")
    general_args.add_argument("-n", "--nproc", type=int, default=1, help="Number of parallel processes.")

    sat_args.add_argument("--saturation", action="store_true", help="Perform saturation mutagenesis.")
    sat_args.add_argument("--reslistfile", type=str, default=None, help="Residue types for saturation.")

    args = parser.parse_args()
    
    # Modern Pathlib usage
    pdb_file = Path(util.get_abspath(args.pdbfile))
    rosetta_path = Path(util.get_abspath(args.rosettapath))
    run_dir = Path(util.get_abspath(args.rundir))
    list_file = Path(util.get_abspath(args.listfile)) if args.listfile else None
    res_list_file = Path(util.get_abspath(args.reslistfile)) if args.reslistfile else None
    
    log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.saturation and not res_list_file:
        log.error("Saturation mutagenesis requested but no reslistfile provided.")
        sys.exit(1)

    try:
        settings = util.get_config_settings(args.configfile_settings)
        options = util.get_config_run(args.configfile_run)
    except Exception as e:
        log.error(f"Configuration parsing failed: {e}")
        sys.exit(1)

    cluster = LocalCluster(n_workers=args.nproc, **settings["localcluster"])
    client = Client(cluster)

    steps = options["steps"]
    family = options["family"]
    exec_path = rosetta_path / settings["rosetta"]["execpath"]
    exec_suffix = settings["rosetta"]["execsuffix"]

    curr_pdb_file = util.check_pdb_file(pdb_file=str(pdb_file), **options["pdb"])
    mutations, mutations_original = [], []

    if list_file:
        mut_options = options.get("mutations")
        if not mut_options:
            log.error("No mutation options found in run config.")
            sys.exit(1)
        mutations, mutations_original = util.get_mutations(
            list_file=str(list_file), res_list_file=str(res_list_file) if res_list_file else None,
            pdb_file=curr_pdb_file, res_numbering=mut_options["resnumbering"],
            extra=mut_options["extra"], n_struct=mut_options["nstruct"]
        )
    else:
        log.info("No mutations list passed. No mutations will be performed.")

    futures = []
    prev_opts, prev_wd = None, None

    for step_name, step in steps.items():
        if futures:
            client.gather(futures)
            futures.clear()
        
        step_opts = step["options"]
        step_features = ROSETTA_PROTOCOLS[family][step_name]
        step_wd = run_dir if step["wd"] == "." else run_dir / step["wd"]

        if step_features["run_by"] == "rosetta":
            clean_level = step["cleanlevel"]
            role = step_features["role"]

            if role == "processing" and step_name in ("relax", "relax2020"):
                process = client.submit(
                    util.run_relax, step_features=step_features, exec_path=str(exec_path),
                    exec_suffix=exec_suffix, step=step, step_wd=str(step_wd),
                    step_opts=step_opts, curr_pdb_file=curr_pdb_file,
                    settings=settings, n_proc=args.nproc
                )
                futures.append(process)
                fire_and_forget(client.submit(cleaning.clean_folders, step_name=step_name, wd=str(step_wd), options=step_opts, level=clean_level, wait_on=[process]))

            elif role == "ddg":
                util.write_mutinfo_file(mutations_original=mutations_original, out_dir=str(step_wd), mutinfo_file=mut_options["mutinfofile"])
                log.info(f"Mutations to be performed: {', '.join(dict.fromkeys(m[MUT_DIR_NAME] for m in mutations_original))}")

                for mut, mut_orig in zip(mutations, mutations_original):
                    mut_wd = step_wd / mut_orig[MUT_DIR_PATH]

                    if step_name in ("cartesian", "cartesian2020"):
                        process = client.submit(util.run_cartesian, step_features=step_features, exec_path=str(exec_path), exec_suffix=exec_suffix, mut=mut, mut_wd=str(mut_wd), step=step, step_opts=step_opts, curr_pdb_file=curr_pdb_file, settings=settings)
                    elif step_name == "flexddg":
                        process = client.submit(util.run_flexddg, step_features=step_features, exec_path=str(exec_path), exec_suffix=exec_suffix, mut=mut, mut_wd=str(mut_wd), step=step, step_opts=step_opts, curr_pdb_file=curr_pdb_file, settings=settings)

                    futures.append(process)
                    fire_and_forget(client.submit(cleaning.clean_folders, step_name=step_name, wd=str(mut_wd), options=step_opts, level=clean_level, wait_on=[process]))

        elif step_features["run_by"] == "python" and step_name == "structure_selection":
            curr_pdb_file = util.run_structure_selection(step_opts=step_opts, curr_pdb_file=curr_pdb_file, prev_opts=prev_opts, prev_wd=str(prev_wd), run_dir=str(run_dir))

        prev_opts, prev_wd = step_opts, step_wd

    client.gather(futures)

if __name__ == "__main__":
    main()
