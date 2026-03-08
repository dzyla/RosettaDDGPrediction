#!/usr/bin/env python
# -*- Mode: python; coding:utf-8 -*-
import argparse
import collections
import logging as log
import sys
from pathlib import Path
from typing import Optional

import dask
from distributed import Client, LocalCluster
import pandas as pd

from . import aggregation 
from .defaults import (
    COMP_SEP,
    CONFIG_AGGR_DIR,
    CONFIG_AGGR_FILE,
    CONFIG_RUN_DIR,
    CONFIG_SETTINGS_DIR,
)
from . import util

def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate Rosetta ΔΔG data.")
    general_args = parser.add_argument_group("General arguments")
    mutatex_args = parser.add_argument_group("MutateX-compatible outputs arguments")

    general_args.add_argument("-cr", "--configfile-run", type=str, required=True, help="Configuration file of the protocol.")
    general_args.add_argument("-cs", "--configfile-settings", type=str, required=True, help="Configuration file for run settings.")
    general_args.add_argument("-ca", "--configfile-aggregate", type=str, default=CONFIG_AGGR_FILE, help="Configuration file for data aggregation.")
    general_args.add_argument("-d", "--running-dir", type=str, default=str(Path.cwd()), help="Directory where the protocol was run.")
    general_args.add_argument("-od", "--output-dir", type=str, default=str(Path.cwd()), help="Directory to store aggregated data.")
    general_args.add_argument("-mf", "--mutinfofile", type=str, help="File with info about mutations.")
    general_args.add_argument("-n", "--nproc", type=int, default=1, help="Number of parallel processes.")

    mutatex_args.add_argument("--mutatex-convert", action="store_true", help="Generate MutateX-compatible outputs.")
    mutatex_args.add_argument("--mutatex-reslistfile", type=str, default=None, help="Residue types list for MutateX.")
    mutatex_args.add_argument("--mutatex-dir", type=str, default="mutatex_compatible", help="Directory name for MutateX outputs.")

    args = parser.parse_args()
    
    run_dir = Path(util.get_abspath(args.running_dir))
    out_dir = Path(util.get_abspath(args.output_dir))
    mutinfo_file = Path(util.get_abspath(args.mutinfofile)) if args.mutinfofile else None
    
    log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        settings = util.get_config_settings(args.configfile_settings)
        config_run = util.get_config_run(args.configfile_run)
        config_aggr = util.get_config_aggregate(args.configfile_aggregate)
    except Exception as e:
        log.error(f"Configuration parsing failed: {e}")
        sys.exit(1)

    cluster = LocalCluster(n_workers=args.nproc, **settings["localcluster"])
    client = Client(cluster)

    family = config_run["family"]
    dfs_config = config_aggr["out_dfs"]
    dfs_options = dfs_config["options"]
    rescale = dfs_config["convert_to_kcalmol"]
    dfs_out_names = dfs_config["out_names"]
    
    mut_aggr = dfs_out_names["out_aggregate"]
    mut_struct = dfs_out_names["out_structures"]
    oa_suffix = dfs_out_names["out_suffix_aggregate"]
    os_suffix = dfs_out_names["out_suffix_structures"]

    futures = []
    mut_aggr_dfs, mut_struct_dfs = [], []
    out_dir.mkdir(parents=True, exist_ok=True)

    mutatex_dirpath = out_dir / args.mutatex_dir
    if args.mutatex_convert:
        mutatex_dirpath.mkdir(parents=True, exist_ok=True)
        mutatex_restypes = client.submit(util.get_res_list, args.mutatex_reslistfile)
        mutatex_dfs = collections.defaultdict(dict)

    if family in ("cartddg", "cartddg2020"):
        step_run_dir = config_run["steps"]["cartesian"]["wd"]
        options = config_run["steps"]["cartesian"]["options"]
        out_name = options[util.get_option_key(options=options, option="ddg_out")]
        scf_name = options[util.get_option_key(options=options, option="scf_name")]
    elif family == "flexddg":
        step_run_dir = config_run["steps"]["flexddg"]["wd"]
        options = config_run["steps"]["flexddg"]["options"]
        r_script_options = options[util.get_option_key(options=options, option="script_vars")]
        out_name = r_script_options[util.get_option_key(options=r_script_options, option="ddg_db_file")]
        scf_name = r_script_options[util.get_option_key(options=r_script_options, option="scf_name")]
        backrub_n_trials = r_script_options[util.get_option_key(options=r_script_options, option="backrub_n_trials")]
        backrub_traj_stride = r_script_options[util.get_option_key(options=r_script_options, option="backrub_traj_stride")]
        n_struct = config_run["mutations"]["nstruct"]
        struct_nums = [str(num) for num in range(1, n_struct + 1)]
        traj_stride = int(backrub_n_trials) // int(backrub_traj_stride)

    list_contributions = config_aggr["energy_contributions"][scf_name]
    conv_fact = config_aggr["conversion_factors"][scf_name]

    step_run_dir_path = run_dir if step_run_dir == "." else run_dir / step_run_dir

    try:
        mutinfo = client.submit(util.get_mutinfo, mutinfo_file=str(mutinfo_file)).result()
    except Exception as e:
        log.error(f"Could not load mutations info from {mutinfo_file}: {e}")
        sys.exit(1)

    for i, (mut_name, dir_name, mut_label, pos_label) in mutinfo.iterrows():
        mut_path = step_run_dir_path / dir_name

        if family in ("cartddg", "cartddg2020"):
            ddg_out = mut_path / out_name
            try:
                df = client.submit(aggregation.parse_output_cartddg, ddg_out=str(ddg_out), list_contributions=list_contributions, scf_name=scf_name)
                dfs = client.submit(aggregation.aggregate_data_cartddg, df=df, list_contributions=list_contributions).result()
            except Exception as e:
                log.warning(f"Failed parsing or aggregation for {mut_path.name}: {e}")
                continue
                
        elif family == "flexddg":
            struct_dfs = []
            for struct_num in struct_nums:
                db3_out = mut_path / struct_num / out_name
                try:
                    df = client.submit(aggregation.parse_output_flexddg, db3_out=str(db3_out), traj_stride=traj_stride, struct_num=struct_num, scf_name=scf_name)
                    struct_dfs.append(df.result())
                except Exception as e:
                    log.warning(f"Could not parse {db3_out}: {e}")
                    continue
            try:
                dfs = client.submit(aggregation.aggregate_data_flexddg, df=pd.concat(struct_dfs), list_contributions=list_contributions).result()
            except Exception as e:
                log.error(f"Failed aggregation for {mut_path.name}: {e}")
                sys.exit(1)

        dg_wt, dg_mut, ddg = dfs

        try:
            aggr_df, struct_df = client.submit(
                aggregation.generate_output_dataframes, dg_wt=dg_wt, dg_mut=dg_mut, ddg=ddg,
                mutation=mut_name, mut_label=mut_label, pos_label=pos_label, rescale=rescale,
                list_contributions=list_contributions, conv_fact=conv_fact, family=family
            ).result()
        except Exception as e:
            log.error(f"Failed output DF generation: {e}")
            sys.exit(1)

        aggr_df_path = out_dir / (mut_label + oa_suffix)
        struct_df_path = out_dir / (mut_label + os_suffix)

        futures.append(client.submit(aggr_df.to_csv, str(aggr_df_path), **dfs_options))
        futures.append(client.submit(struct_df.to_csv, str(struct_df_path), **dfs_options))
        
        mut_aggr_dfs.append(aggr_df)
        mut_struct_dfs.append(struct_df)

        if args.mutatex_convert:
            try:
                chain, wtr, numr, mutr = mut_name.split(COMP_SEP)
                mutatex_dfs[f"{wtr}{chain}{numr}"].update({mutr: struct_df})
            except Exception as e:
                log.error(f"Failed MutateX conversion for {mut_name}: {e}")
                continue

    if mut_aggr_dfs:
        mut_aggr_df = client.submit(pd.concat, mut_aggr_dfs, sort=False).result()
        mut_struct_df = client.submit(pd.concat, mut_struct_dfs, sort=False).result()

        futures.append(client.submit(mut_aggr_df.to_csv, str(out_dir / mut_aggr), **dfs_options))
        futures.append(client.submit(mut_struct_df.to_csv, str(out_dir / mut_struct), **dfs_options))

    if args.mutatex_convert:
        for mutatex_filename, m_dfs in mutatex_dfs.items():
            mutatex_filepath = mutatex_dirpath / mutatex_filename
            futures.append(client.submit(aggregation.write_mutatex_df, dfs=m_dfs, mutatex_file=str(mutatex_filepath), index=mutatex_restypes, family=family))

    client.gather(futures)

if __name__ == "__main__":
    main()
