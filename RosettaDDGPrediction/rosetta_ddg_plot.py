#!/usr/bin/env python
# -*- Mode: python; coding:utf-8 -*-
import argparse
import logging as log
import sys
from pathlib import Path

from .defaults import CONFIG_AGGR_DIR, CONFIG_AGGR_FILE, CONFIG_PLOT_DIR, PLOT_TYPES, ROSETTA_DF_COLS
from . import plotting
from . import util

def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Rosetta ΔΔG data.")
    parser.add_argument("-i", "--infile", type=str, required=True, help="Input CSV file.")
    parser.add_argument("-o", "--outfile", type=str, required=True, help="Output plot file.")
    parser.add_argument("-ca", "--configfile-aggregate", type=str, default=CONFIG_AGGR_FILE, help="Aggregate config file.")
    parser.add_argument("-cp", "--configfile-plot", type=str, required=True, help="Plotting config file.")

    args = parser.parse_args()
    
    in_file = Path(util.get_abspath(args.infile))
    out_file = Path(util.get_abspath(args.outfile))
    
    log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        config_aggr = util.get_config_aggregate(args.configfile_aggregate)
        config_plot = util.get_config_plot(args.configfile_plot)
    except Exception as e:
        log.error(f"Configuration error: {e}")
        sys.exit(1)

    plot_type = config_plot["plot"]["type"]
    config = config_plot["plot"]["options"]
    out_config = config_plot.get("output", {})

    try:
        # Modern Python 3.10+ Match-Case
        match plot_type:
            case "total_heatmap":
                df = plotting.load_aggregated_data(in_file=str(in_file))
                plotting.plot_total_heatmap(df=df, config=config, out_file=str(out_file), out_config=out_config)

            case "total_heatmap_saturation":
                df = plotting.load_aggregated_data(in_file=str(in_file), saturation=True)
                plotting.plot_total_heatmap(df=df, config=config, out_file=str(out_file), out_config=out_config, saturation=True)

            case "contributions_barplot":
                df = plotting.load_aggregated_data(in_file=str(in_file))
                scf_name = df.get(ROSETTA_DF_COLS["scf_name"])
                if scf_name is None or scf_name.empty:
                    log.error(f"Missing scoring function column: '{ROSETTA_DF_COLS['scf_name']}'")
                    sys.exit(1)
                    
                contributions = config_aggr["energy_contributions"][scf_name.unique()[0]]
                out_config.pop("format", None)
                plotting.plot_contributions_barplot(df=df, config=config, contributions=contributions, out_file=str(out_file), out_config=out_config)

            case "dg_swarmplot":
                df = plotting.load_aggregated_data(in_file=str(in_file))
                plotting.plot_dg_swarmplot(df=df, config=config, out_file=str(out_file), out_config=out_config)

            case _:
                log.error(f"Unrecognized plot type: {plot_type}")
                sys.exit(1)

    except Exception as e:
        log.exception(f"Failed to generate {plot_type} plot.")
        sys.exit(1)

if __name__ == "__main__":
    main()
