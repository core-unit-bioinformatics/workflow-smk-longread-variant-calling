#!/usr/bin/env python3

import argparse as argp
import pathlib as pl

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_command_line():

    parser = argp.ArgumentParser()

    parser.add_argument(
        "--agg-table",
        "-a",
        type=lambda x: pl.Path(x).resolve(strict=True),
        dest="agg_table",
        help="Path to TSV table with coverage data aggregated in windows."
    )

    parser.add_argument(
        "--out-pdf",
        "-o",
        type=lambda x: pl.Path(x).resolve(strict=False),
        dest="out_pdf",
        help="Path to PDF output file."
    )

    parser.add_argument(
        "--title", "-t",
        type=str,
        default="auto",
        dest="title",
        help="Title for figure"
    )

    parser.add_argument(
        "--colors", "-c",
        type=str,
        nargs=2,
        default=["green", "magenta"],
        dest="colors",
        help="Two color [names] for coloring by chromosome. Default: green and magenta"
    )

    parser.add_argument(
        "--fig-size", "-fs",
        type=int,
        nargs=2,
        default=[20, 6],
        dest="figsize",
        help="Figure size in width X height. Default: 20 x 6"
    )

    args = parser.parse_args()

    return args


def read_global_coverage_info(file_path):

    with open(file_path, "r") as table:
        glob_cov, value = table.readline().strip().split()
        assert glob_cov.startswith("#")
    return int(float(value))


def determine_xticks_and_chrom_boundaries(alt_colors, chrom_order_nums):

    chrom_boundaries = []
    x_tick_pos = []
    x_tick_labels = []

    left_border = 0
    right_border = None
    colorize = False
    for pos, (color, x_label) in enumerate(zip(alt_colors, chrom_order_nums)):
        try:
            next_color = alt_colors[pos+1]
        except IndexError:
            right_border = pos
            break
        if color != next_color:
            right_border = pos
            x_tick = left_border + ((right_border - left_border) // 2)
            x_tick_pos.append(x_tick)
            x_tick_labels.append(x_label)
            if colorize:
                assert right_border is not None
                chrom_boundaries.append((left_border, right_border))
                colorize = False
            else:
                colorize = True
            left_border = pos+1
            right_border = None

    if right_border is not None and colorize:
        chrom_boundaries.append((left_border, right_border))
        x_tick = left_border + ((right_border - left_border) // 2)
        x_tick_pos.append(x_tick)
        x_tick_labels.append(x_label)

    return x_tick_pos, x_tick_labels, chrom_boundaries


def create_read_depth_profile_plot(figsize, colors, wg_cov, global_median, fig_title, out_pdf):

    fig, ax = plt.subplots(figsize=figsize)

    color_a, color_b = colors

    alt_colors = [
        color_a if sort_num % 2 == 0 else color_b
        for sort_num in wg_cov.index.get_level_values("sort_order")
    ]

    x_ticks, x_ticklabels, chrom_bounds = determine_xticks_and_chrom_boundaries(
        alt_colors, wg_cov.index.get_level_values("sort_order")
    )

    x_vals = np.arange(wg_cov.values.size)

    for (left, right) in chrom_bounds:
        ax.axvspan(left, right, alpha=0.5, color="lightgrey", zorder=0)

    ax.scatter(
        x_vals,
        wg_cov.values,
        s=6,
        c=alt_colors
    )

    ax.set_xlim(-25, x_vals.max() + 25)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    ax.set_ylabel("Median read depth in windows", fontsize=12)
    ax.set_xlabel("Chromosomes", fontsize=12)

    _ = ax.set_xticks(x_ticks)
    _ = ax.set_xticklabels(x_ticklabels, fontsize=12)

    _ = ax.axhline(
        int(global_median * 0.5), xmin=0, xmax=1, zorder=0,
        color="darkgrey", ls="dashed", label="MED x 0.5"
    )
    _ = ax.axhline(
        global_median, xmin=0, xmax=1, zorder=0,
        color="black", label="Median"
    )
    _ = ax.axhline(
        int(global_median * 1.5), xmin=0, xmax=1, zorder=0,
        color="black", ls="dashed", label="MED x 1.5"
    )
    _ = ax.axhline(
        int(global_median * 2), xmin=0, xmax=1, zorder=0,
        color="black", ls="dotted", label="MED x 2"
    )

    ax.legend(loc="best", fontsize=12)

    ax.set_title(fig_title, fontsize=14)

    plt.savefig(out_pdf, bbox_inches="tight")

    return


def main():

    args = parse_command_line()

    global_cov = read_global_coverage_info(args.agg_table)

    plot_data = pd.read_csv(
        args.agg_table, sep="\t",
        header=0, comment="#",
        index_col=[0,1,2,3,4]
    )

    if args.title == "auto":
        fig_title = args.agg_table.stem
    else:
        fig_title = args.title

    args.out_pdf.parent.mkdir(exist_ok=True, parents=True)
    _ = create_read_depth_profile_plot(
        tuple(args.figsize), args.colors,
        plot_data, global_cov,
        fig_title, args.out_pdf
    )

    return 0


if __name__ == "__main__":
    main()