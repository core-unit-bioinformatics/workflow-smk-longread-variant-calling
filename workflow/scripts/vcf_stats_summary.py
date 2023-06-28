#!/usr/bin/env python3

import argparse as argp
import collections as col
import pathlib as pl
import re
import sys

import numpy as np
import pandas as pd
import pysam
import scipy.stats as scistats

STATS_TABLE_HEADER = [
    "location",
    "filter_status",
    "call_type",
    "variant_type",
    "attribute",
    "statistic",
    "value",
]


def parse_command_line():

    parser = argp.ArgumentParser()
    parser.add_argument(
        "--vcf-input",
        "--vcf",
        "--input",
        "-i",
        type=lambda x: pl.Path(x).resolve(strict=True),
        dest="vcf",
        help="Path to (preferably) indexed VCF file.",
    )
    parser.add_argument(
        "--fix-variant-type",
        "--var-type",
        "-t",
        type=str,
        default=None,
        dest="variant_type",
        help="If the variant type is not explicitly reported in the VCF "
        "(as part of the INFO field), set it to this value. Default: None",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=lambda x: pl.Path(x).resolve(strict=False),
        dest="output",
        help="Path to output TSV table file.",
    )
    args = parser.parse_args()
    return args


class TableSorter:
    """Helper class to sort the statistics
    table in a way that the most commonly
    used data are presented at the top.
    Unknown values are sorted in lex-order.
    """

    loc_order = {
        "wg": 0,
        "genome": 0,
        "chrX": 23,
        "X": 23,
        "chrY": 24,
        "Y": 24,
        "chrM": 25,
        "chrMT": 25,
        "M": 25,
        "MT": 25,
    }

    filter_order = {"PASS": 0, "RefCall": 1, "Refcall": 1}

    calltype_order = {"PRECISE": 0, "IMPRECISE": 1, "UNSPECIFIED": 2, "UNKNOWN": 3}

    vartype_order = {"SNV": 0, "INS": 1, "DEL": 2, "DUP": 3, "INV": 4, "BND": 5}

    attribute_order = {
        "GT:0/1": 0,
        "GT:1/1": 1,
        "GT:0/1": 2,
        "GT:1/0": 3,
        "GT:0/0": 4,
        "length": 5,
        "support": 6,
        "quality": 7,
    }

    statistics_order = {"count": 0, "mean": 55}

    def __init__(self, locations, filters, calltypes, vartypes, attributes, statistics):
        self = self._init_location_order(locations)
        self = self._init_generic_order(filters, self.filter_order)
        self = self._init_generic_order(calltypes, self.calltype_order)
        self = self._init_generic_order(vartypes, self.vartype_order)
        self = self._init_generic_order(attributes, self.attribute_order)
        self = self._init_statistics_order(statistics)
        return None

    def _init_location_order(self, locations):

        order_max = max(self.loc_order.values())
        match_autosome = re.compile("^(chr)?[0-9]{1,2}$")
        for loc in sorted(locations):
            mobj = match_autosome.search(loc)
            if loc in self.loc_order:
                continue
            elif mobj is not None:
                order_num = int(loc.strip("chr"))
                self.loc_order[loc] = order_num
            else:
                order_max += 1
                self.loc_order[loc] = order_max
        return self

    def _init_statistics_order(self, statistics):

        order_max = max(self.statistics_order.values())
        for statistic in sorted(statistics):
            if statistic in self.statistics_order:
                continue
            elif "pct_" in statistic:
                pct_num = int(statistic.rsplit("_", 1)[-1])
                self.statistics_order[statistic] = pct_num
                order_max = max(pct_num, order_max)
            else:
                order_max += 1
                self.statistics_order[statistic] = order_max
        return self

    def _init_generic_order(self, values, value_map):

        order_max = max(value_map.values())
        for value in sorted(values):
            if value in value_map:
                continue
            else:
                order_max += 1
                value_map[value] = order_max
        return self

    def get_order_number(self, data_row):

        order_loc = self.loc_order[data_row["location"]]
        order_filter = self.filter_order[data_row["filter_status"]]
        order_calltype = self.calltype_order[data_row["call_type"]]
        order_vartype = self.vartype_order[data_row["variant_type"]]
        order_attribute = self.attribute_order[data_row["attribute"]]
        order_statistic = self.statistics_order[data_row["statistic"]]
        order_num = (
            order_loc,
            order_filter,
            order_calltype,
            order_vartype,
            order_attribute,
            order_statistic,
        )
        return order_num


def collect_vcf_statistics(vcf_file, variant_type):
    """_summary_

    Args:
        vcf_file (pathlib.Path): Full path to VCF file
        variant_type (str or None): if set, fixed variant type
    """

    # PySam reads missing genotype as None
    gt_map = lambda x: "." if x is None else x

    count_stats = col.Counter()
    # stats to aggregate such as length, support/read depth etc.
    agg_stats = col.defaultdict(list)
    with pysam.VariantFile(vcf_file) as vcf:
        for record in vcf.fetch():
            contig = record.contig
            filter_status = "|".join(record.filter.keys())
            if "PRECISE" in record.info.keys():
                calltype = "PRECISE"
            elif "IMPRECISE" in record.info.keys():
                calltype = "IMPRECISE"
            else:
                calltype = "UNSPECIFIED"
            if "SUPPORT" in record.info.keys():
                read_support = record.info["SUPPORT"]
            elif "RE" in record.info.keys():
                read_support = record.info["RE"]
            elif "RNAMES" in record.info.keys():
                read_support = len(record.info["RNAMES"])
            else:
                # NB: this will be updated to read depth
                # below (sample info key 'DP')
                read_support = -1
            try:
                vartype = record.info["SVTYPE"]
            except KeyError:
                # this triggers for short variant
                # calling VCFs such as generated
                # by DeepVariant
                vartype = variant_type
                if variant_type is None:
                    raise ValueError(
                        "No variant type in INFO field (key: SVTYPE) "
                        f"and no variant type set for file {vcf_file}."
                    )
            try:
                varlen = abs(record.info["SVLEN"])
            except KeyError:
                ref_length = len(record.ref)
                assert ref_length > 0
                diff_lengths = set(abs(ref_length - len(alt)) for alt in record.alts)
                if len(diff_lengths) == 1:
                    # NB: works if all ALTs have the same length,
                    # e.g. T --> G,A
                    varlen = max(1, diff_lengths.pop())
                else:
                    # ALTs have different length, so any
                    # size estimate can be totally off
                    varlen = -1
            except:
                sys.stderr.write("\nERROR processing VCF record:\n")
                sys.stderr.write(f"{record}\n")
                sys.stderr.write(f"{record.info}\n")
                raise
            quality = record.qual
            if quality is None:
                quality = -1
            vcf_samples = list(record.samples.keys())
            assert (
                len(vcf_samples) == 1
            ), f"Multi-sample VCFs are not supported: {vcf_samples}"
            vcf_sample_name = vcf_samples[0]
            sample_info = dict(record.samples[vcf_sample_name].items())
            gt = sample_info["GT"]
            if read_support == -1:
                dp = sample_info["DP"]
                read_support = dp

            genotype = f"GT:{gt_map(gt[0])}/{gt_map(gt[1])}"
            count_stats[(contig, filter_status, calltype, vartype, genotype)] += 1
            count_stats[("genome", filter_status, calltype, vartype, genotype)] += 1
            agg_stats[(contig, filter_status, calltype, vartype, "length")].append(
                varlen
            )
            agg_stats[("genome", filter_status, calltype, vartype, "length")].append(
                varlen
            )
            agg_stats[(contig, filter_status, calltype, vartype, "quality")].append(
                quality
            )
            agg_stats[("genome", filter_status, calltype, vartype, "quality")].append(
                quality
            )
            agg_stats[(contig, filter_status, calltype, vartype, "support")].append(
                read_support
            )
            agg_stats[("genome", filter_status, calltype, vartype, "support")].append(
                read_support
            )

    return count_stats, agg_stats


def prepare_summary_statistics(count_stats, agg_stats):
    print(agg_stats)
    summary = []
    for key, value in count_stats.items():
        row = list(key)
        row.extend(["count", value])
        summary.append(row)

    for key, values in agg_stats.items():
        row = list(key)
        data_array = np.array(values)
        pct_scores = scistats.scoreatpercentile(
            data_array, per=[1, 5, 25, 50, 75, 95, 99]
        )
        mean = data_array.mean()
        if row[-1] in ["length", "support"]:
            normalize = lambda x: int(round(x, 0))
        else:
            normalize = lambda x: float(round(x, 4))
        labels = [
            "mean",
            "pct_01",
            "pct_05",
            "Q1_pct_25",
            "median_pct_50",
            "Q3_pct_75",
            "pct_95",
            "pct_99",
        ]
        values = [mean] + list(pct_scores)
        for l, v in zip(labels, values):
            norm_v = normalize(v)
            new_row = row + [l, norm_v]
            summary.append(new_row)

    return summary


def main():
    args = parse_command_line()
    count_stats, agg_stats = collect_vcf_statistics(args.vcf, args.variant_type)
    if count_stats:
        stats_summary = prepare_summary_statistics(count_stats, agg_stats)

        df = pd.DataFrame.from_records(stats_summary, columns=STATS_TABLE_HEADER)
        sorter = TableSorter(
            df["location"].unique(),
            df["filter_status"].unique(),
            df["call_type"].unique(),
            df["variant_type"].unique(),
            df["attribute"].unique(),
            df["statistic"].unique(),
        )
        df["sort_order"] = df.apply(sorter.get_order_number, axis=1)
        df.sort_values("sort_order", ascending=True, inplace=True)
        df.drop("sort_order", axis=1, inplace=True)

        args.output.parent.mkdir(exist_ok=True, parents=True)
        with open(args.output, "w") as table:
            _ = table.write(f"# {args.vcf.name}\n")
            df.to_csv(table, header=True, index=False, sep="\t")
    else:
        sys.stderr.write(f"WARNING - VCF is empty: {args.vcf.name}\n")
        sys.stderr.write(f"Creating empty output file\n")
        args.output.parent.mkdir(exist_ok=True, parents=True)
        with open(args.output, "w") as table:
            _ = table.write(f"# {args.vcf.name} - HAS NO RECORDS\n")
            _ = table.write("\t".join(STATS_TABLE_HEADER) + "\n")

    return 0


if __name__ == "__main__":
    main()
