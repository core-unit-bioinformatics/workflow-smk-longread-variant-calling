#!/usr/bin/env python3

import argparse as argp
import collections as col
import fileinput as finp
import functools as fnt
import itertools as itt
import pathlib as pl
import sys

import pandas as pd
import numpy as np


VCF_INFO_COLUMN_INDEX = 7
VCF_SAMPLE_COLUMN_FIRST = 9


def parse_command_line():

    parser = argp.ArgumentParser()

    parser.add_argument(
        "--calling-algorithm", "-a",
        type=str,
        choices=["sniffles", "pbsv"],
        dest="caller",
        default=None
    )

    parser.add_argument(
        "--baseline-samples", "-b",
        type=str,
        nargs="*",
        default=None,
        dest="baseline_samples"
    )

    parser.add_argument(
        "--case-samples", "-c",
        type=str,
        nargs="*",
        default=None,
        dest="case_samples"
    )

    parser.add_argument(
        "--include-singletons", "-s",
        action="store_true",
        default=False,
        dest="include_singletons"
    )

    parser.add_argument(
        "--vcf-subset", "-v",
        action="store_true",
        default=False,
        dest="vcf_subset"
    )

    args = parser.parse_args()

    # fix: fileinput.input
    # default to sys.stdin
    # if not further arguments
    # are part of sys.argv
    sys.argv = sys.argv[:1]

    return args


def load_sample_lists(cli_arg):

    if cli_arg is None:
        sample_listing = None
    else:
        assert isinstance(cli_arg, list)
        try:
            file_path = pl.Path(cli_arg[0]).resolve(strict=True)
            assert len(cli_arg) == 1
        except FileNotFoundError:
            sample_listing = cli_arg
        else:
            with open(file_path, "r") as listing:
                sample_listing = listing.read().strip().split()
    if sample_listing is not None:
        sample_listing = list([s.strip() for s in sample_listing])
    return sample_listing


def get_sample_list(header_line):
    columns = header_line.strip().split()
    samples = columns[VCF_SAMPLE_COLUMN_FIRST:]
    return samples


def get_genotype_info(sample_genotype):
    gt = sample_genotype.split(":")[0]
    return gt


def get_sniffles_support_vector(info_column):

    info_fields = info_column.split(";")
    # should always be last
    supp_vec = None
    if info_fields[-1].startswith("SUPP_VEC="):
        supp_vec = list(info_fields[-1].strip("SUPP_VEC="))
    else:
        for field in info_fields:
            if not field.startswith("SUPP_VEC="):
                continue
            supp_vec = list(field.strip("SUPP_VEC="))
            break
    assert supp_vec is not None
    supp_vec = list(map(lambda s: bool(int(s)), supp_vec))
    return supp_vec


def is_case_or_baseline_sample(case_samples, baseline_samples, query_sample):
    return query_sample in case_samples or query_sample in baseline_samples


def is_case_sample(case_samples, query_sample):
    return query_sample in case_samples


def check_unknown_samples(case_samples, baseline_samples, vcf_samples):

    if case_samples is not None:
        missing_case = set(case_samples) - set(vcf_samples)
        if missing_case:
            raise ValueError(f"Unknown case samples: {missing_case}")

    if baseline_samples is not None:
        missing_baseline = set(baseline_samples) - set(vcf_samples)
        if missing_baseline:
            raise ValueError(f"Unknown baseline samples: {missing_baseline}")

    return


def process_sniffles_callset(case_samples, baseline_samples, keep_singletons, forward):

    samples = None

    if case_samples is not None and forward:
        send = lambda line: sys.stdout.write(line)
    else:
        send = lambda line: None

    # this works always: just record which samples
    # have joined support for a call (irrespective of
    # sample genotype)
    shared_support = col.Counter()
    optional_stats = col.Counter()

    # this may or may not be active depending
    # on command line arguments
    check_case = case_samples is not None
    check_baseline = baseline_samples is not None

    if check_case and check_baseline:
        case_or_base = fnt.partial(is_case_or_baseline_sample, case_samples, baseline_samples)
        is_case = fnt.partial(is_case_sample, case_samples)
    elif check_case:
        is_case = fnt.partial(is_case_sample, case_samples)
        case_or_base = fnt.partial(is_case_or_baseline_sample, case_samples, set())
    elif check_baseline:
        is_case = lambda x: False
        case_or_base = fnt.partial(is_case_or_baseline_sample, set(), baseline_samples)
    else:
        is_case = lambda x: False
        case_or_base = lambda x: False

    for vcf_line in finp.input(encoding="utf-8"):
        if vcf_line.startswith("##"):
            send(vcf_line)
            continue
        if vcf_line.startswith("#CHROM"):
            send(vcf_line)
            samples = get_sample_list(vcf_line)
            check_unknown_samples(case_samples, baseline_samples, samples)
            continue

        vcf_columns = vcf_line.strip().split()
        supp_vec = get_sniffles_support_vector(vcf_columns[VCF_INFO_COLUMN_INDEX])
        shared_samples = tuple(
            [sample for support, sample in zip(supp_vec, samples) if support]
        )
        shared_support[shared_samples] += 1

        if len(shared_samples) == 1 and not keep_singletons:
            continue

        # following: more detailed statistics or subsetting of the input vcf
        if check_case:
            if all(is_case(sample) for sample in shared_samples):
                # discard calls where all genotypes are 0/0
                sample_genotypes = [
                    get_genotype_info(sample) for support, sample in
                    zip(supp_vec, vcf_columns[VCF_SAMPLE_COLUMN_FIRST:])
                    if support
                ]
                if all(gt == "0/0" for gt in sample_genotypes):
                    optional_stats["skip_all_case_ref"] += 1
                    continue
                if keep_singletons or len(shared_samples) > 1:
                    optional_stats["select_all_case_alt"] += 1
                    send(vcf_line)
            elif all(case_or_base(sample) for sample in shared_samples):
                # if so, shared_samples must contain at least one baseline
                optional_stats["skip_case_base_mix"] += 1
                continue
            else:
                # not all case or mixed case/baseline
                optional_stats["skip_control_mix"] += 1
                continue

        if check_baseline and not check_case:
            if all(case_or_base(sample) for sample in shared_samples):
                # NB: since check case is False, must be all base
                optional_stats["skip_all_baseline"] += 1

    return shared_support, optional_stats, samples


def build_sample_matrix(shared_support, samples):

    counts = pd.DataFrame(
        np.zeros((len(samples), len(samples)), dtype=int),
        index=sorted(samples),
        columns=sorted(samples)
    )

    for sample_set, count in shared_support.items():
        if len(sample_set) == 1:
            counts.loc[sample_set[0], sample_set[0]] += count
            continue
        for a, b in itt.pairwise(sample_set):
            counts.loc[a, b] += count
            counts.loc[b, a] += count

    print(counts)

    return


def main():

    args = parse_command_line()

    case_samples = load_sample_lists(args.case_samples)
    if case_samples is not None and len(case_samples) == 1:
        setattr(args, "include_singletons", True)
    baseline_samples = load_sample_lists(args.baseline_samples)

    if args.caller == "sniffles":
        shared_support, optional_stats, vcf_samples = process_sniffles_callset(
            case_samples, baseline_samples,
            args.include_singletons, args.vcf_subset
        )
        print(optional_stats)

    #_ = build_sample_matrix(shared_support, vcf_samples)


if __name__ == "__main__":
    main()
