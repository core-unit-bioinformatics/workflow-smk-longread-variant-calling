#!/usr/bin/env python3

import argparse as argp
import fileinput
import functools as fnt
import pathlib as pl
import sys


def parse_command_line():

    parser = argp.ArgumentParser()
    parser.add_argument(
        "--male-samples",
        type=lambda x: pl.Path(x).resolve(strict=True),
        dest="male_samples"
    )

    args = parser.parse_args()

    return args


def find_sex_indices(header_line, male_samples):

    indices_female = []
    indices_male = []

    for pos, sample in enumerate(header_line.strip().split()[9:], start=9):
        if sample in male_samples:
            indices_male.append(pos)
        else:
            indices_female.append(pos)

    return tuple(indices_female), tuple(indices_male)


def mock_phase_genotypes(indices_female, indices_male, genotypes):

    columns = genotypes.strip().split()

    for female_idx in indices_female:
        columns[female_idx] = "0|0"

    for male_idx in indices_male:
        columns[male_idx] = columns[male_idx].replace("/", "|")

    out_line = "\t".join(columns) + "\n"

    return out_line



def main():

    args = parse_command_line()

    with open(args.male_sample, "r") as listing:
        male_samples = listing.read().strip().split()

    indices_female = None
    indices_male = None

    phase_line = fnt.partial(mock_phase_genotypes, args=(indices_female, indices_male))

    for line in fileinput.input(files=(sys.stdin,), encoding="utf-8"):
        if line.startswith("##"):
            sys.stdout.write(line)
        elif line.startswith("#CHROM"):
            indices_female, indices_male = find_sex_indices(line, male_samples)
            sys.stdout.write(line)
        else:
            phased_gt = phase_line(line)
            sys.stdout.write(phased_gt)

    return 0


if __name__ == "__main__":
    main()
