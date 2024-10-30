#!/usr/bin/env python3

import argparse as argp
import fileinput
import functools as fnt
import pathlib as pl
import sys


def parse_command_line():

    parser = argp.ArgumentParser()
    parser.add_argument(
        "--sample",
        type=str,
        dest="sample"
    )

    parser.add_argument(
        "--haplotype",
        type=int,
        dest="haplotype",
        choices=[1,2]
    )

    args = parser.parse_args()

    # to make fileinput switch to stdin
    sys.argv = sys.argv[:1]

    return args


def main():

    args = parse_command_line()

    header_suffix = f".PRG.{args.sample}.H{args.haplotype}"

    for line in fileinput.input(encoding="utf-8"):
        if line.startswith(">"):
            header = line.strip() + header_suffix
            sys.stdout.write(header + "\n")
        else:
            sys.stdout.write(line)

    return 0


if __name__ == "__main__":
    main()
