#!/usr/bin/env python3

import argparse as argp
import fileinput as finp
import sys


def parse_command_line():

    parser = argp.ArgumentParser()

    parser.add_argument(
        "--is-male",
        action="store_true",
        dest="is_male",
        default=False
    )

    args = parser.parse_args()

    sys.argv = sys.argv[:1]

    return args


def main():

    args = parse_command_line()

    for vcf_line in finp.input(encoding="utf-8"):
        if vcf_line.startswith("#"):
            sys.stdout.write(vcf_line)
            continue
        elif vcf_line.startswith("chrX") and args.is_male:
            # 2024-12-04 everything called as HOM (REF or ALT)
            # is likely ok, all HET calls should be hard fixed
            # to HOM REF to skip over likely errors
            if "0/1:" in vcf_line:
                vcf_line = vcf_line.replace("0/1:", "0/0:")
            sys.stdout.write(vcf_line)
            continue
        elif vcf_line.startswith("chrY"):
            # 2024-12-04 general statement
            # genotypes on chrY should not be trusted
            # => set everything to 0/0
            if "0/0:" not in vcf_line:
                vcf_line = vcf_line.replace("1/1:", "0/0:").replace("0/1:", "0/0:")
            sys.stdout.write(vcf_line)
            continue
        else:
            sys.stdout.write(vcf_line)

    return 0


if __name__ == "__main__":
    main()
