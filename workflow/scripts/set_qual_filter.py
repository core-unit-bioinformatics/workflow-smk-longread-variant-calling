#!/usr/bin/env python3

import sys
import fileinput

def main():

    for line in fileinput.input(encoding="utf-8"):
        if line.startswith("#"):
            sys.stdout.write(line)
            continue
        columns = line.strip().split()
        columns[5] = str(60)
        columns[6] = "PASS"

        # temp fix - ?
        # SHAPEIT does not allow missing genotypes in ref panel
        # - can't drop all variants that have a missing GT in one
        # sample (= essentially all?), so setting everything missing
        # to REF. Using bcftools plugin setGT does not work because
        # it replaces a partial missing like ".|1" with "0|0" for
        # whatever reason ...

        columns = columns[:9] + [gt.replace(".", "0") for gt in columns[9:]]

        out_line = "\t".join(columns)
        sys.stdout.write(out_line + "\n")

    return 0


if __name__ == "__main__":
    main()
