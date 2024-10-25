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
        out_line = "\t".join(columns)
        sys.stdout.write(out_line + "\n")

    return 0


if __name__ == "__main__":
    main()
