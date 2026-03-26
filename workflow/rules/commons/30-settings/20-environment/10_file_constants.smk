"""This module sets file-related
constants that are partly user-defined:
- the run suffix (if provided)
- the sample sheet path / name (if provided)
- the output path of the config dump
- the output path of the manifest dump

This module does not specify the locations
of the internal accounting and cache files
used for reference containers.
"""

import re


# Default for the run suffix is >derive< behavior
RUN_SUFFIX = config.get(OPTIONS.suffix.name, OPTIONS.suffix.default)

# A workflow may run w/o a sample sheet, so this can also be empty
SAMPLE_SHEET_PATH = config.get(OPTIONS.samples.name, OPTIONS.samples.default)
SAMPLE_SHEET_NAME = None

if RUN_SUFFIX == "derive" and not SAMPLE_SHEET_PATH:
    # 2025-07 changed behavior here:
    # the default for the run suffix is now >derive<.
    # If the workflow runs w/o a sample sheet,
    # change the run suffix to the empty
    # string instead of throwing an error.
    RUN_SUFFIX = ""

if SAMPLE_SHEET_PATH:
    SAMPLE_SHEET_PATH = pathlib.Path(SAMPLE_SHEET_PATH).resolve(strict=True)
    if SAMPLE_SHEET_PATH.suffix != ".tsv":
        raise RuntimeError(
            f"Only TSV tables are allowed as sample sheet (*.tsv): {SAMPLE_SHEET_PATH.name}"
        )
    # Note: name is the name w/o the file extension
    SAMPLE_SHEET_NAME = SAMPLE_SHEET_PATH.stem
    assert SAMPLE_SHEET_NAME, f"Empty sample sheet name?! - {SAMPLE_SHEET_NAME}"
    if RUN_SUFFIX == "derive":
        RUN_SUFFIX = SAMPLE_SHEET_NAME
    # set path in results/ folder to keep a copy
    # of the sample sheet as part of the output
    COPY_SAMPLE_SHEET_RELPATH = DIR_RES.joinpath(f"{SAMPLE_SHEET_NAME}.tsv")
    COPY_SAMPLE_SHEET_ABSPATH = COPY_SAMPLE_SHEET_RELPATH.resolve()
else:
    # set target path of sample sheet under results/
    # to empty path if not sample sheet provided
    COPY_SAMPLE_SHEET_RELPATH = "no-sample-sheet"
    COPY_SAMPLE_SHEET_ABSPATH = ""

# Postprocess the run suffix to consist
# only of digits, chars, and "minus"
RUN_SUFFIX = RUN_SUFFIX.replace(".", "-").replace("_", "-")
RUN_SUFFIX = "".join(re.findall("[a-z0-9\-]+", RUN_SUFFIX, re.IGNORECASE))
# in case the above resulted in two or more
# consecutive hyphens, replace with single one
RUN_SUFFIX = re.sub("\-\-+", "-", RUN_SUFFIX)

if RUN_SUFFIX:
    # implies: it's not the empty string
    assert re.match("^[a-z0-9\-]+$", RUN_SUFFIX, re.IGNORECASE) is not None,\
        f"Invalid run suffix: {RUN_SUFFIX}"
    RUN_SUFFIX = f".{RUN_SUFFIX}"


##############################
# Settings of fixed file names
# (modulo the run suffix) for
# manifest and config dump

# NB: these two files are initially defined
# (in commons::10_constants.smk) as files
# underneath 'results/'
RUN_CONFIG_ABSPATH = DIR_WORKING.joinpath(
    CONST_FILES.config_dump.with_suffix(f"{RUN_SUFFIX}.yaml")
).resolve(strict=False)
RUN_CONFIG_RELPATH = RUN_CONFIG_ABSPATH.relative_to(DIR_WORKING)

MANIFEST_ABSPATH = DIR_WORKING.joinpath(
    CONST_FILES.manifest.with_suffix(f"{RUN_SUFFIX}.tsv")
).resolve(strict=False)
MANIFEST_RELPATH = MANIFEST_ABSPATH.relative_to(DIR_WORKING)
