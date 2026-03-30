import sys

###############################
### SETTINGS FOR ONT ALIGNERS
###############################

# depending on the nature of the input samples,
# checking secondary alignments may be informative
# but, usually, any of the downstream tools does
# not use them, hence the main BAM file does not
# include those (see SAM_FLAGS above)

RUN_ONT_ALIGNER = config.get("run_ont_aligner", [])
assert isinstance(RUN_ONT_ALIGNER, list)
if not RUN_ONT_ALIGNER and VERBOSE:
    sys.stderr.write("Warning: no ONT aligner configured to run.")

# TODO: fix - make generic ALIGNER_NAME_MAPPING
ONT_ALIGNER_NAME_MAPPING = {
    "minimap2": "mm2",
}

ONT_ALIGNER_WILDCARDS = sorted(
    set(
        ONT_ALIGNER_NAME_MAPPING[name.lower()] for name in RUN_ONT_ALIGNER
    )
)

CONSTRAINT_ONT_ALIGNER = _build_constraint(ONT_ALIGNER_WILDCARDS)
