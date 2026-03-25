import sys

###############################
### SETTINGS FOR HIFI ALIGNERS
###############################

# depending on the nature of the input samples,
# checking secondary alignments may be informative
# but, usually, any of the downstream tools does
# not use them, hence the main BAM file does not
# include those (see SAM_FLAGS above)
MIN_RATIO_PRIME_TO_SECOND = config.get("min_ratio_prime_to_second", 0.9)
assert isinstance(MIN_RATIO_PRIME_TO_SECOND, float)
KEEP_AT_MOST_N_SECOND = config.get("keep_at_most_n_second", 5)
assert isinstance(KEEP_AT_MOST_N_SECOND, int)


RUN_HIFI_ALIGNER = config.get("run_hifi_aligner", [])
assert isinstance(RUN_HIFI_ALIGNER, list)
if not RUN_HIFI_ALIGNER and VERBOSE:
    sys.stderr.write("Warning: no HiFi aligner configured to run.")

HIFI_ALIGNER_NAME_MAPPING = {
    "minimap2": "mm2",
    "pbmm2": "pbmm2",
    "lra": "lra"
}

HIFI_ALIGNER_WILDCARDS = sorted(
    set(
        HIFI_ALIGNER_NAME_MAPPING[name.lower()] for name in RUN_HIFI_ALIGNER
    )
)

CONSTRAINT_HIFI_ALIGNER = _build_constraint(HIFI_ALIGNER_WILDCARDS)

# This dict is populated below to link aligners
# to run for the individual variant callers
ALIGNER_FOR_CALLER = collections.defaultdict(list)
