import sys

####################################
### SETTINGS FOR HIFI SHORT CALLERS
### VARIANTS < 50 bp
####################################

HIFI_SHORT_CALLER_NAME_MAPPING = {
    "deepvariant": "deepvar",
}

##############################################
### SETTINGS FOR HIFI SHORT CALLING TOOLCHAIN
##############################################

RUN_HIFI_SHORT_CALLING_TOOLCHAIN = config.get("run_hifi_short_toolchain", [])
if not RUN_HIFI_SHORT_CALLING_TOOLCHAIN and VERBOSE:
    sys.stderr.write("Warning: no HiFi short variant calling toolchain configured to run.")

HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS = []

for toolchain in RUN_HIFI_SHORT_CALLING_TOOLCHAIN:
    aligner, caller = toolchain.split(",")
    wildcard_aln = HIFI_ALIGNER_NAME_MAPPING[aligner.strip().lower()]
    wildcard_call = HIFI_SHORT_CALLER_NAME_MAPPING[caller.strip().lower()]
    HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS.append(
        f"{wildcard_aln}-{wildcard_call}"
    )
    ALIGNER_FOR_CALLER[(wildcard_call, "hifi")].append(wildcard_aln)


#################################
### SETTINGS FOR HIFI SV CALLERS
### VARIANTS >= 50 bp
#################################

MIN_SV_LEN_CALL = int(config["minimum_sv_length_call"])
MIN_MAPQ = int(config["minimum_mapq"])
MIN_COV = int(config["minimum_coverage"])
MIN_ALN_LEN = int(config["minimum_alignment_length"])
