import sys

# Prepare lookup structure for
# reference genomes
user_ref_genomes = config.get("reference_genomes", None)
if user_ref_genomes is None:
    raise ValueError(f"Config does not contain key: 'reference_genomes'")
REF_GENOMES = dict()
USE_REF_GENOMES = []
for key, value in user_ref_genomes.items():
    _path_to_ref = DIR_GLOBAL_REF.joinpath(value)
    REF_GENOMES[key] = _path_to_ref
    _ref_suffix = _path_to_ref.suffix
    _ref_fai_suffix = f"{_ref_suffix}.fai"
    _path_to_idx = _path_to_ref.with_suffix(_ref_fai_suffix)
    REF_GENOMES[(key, "fai")] = _path_to_idx
    USE_REF_GENOMES.append(key)


CHROMOSOMES = config.get("call_chromosomes", ["chr1"])
assert isinstance(CHROMOSOMES, list)

#############################
### Check if user-specified
### ROI files are available
#############################

USER_ROI_FILES = dict()
USER_ROI_FILE_WILDCARDS = []
user_roi_config = config.get("user_roi", None)
if user_roi_config is not None:
    for label, roi_file in user_roi_config.items():
        _path_to_roi = DIR_LOCAL_REF.joinpath(roi_file)
        if not _path_to_roi.is_file():
            err_msg = (
                "ERROR processing user-specified ROI files.\n"
                f"The file identified as >{label}< does not exist "
                f"at location: {_path_to_roi}\n"
                "Please copy the file to that folder."
            )
            logerr(err_msg)
            raise ValueError(err_msg)
        USER_ROI_FILES[label] = _path_to_roi
        USER_ROI_FILE_WILDCARDS.append(label)


###############################
### SETTINGS FOR VARIOUS TOOLS
###############################

# Affects alignment
# (reads are separated) and
# mosdepth read depth calculation
# ---
# Default is (or-chained):
# - read unmapped
# - not primary alignment
# - read fails platform/vendor quality checks
# - read is PCR or optical duplicate
SAM_FLAG_EXCLUDE = config.get("sam_flag_exclude", 1796)
assert isinstance(SAM_FLAG_EXCLUDE, int)

MOSDEPTH_QUANTIZE_STEPS = config.get(
    "mosdepth_quantize_steps", [0, 1, 5, 10, 15]
)
MOSDEPTH_QUANTIZE_NAMES = config.get(
    "mosdepth_quantize_names",
    ["NO_COV", "LOW_COV", "CALLABLE", "GOOD_COV", "HIGH_COV"]
)
MOSDEPTH_WINDOW_SIZE = config.get("mosdepth_window_size", 10000)
assert isinstance(MOSDEPTH_WINDOW_SIZE, int)

MOSDEPTH_COV_THRESHOLDS = config.get(
    "mosdepth_cov_thresholds", [0, 1, 5, 10, 15]
)

MOSDEPTH_MIN_MAPQ = config.get(
    "mosdepth_min_mapq", [0, 20]
)
assert all(isinstance(v, int) for v in MOSDEPTH_MIN_MAPQ)

###############################
### SETTINGS FOR HIFI ALIGNERS
###############################

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

# This dict is populated below to link aligners
# to run for the individual variant callers
ALIGNER_FOR_CALLER = collections.defaultdict(list)

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

HIFI_SV_CALLER_NAME_MAPPING = {
    "sniffles": "sniffles",
    "cutesv": "cutesv"
}

###########################################
### SETTINGS FOR HIFI SV CALLING TOOLCHAIN
###########################################

RUN_HIFI_SV_CALLING_TOOLCHAIN = config.get("run_hifi_sv_toolchain", [])
if not RUN_HIFI_SV_CALLING_TOOLCHAIN and VERBOSE:
    sys.stderr.write("Warning: no HiFi SV calling toolchain configured to run.")

HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS = []

for toolchain in RUN_HIFI_SV_CALLING_TOOLCHAIN:
    aligner, caller = toolchain.split(",")
    wildcard_aln = HIFI_ALIGNER_NAME_MAPPING[aligner.strip().lower()]
    wildcard_call = HIFI_SV_CALLER_NAME_MAPPING[caller.strip().lower()]
    HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS.append(
        f"{wildcard_aln}-{wildcard_call}"
    )
    ALIGNER_FOR_CALLER[(wildcard_call, "hifi")].append(wildcard_aln)
