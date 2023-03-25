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

# Default is (or-chained):
# - read unmapped
# - not primary alignment
# - read fails platform/vendor quality checks
# - read is PCR or optical duplicate
SAM_FLAG_EXCLUDE = config.get("sam_flag_exclude", 1796)
assert isinstance(SAM_FLAG_EXCLUDE, int)


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

#################################
### SETTINGS FOR HIFI SV CALLERS
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
ALIGNER_FOR_CALLER = collections.defaultdict(list)

for toolchain in RUN_HIFI_SV_CALLING_TOOLCHAIN:
    aligner, caller = toolchain.split(",")
    wildcard_aln = HIFI_ALIGNER_NAME_MAPPING[aligner.strip()]
    wildcard_call = HIFI_SV_CALLER_NAME_MAPPING[caller.strip()]
    HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS.append(
        f"{wildcard_aln}-{wildcard_call}"
    )
    ALIGNER_FOR_CALLER[(wildcard_call, "hifi")].append(wildcard_aln)
