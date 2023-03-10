import sys

# Prepare lookup structure for
# reference genomes
user_ref_genomes = config.get("reference_genomes", None)
if user_ref_genomes is None:
    raise ValueError(f"Config does not contain key: 'reference_genomes'")
REF_GENOMES = dict()
for key, value in user_ref_genomes.items():
    _path_to_ref = DIR_GLOBAL_REF.joinpath(value)
    REF_GENOMES[key] = _path_to_ref
    _ref_suffix = _path_to_ref.suffix
    _ref_fai_suffix = f"{_ref_suffix}.fai"
    _path_to_idx = _path_to_ref.with_suffix(_ref_fai_suffix)
    REF_GENOMES[(key, "fai")] = _path_to_idx

# Default is (or-chained):
# - read unmapped
# - not primary alignment
# - read fails platform/vendor quality checks
# - read is PCR or optical duplicate
SAM_FLAG_EXCLUDE = config.get("sam_flag_exclude", 1796)
assert isinstance(SAM_FLAG_EXCLUDE, int)

RUN_HIFI_ALIGNER = config.get("run_hifi_aligner", [])
assert isinstance(RUN_HIFI_ALIGNER, list)
if not RUN_HIFI_ALIGNER and VERBOSE:
    sys.stderr.write("Warning: no HiFi aligner configured to run.")

HIFI_ALIGNER_NAME_MAPPING = {
    "minimap2": "mm2",
    "pbmm2": "pbmm2",
    "lra": "lra"
}

HIFI_ALIGNER_WILDCARDS = [
    HIFI_ALIGNER_NAME_MAPPING[name] for name in RUN_HIFI_ALIGNER
]

CHROMOSOMES = config.get("call_chromosomes", ["chr1"])
