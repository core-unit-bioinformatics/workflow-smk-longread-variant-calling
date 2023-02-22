"""
Aggregate all
alignment jobs
"""

ALIGN_HIFI_OUTPUT = []
ALIGN_ONT_OUTPUT = []

if "mm2" in HIFI_ALIGNER_WILDCARDS:
    ALIGN_HIFI_OUTPUT.extend(
        rules.run_minimap2_hifi_align.input.bams
    )

if "pbmm2" in HIFI_ALIGNER_WILDCARDS:
    ALIGN_HIFI_OUTPUT.extend(
        rules.run_pbmm2_hifi_align.input.bams
    )

if "lra" in HIFI_ALIGNER_WILDCARDS:
    ALIGN_HIFI_OUTPUT.extend(
        rules.run_lra_hifi_align.input.bams
    )

